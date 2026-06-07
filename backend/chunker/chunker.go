// Package chunker provides wrappers for Fs and Object which split large files in chunks
package chunker

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	gohash "hash"
	"io"
	"math/rand"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/multipart"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/pool"
)

// Chunker's composite files have one or more chunks
// and optional metadata object. If it's present,
// meta object is named after the original file.
//
// The only supported metadata format is simplejson atm.
// It supports only per-file meta objects that are rudimentary,
// used mostly for consistency checks (lazily for performance reasons).
// Other formats can be developed that use an external meta store
// free of these limitations, but this needs some support from
// rclone core (e.g. metadata store interfaces).
//
// The following types of chunks are supported:
// data and control, active and temporary.
// Chunk type is identified by matching chunk file name
// based on the chunk name format configured by user and transaction
// style being used.
//
// Both data and control chunks can be either temporary (aka hidden)
// or active (non-temporary aka normal aka permanent).
// An operation creates temporary chunks while it runs.
// By completion it removes temporary and leaves active chunks.
//
// Temporary chunks have a special hardcoded suffix in addition
// to the configured name pattern.
// Temporary suffix includes so called transaction identifier
// (abbreviated as `xactID` below), a generic non-negative base-36 "number"
// used by parallel operations to share a composite object.
// Chunker also accepts the longer decimal temporary suffix (obsolete),
// which is transparently converted to the new format. In its maximum
// length of 13 decimals it makes a 7-digit base-36 number.
//
// When transactions is set to the norename style, data chunks will
// keep their temporary chunk names (with the transaction identifier
// suffix). To distinguish them from temporary chunks, the txn field
// of the metadata file is set to match the transaction identifier of
// the data chunks.
//
// Chunker can tell data chunks from control chunks by the characters
// located in the "hash placeholder" position of configured format.
// Data chunks have decimal digits there.
// Control chunks have in that position a short lowercase alphanumeric
// string (starting with a letter) prepended by underscore.
//
// Metadata format v1 does not define any control chunk types,
// they are currently ignored aka reserved.
// In future they can be used to implement resumable uploads etc.
const (
	ctrlTypeRegStr   = `[a-z][a-z0-9]{2,6}`
	tempSuffixFormat = `_%04s`
	tempSuffixRegStr = `_([0-9a-z]{4,9})`
	tempSuffixRegOld = `\.\.tmp_([0-9]{10,13})`
)

var (
	// regular expressions to validate control type and temporary suffix
	ctrlTypeRegexp   = regexp.MustCompile(`^` + ctrlTypeRegStr + `$`)
	tempSuffixRegexp = regexp.MustCompile(`^` + tempSuffixRegStr + `$`)
)

// Normally metadata is a small piece of JSON (about 100-300 bytes).
// The size of valid metadata must never exceed this limit.
// Current maximum provides a reasonable room for future extensions.
//
// Please refrain from increasing it, this can cause old rclone versions
// to fail, or worse, treat meta object as a normal file (see NewObject).
// If more room is needed please bump metadata version forcing previous
// releases to ask for upgrade, and offload extra info to a control chunk.
//
// And still chunker's primary function is to chunk large files
// rather than serve as a generic metadata container.
const (
	maxMetadataSize        = 1023
	maxMetadataSizeWritten = 255
)

// Current/highest supported metadata format.
const metadataVersion = 2

// optimizeFirstChunk enables the following optimization in the Put:
// If a single chunk is expected, put the first chunk using the
// base target name instead of a temporary name, thus avoiding
// extra rename operation.
// Warning: this optimization is not transaction safe.
const optimizeFirstChunk = false

// revealHidden is a stub until chunker lands the `reveal hidden` option.
const revealHidden = false

// Prevent memory overflow due to specially crafted chunk name
const maxSafeChunkNumber = 10000000

// Number of attempts to find unique transaction identifier
const maxTransactionProbes = 100

// standard chunker errors
var (
	ErrChunkOverflow = errors.New("chunk number overflow")
	ErrMetaTooBig    = errors.New("metadata is too big")
	ErrMetaUnknown   = errors.New("unknown metadata, please upgrade rclone")
)

// variants of baseMove's parameter delMode
const (
	delNever  = 0 // don't delete, just move
	delAlways = 1 // delete destination before moving
	delFailed = 2 // move, then delete and try again if failed
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "chunker",
		Description: "Transparently chunk/split large files",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "remote",
			Required: true,
			Help: `Remote to chunk/unchunk.

Normally should contain a ':' and a path, e.g. "myremote:path/to/dir",
"myremote:bucket" or maybe "myremote:" (not recommended).`,
		}, {
			Name:     "chunk_size",
			Advanced: false,
			Default:  fs.SizeSuffix(2147483648), // 2 GiB
			Help:     `Files larger than chunk size will be split in chunks.`,
		}, {
			Name:     "name_format",
			Advanced: true,
			Hide:     fs.OptionHideCommandLine,
			Default:  `*.rclone_chunk.###`,
			Help: `String format of chunk file names.

The two placeholders are: base file name (*) and chunk number (#...).
There must be one and only one asterisk and one or more consecutive hash characters.
If chunk number has less digits than the number of hashes, it is left-padded by zeros.
If there are more digits in the number, they are left as is.
Possible chunk files are ignored if their name does not match given format.`,
		}, {
			Name:     "start_from",
			Advanced: true,
			Hide:     fs.OptionHideCommandLine,
			Default:  1,
			Help: `Minimum valid chunk number. Usually 0 or 1.

By default chunk numbers start from 1.`,
		}, {
			Name:     "meta_format",
			Advanced: true,
			Hide:     fs.OptionHideCommandLine,
			Default:  "simplejson",
			Help: `Format of the metadata object or "none".

By default "simplejson".
Metadata is a small JSON file named after the composite file.`,
			Examples: []fs.OptionExample{{
				Value: "none",
				Help: `Do not use metadata files at all.
Requires hash type "none".`,
			}, {
				Value: "simplejson",
				Help: `Simple JSON supports hash sums and chunk validation.

It has the following fields: ver, size, nchunks, md5, sha1.`,
			}},
		}, {
			Name:     "hash_type",
			Advanced: false,
			Default:  "md5",
			Help: `Choose how chunker handles hash sums.

All modes but "none" require metadata.`,
			Examples: []fs.OptionExample{{
				Value: "none",
				Help: `Pass any hash supported by wrapped remote for non-chunked files.
Return nothing otherwise.`,
			}, {
				Value: "md5",
				Help:  `MD5 for composite files.`,
			}, {
				Value: "sha1",
				Help:  `SHA1 for composite files.`,
			}, {
				Value: "md5all",
				Help:  `MD5 for all files.`,
			}, {
				Value: "sha1all",
				Help:  `SHA1 for all files.`,
			}, {
				Value: "md5quick",
				Help: `Copying a file to chunker will request MD5 from the source.
Falling back to SHA1 if unsupported.`,
			}, {
				Value: "sha1quick",
				Help:  `Similar to "md5quick" but prefers SHA1 over MD5.`,
			}},
		}, {
			Name:     "fail_hard",
			Advanced: true,
			Default:  false,
			Help:     `Choose how chunker should handle files with missing or invalid chunks.`,
			Examples: []fs.OptionExample{
				{
					Value: "true",
					Help:  "Report errors and abort current command.",
				}, {
					Value: "false",
					Help:  "Warn user, skip incomplete file and proceed.",
				},
			},
		}, {
			Name:     "concurrent_buffer",
			Advanced: true,
			Default:  fs.Tristate{},
			Help: `Choose whether chunker buffers user-forced concurrent chunk writer uploads.

When unset, chunker chooses the default strategy. Setting this to "true" buffers
each concurrent chunk in memory so retries and ordered hash calculation can
replay it from the buffer. Setting it to "false" avoids that chunk-sized buffer
and retries rewind the supplied chunk reader with Seek(0).`,
			Examples: []fs.OptionExample{{
				Value: "true",
				Help:  "Buffer each concurrent chunk in memory.",
			}, {
				Value: "false",
				Help:  "Do not buffer concurrent chunks; retry by seeking the supplied chunk reader.",
			}},
		}, {
			Name:     "transactions",
			Advanced: true,
			Default:  "rename",
			Help:     `Choose how chunker should handle temporary files during transactions.`,
			Hide:     fs.OptionHideCommandLine,
			Examples: []fs.OptionExample{
				{
					Value: "rename",
					Help:  "Rename temporary files after a successful transaction.",
				}, {
					Value: "norename",
					Help: `Leave temporary file names and write transaction ID to metadata file.
Metadata is required for no rename transactions (meta format cannot be "none").
If you are using norename transactions you should be careful not to downgrade Rclone
as older versions of Rclone don't support this transaction style and will misinterpret
files manipulated by norename transactions.
This method is EXPERIMENTAL, don't use on production systems.`,
				}, {
					Value: "auto",
					Help: `Rename or norename will be used depending on capabilities of the backend.
If meta format is set to "none", rename transactions will always be used.
This method is EXPERIMENTAL, don't use on production systems.`,
				},
			},
		}},
	})
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, rpath string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.StartFrom < 0 {
		return nil, errors.New("start_from must be non-negative")
	}

	remote := opt.Remote
	if strings.HasPrefix(remote, name+":") {
		return nil, errors.New("can't point remote at itself - check the value of the remote setting")
	}

	baseName, basePath, err := fspath.SplitFs(remote)
	if err != nil {
		return nil, fmt.Errorf("failed to parse remote %q to wrap: %w", remote, err)
	}
	// Look for a file first
	remotePath := fspath.JoinRootPath(basePath, rpath)
	baseFs, err := cache.Get(ctx, baseName+remotePath)
	if err != fs.ErrorIsFile && err != nil {
		return nil, fmt.Errorf("failed to make remote %q to wrap: %w", baseName+remotePath, err)
	}
	if !operations.CanServerSideMove(baseFs) {
		return nil, errors.New("can't use chunker on a backend which doesn't support server-side move or copy")
	}

	f := &Fs{
		base: baseFs,
		name: name,
		root: rpath,
		opt:  *opt,
	}
	f.dirSort = true // processEntries requires that meta Objects prerun data chunks atm.

	if err := f.configure(opt.NameFormat, opt.MetaFormat, opt.HashType, opt.Transactions); err != nil {
		return nil, err
	}

	// Handle the tricky case detected by FsMkdir/FsPutFiles/FsIsFile
	// when `rpath` points to a composite multi-chunk file without metadata,
	// i.e. `rpath` does not exist in the wrapped remote, but chunker
	// detects a composite file because it finds the first chunk!
	// (yet can't satisfy fstest.CheckListing, will ignore)
	if err == nil && !f.useMeta {
		firstChunkPath := f.makeChunkName(remotePath, 0, "", "")
		newBase, testErr := cache.Get(ctx, baseName+firstChunkPath)
		if testErr == fs.ErrorIsFile {
			f.base = newBase
			err = testErr
		}
	}
	cache.PinUntilFinalized(f.base, f)

	// Correct root if definitely pointing to a file
	if err == fs.ErrorIsFile {
		f.root = path.Dir(f.root)
		if f.root == "." || f.root == "/" {
			f.root = ""
		}
	}

	// Note 1: the features here are ones we could support, and they are
	// ANDed with the ones from wrappedFs.
	// Note 2: PutStream and OpenChunkWriter are provided by chunker itself,
	// so they are restored after Mask regardless of wrappedFs features.
	f.features = (&fs.Features{
		CaseInsensitive:          true,
		DuplicateFiles:           true,
		ReadMimeType:             false, // Object.MimeType not supported
		WriteMimeType:            true,
		BucketBased:              true,
		CanHaveEmptyDirectories:  true,
		ServerSideAcrossConfigs:  true,
		ReadDirMetadata:          true,
		WriteDirMetadata:         true,
		WriteDirSetModTime:       true,
		UserDirMetadata:          true,
		DirModTimeUpdatesOnWrite: true,
	}).Fill(ctx, f).Mask(ctx, baseFs).WrapsFs(f, baseFs)

	f.features.ListR = nil // Recursive listing may cause chunker skip files
	f.features.ListP = nil // ListP not supported yet
	f.features.OpenChunkWriter = f.OpenChunkWriter
	f.features.ChunkWriterDoesntSeek = true
	f.features.PutStream = f.PutStream
	f.features.DisableList(fs.GetConfig(ctx).DisableFeatures)

	return f, err
}

// Options defines the configuration for this backend
type Options struct {
	Remote           string        `config:"remote"`
	ChunkSize        fs.SizeSuffix `config:"chunk_size"`
	NameFormat       string        `config:"name_format"`
	StartFrom        int           `config:"start_from"`
	MetaFormat       string        `config:"meta_format"`
	HashType         string        `config:"hash_type"`
	FailHard         bool          `config:"fail_hard"`
	ConcurrentBuffer fs.Tristate   `config:"concurrent_buffer"`
	Transactions     string        `config:"transactions"`
}

// Fs represents a wrapped fs.Fs
type Fs struct {
	name         string
	root         string
	base         fs.Fs          // remote wrapped by chunker overlay
	wrapper      fs.Fs          // wrapper is used by SetWrapper
	useMeta      bool           // false if metadata format is 'none'
	useMD5       bool           // mutually exclusive with useSHA1
	useSHA1      bool           // mutually exclusive with useMD5
	hashFallback bool           // allows fallback from MD5 to SHA1 and vice versa
	hashAll      bool           // hash all files, mutually exclusive with hashFallback
	dataNameFmt  string         // name format of data chunks
	ctrlNameFmt  string         // name format of control chunks
	nameRegexp   *regexp.Regexp // regular expression to match chunk names
	xactIDRand   *rand.Rand     // generator of random transaction identifiers
	xactIDMutex  sync.Mutex     // mutex for the source of randomness
	opt          Options        // copy of Options
	features     *fs.Features   // optional features
	dirSort      bool           // reserved for future, ignored
	useNoRename  bool           // can be set with the transactions option
}

// configure sets up chunker for given name format, meta format and hash type.
// It also seeds the source of random transaction identifiers.
// configure must be called only from NewFs or by unit tests.
func (f *Fs) configure(nameFormat, metaFormat, hashType, transactionMode string) error {
	if err := f.setChunkNameFormat(nameFormat); err != nil {
		return fmt.Errorf("invalid name format '%s': %w", nameFormat, err)
	}
	if err := f.setMetaFormat(metaFormat); err != nil {
		return err
	}
	if err := f.setHashType(hashType); err != nil {
		return err
	}
	if err := f.setTransactionMode(transactionMode); err != nil {
		return err
	}

	randomSeed := time.Now().UnixNano()
	f.xactIDRand = rand.New(rand.NewSource(randomSeed))

	return nil
}

func (f *Fs) setMetaFormat(metaFormat string) error {
	switch metaFormat {
	case "none":
		f.useMeta = false
	case "simplejson":
		f.useMeta = true
	default:
		return fmt.Errorf("unsupported meta format '%s'", metaFormat)
	}
	return nil
}

// setHashType
// must be called *after* setMetaFormat.
//
// In the "All" mode chunker will force metadata on all files
// if the wrapped remote can't provide given hashsum.
func (f *Fs) setHashType(hashType string) error {
	f.useMD5 = false
	f.useSHA1 = false
	f.hashFallback = false
	f.hashAll = false
	requireMetaHash := true

	switch hashType {
	case "none":
		requireMetaHash = false
	case "md5":
		f.useMD5 = true
	case "sha1":
		f.useSHA1 = true
	case "md5quick":
		f.useMD5 = true
		f.hashFallback = true
	case "sha1quick":
		f.useSHA1 = true
		f.hashFallback = true
	case "md5all":
		f.useMD5 = true
		f.hashAll = !f.base.Hashes().Contains(hash.MD5) || f.base.Features().SlowHash
	case "sha1all":
		f.useSHA1 = true
		f.hashAll = !f.base.Hashes().Contains(hash.SHA1) || f.base.Features().SlowHash
	default:
		return fmt.Errorf("unsupported hash type '%s'", hashType)
	}
	if requireMetaHash && !f.useMeta {
		return fmt.Errorf("hash type '%s' requires compatible meta format", hashType)
	}
	return nil
}

func (f *Fs) setTransactionMode(transactionMode string) error {
	switch transactionMode {
	case "rename":
		f.useNoRename = false
	case "norename":
		if !f.useMeta {
			return errors.New("incompatible transaction options")
		}
		f.useNoRename = true
	case "auto":
		f.useNoRename = !f.CanQuickRename()
		if f.useNoRename && !f.useMeta {
			f.useNoRename = false
			return errors.New("using norename transactions requires metadata")
		}
	default:
		return fmt.Errorf("unsupported transaction mode '%s'", transactionMode)
	}
	return nil
}

// setChunkNameFormat converts pattern based chunk name format
// into Printf format and Regular expressions for data and
// control chunks.
func (f *Fs) setChunkNameFormat(pattern string) error {
	// validate pattern
	if strings.Count(pattern, "*") != 1 {
		return errors.New("pattern must have exactly one asterisk (*)")
	}
	numDigits := strings.Count(pattern, "#")
	if numDigits < 1 {
		return errors.New("pattern must have a hash character (#)")
	}
	if strings.Index(pattern, "*") > strings.Index(pattern, "#") {
		return errors.New("asterisk (*) in pattern must come before hashes (#)")
	}
	if ok, _ := regexp.MatchString("^[^#]*[#]+[^#]*$", pattern); !ok {
		return errors.New("hashes (#) in pattern must be consecutive")
	}
	if dir, _ := path.Split(pattern); dir != "" {
		return errors.New("directory separator prohibited")
	}
	if pattern[0] != '*' {
		return errors.New("pattern must start with asterisk") // to be lifted later
	}

	// craft a unified regular expression for all types of chunks
	reHashes := regexp.MustCompile("[#]+")
	reDigits := "[0-9]+"
	if numDigits > 1 {
		reDigits = fmt.Sprintf("[0-9]{%d,}", numDigits)
	}
	reDataOrCtrl := fmt.Sprintf("(?:(%s)|_(%s))", reDigits, ctrlTypeRegStr)

	// this must be non-greedy or else it could eat up temporary suffix
	const mainNameRegStr = "(.+?)"

	strRegex := regexp.QuoteMeta(pattern)
	strRegex = reHashes.ReplaceAllLiteralString(strRegex, reDataOrCtrl)
	strRegex = strings.ReplaceAll(strRegex, "\\*", mainNameRegStr)
	strRegex = fmt.Sprintf("^%s(?:%s|%s)?$", strRegex, tempSuffixRegStr, tempSuffixRegOld)
	f.nameRegexp = regexp.MustCompile(strRegex)

	// craft printf formats for active data/control chunks
	fmtDigits := "%d"
	if numDigits > 1 {
		fmtDigits = fmt.Sprintf("%%0%dd", numDigits)
	}
	strFmt := strings.ReplaceAll(pattern, "%", "%%")
	strFmt = strings.Replace(strFmt, "*", "%s", 1)
	f.dataNameFmt = reHashes.ReplaceAllLiteralString(strFmt, fmtDigits)
	f.ctrlNameFmt = reHashes.ReplaceAllLiteralString(strFmt, "_%s")
	return nil
}

// makeChunkName produces chunk name (or path) for a given file.
//
// filePath can be name, relative or absolute path of main file.
//
// chunkNo must be a zero based index of data chunk.
// Negative chunkNo e.g. -1 indicates a control chunk.
// ctrlType is type of control chunk (must be valid).
// ctrlType must be "" for data chunks.
//
// xactID is a transaction identifier. Empty xactID denotes active chunk,
// otherwise temporary chunk name is produced.
func (f *Fs) makeChunkName(filePath string, chunkNo int, ctrlType, xactID string) string {
	dir, parentName := path.Split(filePath)
	var name, tempSuffix string
	switch {
	case chunkNo >= 0 && ctrlType == "":
		name = fmt.Sprintf(f.dataNameFmt, parentName, chunkNo+f.opt.StartFrom)
	case chunkNo < 0 && ctrlTypeRegexp.MatchString(ctrlType):
		name = fmt.Sprintf(f.ctrlNameFmt, parentName, ctrlType)
	default:
		panic("makeChunkName: invalid argument") // must not produce something we can't consume
	}
	if xactID != "" {
		tempSuffix = fmt.Sprintf(tempSuffixFormat, xactID)
		if !tempSuffixRegexp.MatchString(tempSuffix) {
			panic("makeChunkName: invalid argument")
		}
	}
	return dir + name + tempSuffix
}

// parseChunkName checks whether given file path belongs to
// a chunk and extracts chunk name parts.
//
// filePath can be name, relative or absolute path of a file.
//
// Returned parentPath is path of the composite file owning the chunk.
// It's a non-empty string if valid chunk name is detected
// or "" if it's not a chunk.
// Other returned values depend on detected chunk type:
// data or control, active or temporary:
//
// data chunk - the returned chunkNo is non-negative and ctrlType is ""
// control chunk - the chunkNo is -1 and ctrlType is a non-empty string
// active chunk - the returned xactID is ""
// temporary chunk - the xactID is a non-empty string
func (f *Fs) parseChunkName(filePath string) (parentPath string, chunkNo int, ctrlType, xactID string) {
	dir, name := path.Split(filePath)
	match := f.nameRegexp.FindStringSubmatch(name)
	if match == nil || match[1] == "" {
		return "", -1, "", ""
	}
	var err error

	chunkNo = -1
	if match[2] != "" {
		if chunkNo, err = strconv.Atoi(match[2]); err != nil {
			chunkNo = -1
		}
		if chunkNo -= f.opt.StartFrom; chunkNo < 0 {
			fs.Infof(f, "invalid data chunk number in file %q", name)
			return "", -1, "", ""
		}
	}

	if match[4] != "" {
		xactID = match[4]
	}
	if match[5] != "" {
		// old-style temporary suffix
		number, err := strconv.ParseInt(match[5], 10, 64)
		if err != nil || number < 0 {
			fs.Infof(f, "invalid old-style transaction number in file %q", name)
			return "", -1, "", ""
		}
		// convert old-style transaction number to base-36 transaction ID
		xactID = fmt.Sprintf(tempSuffixFormat, strconv.FormatInt(number, 36))
		xactID = xactID[1:] // strip leading underscore
	}

	parentPath = dir + match[1]
	ctrlType = match[3]
	return
}

// forbidChunk prints error message or raises error if file is chunk.
// First argument sets log prefix, use `false` to suppress message.
func (f *Fs) forbidChunk(o any, filePath string) error {
	if parentPath, _, _, _ := f.parseChunkName(filePath); parentPath != "" {
		if f.opt.FailHard {
			return fmt.Errorf("chunk overlap with %q", parentPath)
		}
		if boolVal, isBool := o.(bool); !isBool || boolVal {
			fs.Errorf(o, "chunk overlap with %q", parentPath)
		}
	}
	return nil
}

// newXactID produces a sufficiently random transaction identifier.
//
// The temporary suffix mask allows identifiers consisting of 4-9
// base-36 digits (ie. digits 0-9 or lowercase letters a-z).
// The identifiers must be unique between transactions running on
// the single file in parallel.
//
// Currently the function produces 6-character identifiers.
// Together with underscore this makes a 7-character temporary suffix.
//
// The first 4 characters isolate groups of transactions by time intervals.
// The maximum length of interval is base-36 "zzzz" ie. 1,679,615 seconds.
// The function rather takes a maximum prime closest to this number
// (see https://primes.utm.edu) as the interval length to better safeguard
// against repeating pseudo-random sequences in cases when rclone is
// invoked from a periodic scheduler like unix cron.
// Thus, the interval is slightly more than 19 days 10 hours 33 minutes.
//
// The remaining 2 base-36 digits (in the range from 0 to 1295 inclusive)
// are taken from the local random source.
// This provides about 0.1% collision probability for two parallel
// operations started at the same second and working on the same file.
//
// Non-empty filePath argument enables probing for existing temporary chunk
// to further eliminate collisions.
func (f *Fs) newXactID(ctx context.Context, filePath string) (xactID string, err error) {
	const closestPrimeZzzzSeconds = 1679609
	const maxTwoBase36Digits = 1295

	unixSec := time.Now().Unix()
	if unixSec < 0 {
		unixSec = -unixSec // unlikely but the number must be positive
	}
	circleSec := unixSec % closestPrimeZzzzSeconds
	first4chars := strconv.FormatInt(circleSec, 36)

	for range maxTransactionProbes {
		f.xactIDMutex.Lock()
		randomness := f.xactIDRand.Int63n(maxTwoBase36Digits + 1)
		f.xactIDMutex.Unlock()

		last2chars := strconv.FormatInt(randomness, 36)
		xactID = fmt.Sprintf("%04s%02s", first4chars, last2chars)

		if filePath == "" {
			return
		}
		probeChunk := f.makeChunkName(filePath, 0, "", xactID)
		_, probeErr := f.base.NewObject(ctx, probeChunk)
		if probeErr != nil {
			return
		}
	}

	return "", fmt.Errorf("can't setup transaction for %s", filePath)
}

// List the objects and directories in dir into entries.
// The entries can be returned in any order but should be
// for a complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't found.
//
// Commands normally cleanup all temporary chunks in case of a failure.
// However, if rclone dies unexpectedly, it can leave behind a bunch of
// hidden temporary chunks. List and its underlying chunkEntries()
// silently skip all temporary chunks in the directory. It's okay if
// they belong to an unfinished command running in parallel.
//
// However, there is no way to discover dead temporary chunks atm.
// As a workaround users can use `purge` to forcibly remove the whole
// directory together with dead chunks.
// In future a flag named like `--chunker-list-hidden` may be added to
// rclone that will tell List to reveal hidden chunks.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	entries, err = f.base.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	return f.processEntries(ctx, entries, dir)
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
//
// dir should be "" to start from the root, and should not
// have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
//
// It should call callback for each tranche of entries read.
// These need not be returned in any particular order.  If
// callback returns an error then the listing will stop
// immediately.
//
// Don't implement this unless you have a more efficient way
// of listing recursively than doing a directory traversal.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) (err error) {
	do := f.base.Features().ListR
	return do(ctx, dir, func(entries fs.DirEntries) error {
		newEntries, err := f.processEntries(ctx, entries, dir)
		if err != nil {
			return err
		}
		return callback(newEntries)
	})
}

// processEntries assembles chunk entries into composite entries
func (f *Fs) processEntries(ctx context.Context, origEntries fs.DirEntries, dirPath string) (newEntries fs.DirEntries, err error) {
	var sortedEntries fs.DirEntries
	if f.dirSort {
		// sort entries so that meta objects go before their chunks
		sortedEntries = make(fs.DirEntries, len(origEntries))
		copy(sortedEntries, origEntries)
		sort.Sort(sortedEntries)
	} else {
		sortedEntries = origEntries
	}

	byRemote := make(map[string]*Object)
	badEntry := make(map[string]bool)
	isSubdir := make(map[string]bool)
	txnByRemote := map[string]string{}

	var tempEntries fs.DirEntries
	for _, dirOrObject := range sortedEntries {
		switch entry := dirOrObject.(type) {
		case fs.Object:
			remote := entry.Remote()
			mainRemote, chunkNo, ctrlType, xactID := f.parseChunkName(remote)
			if mainRemote == "" {
				// this is meta object or standalone file
				object := f.newObject("", entry, nil)
				byRemote[remote] = object
				tempEntries = append(tempEntries, object)
				if f.useNoRename {
					txnByRemote[remote], err = object.readXactID(ctx)
					if err != nil {
						return nil, err
					}
				}
				break
			}
			// this is some kind of chunk
			// metobject should have been created above if present
			mainObject := byRemote[mainRemote]
			isSpecial := xactID != txnByRemote[mainRemote] || ctrlType != ""
			if mainObject == nil && f.useMeta && !isSpecial {
				fs.Debugf(f, "skip orphan data chunk %q", remote)
				break
			}
			if mainObject == nil && !f.useMeta {
				// this is the "nometa" case
				// create dummy chunked object without metadata
				mainObject = f.newObject(mainRemote, nil, nil)
				byRemote[mainRemote] = mainObject
				if !badEntry[mainRemote] {
					tempEntries = append(tempEntries, mainObject)
				}
			}
			if isSpecial {
				if revealHidden {
					fs.Infof(f, "ignore non-data chunk %q", remote)
				}
				// need to read metadata to ensure actual object type
				// no need to read if metaobject is too big or absent,
				// use the fact that before calling validate()
				// the `size` field caches metaobject size, if any
				if f.useMeta && mainObject != nil && mainObject.size <= maxMetadataSize {
					mainObject.unsure = true
				}
				break
			}
			if err := mainObject.addChunk(entry, chunkNo); err != nil {
				if f.opt.FailHard {
					return nil, err
				}
				badEntry[mainRemote] = true
			}
		case fs.Directory:
			isSubdir[entry.Remote()] = true
			wrapDir := fs.NewDirWrapper(entry.Remote(), entry)
			tempEntries = append(tempEntries, wrapDir)
		default:
			if f.opt.FailHard {
				return nil, fmt.Errorf("unknown object type %T", entry)
			}
			fs.Debugf(f, "unknown object type %T", entry)
		}
	}

	for _, entry := range tempEntries {
		if object, ok := entry.(*Object); ok {
			remote := object.Remote()
			if isSubdir[remote] {
				if f.opt.FailHard {
					return nil, fmt.Errorf("%q is both meta object and directory", remote)
				}
				badEntry[remote] = true // fall thru
			}
			if badEntry[remote] {
				fs.Debugf(f, "invalid directory entry %q", remote)
				continue
			}
			if err := object.validate(); err != nil {
				if f.opt.FailHard {
					return nil, err
				}
				fs.Debugf(f, "invalid chunks in object %q", remote)
				continue
			}
		}
		newEntries = append(newEntries, entry)
	}

	if f.dirSort {
		sort.Sort(newEntries)
	}
	return newEntries, nil
}

// NewObject finds the Object at remote.
//
// Please note that every NewObject invocation will scan the whole directory.
// Using here something like fs.DirCache might improve performance
// (yet making the logic more complex).
//
// Note that chunker prefers analyzing file names rather than reading
// the content of meta object assuming that directory scans are fast
// but opening even a small file can be slow on some backends.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.scanObject(ctx, remote, false)
}

// scanObject is like NewObject with optional quick scan mode.
// The quick mode avoids directory requests other than `List`,
// ignores non-chunked objects and skips chunk size checks.
func (f *Fs) scanObject(ctx context.Context, remote string, quickScan bool) (fs.Object, error) {
	if err := f.forbidChunk(false, remote); err != nil {
		return nil, fmt.Errorf("can't access: %w", err)
	}

	var (
		o             *Object
		baseObj       fs.Object
		currentXactID string
		err           error
		sameMain      bool
	)

	if f.useMeta {
		baseObj, err = f.base.NewObject(ctx, remote)
		if err != nil {
			return nil, err
		}
		remote = baseObj.Remote()

		// Chunker's meta object cannot be large and maxMetadataSize acts
		// as a hard limit. Anything larger than that is treated as a
		// non-chunked file without even checking its contents, so it's
		// paramount to prevent metadata from exceeding the maximum size.
		// Anything smaller is additionally checked for format.
		o = f.newObject("", baseObj, nil)
		if o.size > maxMetadataSize {
			return o, nil
		}
	} else {
		// Metadata is disabled, hence this is either a multi-chunk
		// composite file without meta object or a non-chunked file.
		// Create an empty wrapper here, scan directory to determine
		// which case it is and postpone reading if it's the latter one.
		o = f.newObject(remote, nil, nil)
	}

	// If the object is small, it's probably a meta object.
	// However, composite file must have data chunks besides it.
	// Scan directory for possible data chunks now and decide later on.
	dir := path.Dir(strings.TrimRight(remote, "/"))
	if dir == "." {
		dir = ""
	}
	entries, err := f.base.List(ctx, dir)
	switch err {
	case nil:
		// OK, fall thru
	case fs.ErrorDirNotFound:
		entries = nil
	default:
		return nil, fmt.Errorf("can't detect composite file: %w", err)
	}

	if f.useNoRename {
		currentXactID, err = o.readXactID(ctx)
		if err != nil {
			return nil, err
		}
	}
	caseInsensitive := f.features.CaseInsensitive

	for _, dirOrObject := range entries {
		entry, ok := dirOrObject.(fs.Object)
		if !ok {
			continue
		}
		entryRemote := entry.Remote()
		if !caseInsensitive && !strings.Contains(entryRemote, remote) {
			continue // bypass regexp to save cpu
		}
		mainRemote, chunkNo, ctrlType, xactID := f.parseChunkName(entryRemote)
		if mainRemote == "" {
			continue // skip non-chunks
		}
		if caseInsensitive {
			sameMain = strings.EqualFold(mainRemote, remote)
			if sameMain && f.base.Features().IsLocal {
				// on local, make sure the EqualFold still holds true when accounting for encoding.
				// sometimes paths with special characters will only normalize the same way in Standard Encoding.
				sameMain = strings.EqualFold(encoder.OS.FromStandardPath(mainRemote), encoder.OS.FromStandardPath(remote))
			}
		} else {
			sameMain = mainRemote == remote
		}
		if !sameMain {
			continue // skip alien chunks
		}
		if ctrlType != "" || xactID != currentXactID {
			if f.useMeta {
				// temporary/control chunk calls for lazy metadata read
				o.unsure = true
			}
			continue
		}
		// fs.Debugf(f, "%q belongs to %q as chunk %d", entryRemote, mainRemote, chunkNo)
		if err := o.addChunk(entry, chunkNo); err != nil {
			return nil, err
		}
	}

	if o.main == nil && len(o.chunks) == 0 {
		// Scanning hasn't found data chunks with conforming names.
		if f.useMeta || quickScan {
			// Metadata is required but absent and there are no chunks.
			return nil, fs.ErrorObjectNotFound
		}

		// Data chunks are not found and metadata is disabled.
		// Thus, we are in the "latter case" from above.
		// Let's try the postponed reading of a non-chunked file and add it
		// as a single chunk to the empty composite wrapper created above
		// with nil metadata.
		baseObj, err = f.base.NewObject(ctx, remote)
		if err == nil {
			err = o.addChunk(baseObj, 0)
		}
		if err != nil {
			return nil, err
		}
	}

	// This is either a composite object with metadata or a non-chunked
	// file without metadata. Validate it and update the total data size.
	// As an optimization, skip metadata reading here - we will call
	// readMetadata lazily when needed (reading can be expensive).
	if !quickScan {
		if err := o.validate(); err != nil {
			return nil, err
		}
	}
	return o, nil
}

// readMetadata reads composite object metadata and caches results,
// in case of critical errors metadata is not cached.
// Returns ErrMetaUnknown if an unsupported metadata format is detected.
// If object is not chunked but marked by List or NewObject for recheck,
// readMetadata will attempt to parse object as composite with fallback
// to non-chunked representation if the attempt fails.
func (o *Object) readMetadata(ctx context.Context) error {
	// return quickly if metadata is absent or has been already cached
	if !o.f.useMeta {
		o.isFull = true
	}
	if o.isFull {
		return nil
	}
	if !o.isComposite() && !o.unsure {
		// this for sure is a non-chunked standalone file
		o.isFull = true
		return nil
	}

	// validate metadata
	metaObject := o.main
	if metaObject.Size() > maxMetadataSize {
		if o.unsure {
			// this is not metadata but a foreign object
			o.unsure = false
			o.chunks = nil  // make isComposite return false
			o.isFull = true // cache results
			return nil
		}
		return ErrMetaTooBig
	}

	// size is within limits, perform consistency checks
	reader, err := metaObject.Open(ctx)
	if err != nil {
		return err
	}
	metadata, err := io.ReadAll(reader)
	_ = reader.Close() // ensure file handle is freed on windows
	if err != nil {
		return err
	}

	switch o.f.opt.MetaFormat {
	case "simplejson":
		metaInfo, madeByChunker, err := unmarshalSimpleJSON(ctx, metaObject, metadata)
		if o.unsure {
			o.unsure = false
			if !madeByChunker {
				// this is not metadata but a foreign object
				o.chunks = nil  // make isComposite return false
				o.isFull = true // cache results
				return nil
			}
		}
		switch err {
		case nil:
			// fall thru
		case ErrMetaTooBig, ErrMetaUnknown:
			return err // return these errors unwrapped for unit tests
		default:
			return fmt.Errorf("invalid metadata: %w", err)
		}
		if o.size != metaInfo.Size() || len(o.chunks) != metaInfo.nChunks {
			return errors.New("metadata doesn't match file size")
		}
		o.md5 = metaInfo.md5
		o.sha1 = metaInfo.sha1
		o.xactID = metaInfo.xactID
	}

	o.isFull = true // cache results
	o.xIDCached = true
	return nil
}

// readXactID returns the transaction ID stored in the passed metadata object
func (o *Object) readXactID(ctx context.Context) (xactID string, err error) {
	// if xactID has already been read and cached return it now
	if o.xIDCached {
		return o.xactID, nil
	}
	// Avoid reading metadata for backends that don't use xactID to identify permanent chunks
	if !o.f.useNoRename {
		return "", errors.New("readXactID requires norename transactions")
	}
	if o.main == nil {
		return "", errors.New("readXactID requires valid metaobject")
	}
	if o.main.Size() > maxMetadataSize {
		return "", nil // this was likely not a metadata object, return empty xactID but don't throw error
	}
	reader, err := o.main.Open(ctx)
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close() // ensure file handle is freed on windows
	if err != nil {
		return "", err
	}

	switch o.f.opt.MetaFormat {
	case "simplejson":
		if len(data) > maxMetadataSizeWritten {
			return "", nil // this was likely not a metadata object, return empty xactID but don't throw error
		}
		var metadata metaSimpleJSON
		err = json.Unmarshal(data, &metadata)
		if err != nil {
			return "", nil // this was likely not a metadata object, return empty xactID but don't throw error
		}
		xactID = metadata.XactID
	}
	o.xactID = xactID
	o.xIDCached = true
	return xactID, nil
}

func (f *Fs) checkUploadTarget(ctx context.Context, src fs.ObjectInfo, remote, action string, target fs.Object) (fs.Object, error) {
	if err := f.forbidChunk(src, remote); err != nil {
		return nil, fmt.Errorf("%s refused: %w", action, err)
	}
	if target == nil {
		// Get target object with a quick directory scan
		// skip metadata check if target object does not exist.
		// ignore not-chunked objects, skip chunk size checks.
		if obj, err := f.scanObject(ctx, remote, true); err == nil {
			target = obj
		}
	}
	if target != nil {
		obj := target.(*Object)
		if err := obj.readMetadata(ctx); err == ErrMetaUnknown {
			// refuse to update a file of unsupported format
			return nil, fmt.Errorf("refusing to %s: %w", action, err)
		}
	}
	return target, nil
}

// putMultipart implements the writer-centered upload path.
func (f *Fs) putMultipart(ctx context.Context, in io.Reader, src fs.ObjectInfo, remote string, options []fs.OpenOption, action string, target fs.Object) (fs.Object, error) {
	if _, err := f.checkUploadTarget(ctx, src, remote, action, target); err != nil {
		return nil, err
	}

	src = f.wrapInfo(src, remote, src.Size())
	_, err := multipart.UploadMultipart(ctx, src, in, multipart.UploadMultipartOptions{
		Open:        chunkerMultipartUpload{f: f},
		OpenOptions: options,
	})
	if err != nil {
		return nil, err
	}
	return f.NewObject(ctx, remote)
}

func (f *Fs) finalizeUpload(
	ctx context.Context,
	st *chunkerUploadState,
	basePut putFn,
	updateHashes func() error,
) (fs.Object, error) {
	// Validate uploaded size
	if st.sizeTotal != -1 && st.readCount != st.sizeTotal {
		return nil, fmt.Errorf("incorrect upload size %d != %d", st.readCount, st.sizeTotal)
	}

	// Check for input that looks like valid metadata
	needMeta := len(st.chunks) > 1
	if st.readCount <= maxMetadataSize && len(st.chunks) == 1 {
		_, madeByChunker, _ := unmarshalSimpleJSON(ctx, st.chunks[0], st.smallHead)
		needMeta = madeByChunker
	}

	// Finalize small object as non-chunked.
	// This can be bypassed, and single chunk with metadata will be
	// created if forced by consistent hashing or due to unsafe input.
	if !needMeta && !f.hashAll && f.useMeta {
		// If previous object was chunked, remove its chunks
		f.removeOldChunks(ctx, st.remote)

		// Rename single data chunk in place
		chunk := st.chunks[0]
		if chunk.Remote() != st.remote {
			chunkMoved, errMove := f.baseMove(ctx, chunk, st.remote, delAlways)
			if errMove != nil {
				silentlyRemove(ctx, chunk)
				return nil, errMove
			}
			chunk = chunkMoved
		}

		return f.newObject("", chunk, nil), nil
	}

	// Validate total size of data chunks
	var sizeTotal int64
	for _, chunk := range st.chunks {
		sizeTotal += chunk.Size()
	}
	if sizeTotal != st.readCount {
		return nil, fmt.Errorf("incorrect chunks size %d != %d", sizeTotal, st.readCount)
	}

	var metadata []byte
	if f.useMeta {
		var err error
		metadataXactID := st.xactID
		if !f.useNoRename {
			metadataXactID = ""
		}
		switch f.opt.MetaFormat {
		case "simplejson":
			if err = updateHashes(); err != nil {
				return nil, err
			}
			metadata, err = marshalSimpleJSON(ctx, sizeTotal, len(st.chunks), st.md5, st.sha1, metadataXactID)
		}
		if err != nil {
			return nil, err
		}
	}

	// If previous object was chunked, remove its chunks
	f.removeOldChunks(ctx, st.remote)

	if !f.useNoRename {
		// The transaction suffix will be removed for backends with quick rename operations
		for chunkNo, chunk := range st.chunks {
			chunkRemote := f.makeChunkName(st.remote, chunkNo, "", "")
			chunkMoved, errMove := f.baseMove(ctx, chunk, chunkRemote, delFailed)
			if errMove != nil {
				return nil, errMove
			}
			st.chunks[chunkNo] = chunkMoved
		}
		st.xactID = ""
	}

	if !f.useMeta {
		// Remove stale metadata, if any
		oldMeta, errOldMeta := f.base.NewObject(ctx, st.remote)
		if errOldMeta == nil {
			silentlyRemove(ctx, oldMeta)
		}

		o := f.newObject(st.remote, nil, st.chunks)
		o.size = sizeTotal
		return o, nil
	}

	// Update meta object
	var err error
	metaInfo := f.wrapInfo(st.src, st.remote, int64(len(metadata)))
	st.metaObject, err = basePut(ctx, bytes.NewReader(metadata), metaInfo)
	if err != nil {
		return nil, err
	}

	o := f.newObject("", st.metaObject, st.chunks)
	o.size = sizeTotal
	o.xactID = st.xactID
	return o, nil
}

type putFn func(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error)

type chunkerMultipartUpload struct {
	f *Fs
}

func (u chunkerMultipartUpload) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (fs.ChunkWriterInfo, fs.ChunkWriter, error) {
	var info fs.ChunkWriterInfo
	chunkWriter, err := u.f.newChunkerChunkWriter(ctx, remote, src, false, options...)
	if err != nil {
		return info, nil, err
	}
	info = fs.ChunkWriterInfo{
		ChunkSize:   int64(u.f.opt.ChunkSize),
		Concurrency: 1,
		// Chunker writer v1 relies on same-session rollback and removes temporary chunks on error rather than leaving parts for durable resume.
		LeavePartsOnError: false,
	}
	return info, chunkWriter, nil
}

// chunkerChunkWriter writes chunker uploads through fs.OpenChunkWriter.
type chunkerChunkWriter struct {
	ctx              context.Context
	f                *Fs
	st               *chunkerUploadState
	basePut          putFn
	options          []fs.OpenOption
	hasher           gohash.Hash
	hashChunk        int
	hashOrdered      bool
	hashBuffers      map[int]*pool.RW
	chunkSize        int64
	sizeKnown        bool
	sizeWritten      int64
	forcedConcurrent bool
	concurrentBuffer bool
	written          map[int]fs.Object
	writing          map[int]struct{}
	mu               sync.Mutex
	cond             *sync.Cond
	inflight         int
	closing          bool
	finalizing       bool
	aborting         bool
	closed           bool
	aborted          bool
}

var _ fs.ChunkWriter = (*chunkerChunkWriter)(nil)

func (f *Fs) newChunkerChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, useConcurrentConfig bool, options ...fs.OpenOption) (*chunkerChunkWriter, error) {
	sizeTotal := src.Size()
	sizeKnown := sizeTotal >= 0
	if !sizeKnown {
		sizeTotal = -1
	}
	ci := fs.GetConfig(ctx)
	forcedConcurrent := useConcurrentConfig && ci.MultiThreadSet && ci.MultiThreadStreams > 1

	xactID, err := f.newXactID(ctx, remote)
	if err != nil {
		return nil, err
	}

	hasher, md5sum, sha1sum := f.prepareChunkWriterHashes(ctx, src)
	w := &chunkerChunkWriter{
		ctx: ctx,
		f:   f,
		st: &chunkerUploadState{
			src:       src,
			remote:    remote,
			sizeTotal: sizeTotal,
			xactID:    xactID,
			md5:       md5sum,
			sha1:      sha1sum,
		},
		basePut:          f.base.Put,
		options:          append([]fs.OpenOption(nil), options...),
		hasher:           hasher,
		hashOrdered:      hasher != nil,
		hashBuffers:      make(map[int]*pool.RW),
		chunkSize:        int64(f.opt.ChunkSize),
		sizeKnown:        sizeKnown,
		forcedConcurrent: forcedConcurrent,
		concurrentBuffer: f.opt.ConcurrentBuffer.Valid && f.opt.ConcurrentBuffer.Value,
		written:          make(map[int]fs.Object),
		writing:          make(map[int]struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	return w, nil
}

func (f *Fs) prepareChunkWriterHashes(ctx context.Context, src fs.ObjectInfo) (gohash.Hash, string, string) {
	hashSrc := chunkWriterHashSource(src)
	switch {
	case f.useMD5:
		srcObj := fs.UnWrapObjectInfo(hashSrc)
		if srcObj != nil && srcObj.Fs().Features().SlowHash {
			fs.Debugf(src, "skip slow MD5 on source file, hashing from uploaded chunks")
			return md5.New(), "", ""
		}
		if md5sum, _ := hashSrc.Hash(ctx, hash.MD5); md5sum != "" {
			return nil, md5sum, ""
		}
		if f.hashFallback {
			sha1sum, _ := hashSrc.Hash(ctx, hash.SHA1)
			return nil, "", sha1sum
		}
		return md5.New(), "", ""
	case f.useSHA1:
		srcObj := fs.UnWrapObjectInfo(hashSrc)
		if srcObj != nil && srcObj.Fs().Features().SlowHash {
			fs.Debugf(src, "skip slow SHA1 on source file, hashing from uploaded chunks")
			return sha1.New(), "", ""
		}
		if sha1sum, _ := hashSrc.Hash(ctx, hash.SHA1); sha1sum != "" {
			return nil, "", sha1sum
		}
		if f.hashFallback {
			md5sum, _ := hashSrc.Hash(ctx, hash.MD5)
			return nil, md5sum, ""
		}
		return sha1.New(), "", ""
	default:
		return nil, "", ""
	}
}

func chunkWriterHashSource(src fs.ObjectInfo) fs.ObjectInfo {
	if wrapped, ok := src.(*ObjectInfo); ok {
		return wrapped.src
	}
	return src
}

func checkpointHash(h gohash.Hash) ([]byte, error) {
	m, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return nil, errors.New("hash does not support checkpoint")
	}
	return m.MarshalBinary()
}

func restoreHash(h gohash.Hash, checkpoint []byte) error {
	u, ok := h.(encoding.BinaryUnmarshaler)
	if !ok {
		return errors.New("hash does not support restore")
	}
	return u.UnmarshalBinary(checkpoint)
}

func (w *chunkerChunkWriter) commitHashBuffersLocked() error {
	for w.hashOrdered {
		buffer := w.hashBuffers[w.hashChunk]
		if buffer == nil {
			return nil
		}
		delete(w.hashBuffers, w.hashChunk)
		if _, err := buffer.Seek(0, io.SeekStart); err != nil {
			_ = buffer.Close()
			w.hashOrdered = false
			return err
		}
		if _, err := io.Copy(w.hasher, buffer); err != nil {
			_ = buffer.Close()
			w.hashOrdered = false
			return err
		}
		if err := buffer.Close(); err != nil {
			w.hashOrdered = false
			return err
		}
		w.hashChunk++
	}
	return nil
}

func (w *chunkerChunkWriter) updateHashesFromChunks(ctx context.Context) error {
	if w.hasher == nil {
		return nil
	}
	if w.hashOrdered && w.hashChunk == len(w.st.chunks) {
		switch {
		case w.f.useMD5:
			w.st.md5 = hex.EncodeToString(w.hasher.Sum(nil))
		case w.f.useSHA1:
			w.st.sha1 = hex.EncodeToString(w.hasher.Sum(nil))
		}
		return nil
	}

	w.hasher.Reset()
	// Multipart currently asks chunker for Concurrency=1, but direct writer callers
	// can still finish chunks out of order, so hash from stored chunks by number.
	for _, chunk := range w.st.chunks {
		reader, err := chunk.Open(ctx)
		if err != nil {
			return err
		}
		_, err = io.Copy(w.hasher, reader)
		closeErr := reader.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}

	switch {
	case w.f.useMD5:
		w.st.md5 = hex.EncodeToString(w.hasher.Sum(nil))
	case w.f.useSHA1:
		w.st.sha1 = hex.EncodeToString(w.hasher.Sum(nil))
	}
	return nil
}

// OpenChunkWriter opens a chunked writer for chunker uploads.
func (f *Fs) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (info fs.ChunkWriterInfo, writer fs.ChunkWriter, err error) {
	if parentPath, _, _, _ := f.parseChunkName(remote); parentPath != "" {
		return info, nil, fmt.Errorf("chunk writer refused: chunk overlap with %q", parentPath)
	}

	if obj, errScan := f.scanObject(ctx, remote, true); errScan == nil {
		if errMeta := obj.(*Object).readMetadata(ctx); errMeta == ErrMetaUnknown {
			return info, nil, fmt.Errorf("refusing to open chunk writer: %w", errMeta)
		}
	}

	chunkWriter, err := f.newChunkerChunkWriter(ctx, remote, src, true, options...)
	if err != nil {
		return info, nil, err
	}
	info = fs.ChunkWriterInfo{
		ChunkSize:   int64(f.opt.ChunkSize),
		Concurrency: 1,
		// Chunker writer v1 relies on same-session rollback and removes temporary chunks on error rather than leaving parts for durable resume.
		LeavePartsOnError: false,
	}
	return info, chunkWriter, nil
}

// WriteChunk writes one temporary hidden data chunk.
func (w *chunkerChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	if chunkNumber < 0 {
		return 0, fmt.Errorf("invalid chunk number %d", chunkNumber)
	}
	if chunkNumber > maxSafeChunkNumber {
		return 0, ErrChunkOverflow
	}

	w.mu.Lock()
	switch {
	case w.closed || w.closing || w.finalizing:
		w.mu.Unlock()
		return 0, errors.New("chunk writer is closed")
	case w.aborted || w.aborting:
		w.mu.Unlock()
		return 0, errors.New("chunk writer is aborted")
	}
	if _, ok := w.written[chunkNumber]; ok {
		w.mu.Unlock()
		return 0, fmt.Errorf("duplicate chunk number %d", chunkNumber+w.f.opt.StartFrom)
	}
	if chunkNumber < len(w.st.chunks) && w.st.chunks[chunkNumber] != nil {
		w.mu.Unlock()
		return 0, fmt.Errorf("duplicate chunk number %d", chunkNumber+w.f.opt.StartFrom)
	}
	if _, ok := w.writing[chunkNumber]; ok {
		w.mu.Unlock()
		return 0, fmt.Errorf("duplicate chunk number %d", chunkNumber+w.f.opt.StartFrom)
	}
	w.writing[chunkNumber] = struct{}{}
	w.inflight++
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		delete(w.writing, chunkNumber)
		w.inflight--
		w.cond.Broadcast()
		w.mu.Unlock()
	}()

	chunkSize, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("failed to seek chunk: %w", err)
	}
	if _, err = reader.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek chunk: %w", err)
	}

	chunkRemote := w.f.makeChunkName(w.st.remote, chunkNumber, "", w.st.xactID)
	chunkInfo := w.f.wrapInfo(w.st.src, chunkRemote, chunkSize)
	uploadReader := reader
	var buffer *pool.RW
	bufferOwned := false
	if w.forcedConcurrent && w.concurrentBuffer {
		buffer = multipart.NewRW().Reserve(chunkSize)
		_, err = io.Copy(buffer, reader)
		if err != nil {
			_ = buffer.Close()
			return 0, fmt.Errorf("failed to buffer chunk: %w", err)
		}
		defer func() {
			if !bufferOwned {
				_ = buffer.Close()
			}
		}()
		uploadReader = buffer
	}

	var hashCheckpoint []byte
	hashThisChunk := false
	if w.hasher != nil && !w.forcedConcurrent {
		w.mu.Lock()
		hashThisChunk = w.hashOrdered && chunkNumber == w.hashChunk
		if hashThisChunk {
			hashCheckpoint, err = checkpointHash(w.hasher)
			if err != nil {
				w.hashOrdered = false
				hashThisChunk = false
			}
		} else {
			w.hashOrdered = false
		}
		w.mu.Unlock()
	}

	var chunk fs.Object
	firstAttempt := true
	err = fs.NewPacer(ctx, pacer.NewDefault()).Call(func() (bool, error) {
		if !firstAttempt && hashThisChunk {
			if err := restoreHash(w.hasher, hashCheckpoint); err != nil {
				return false, err
			}
		}
		firstAttempt = false
		if _, err := uploadReader.Seek(0, io.SeekStart); err != nil {
			return false, fmt.Errorf("failed to seek chunk: %w", err)
		}

		putReader := io.Reader(uploadReader)
		if hashThisChunk {
			putReader = io.TeeReader(uploadReader, w.hasher)
		}
		chunk, err = w.basePut(ctx, putReader, chunkInfo, w.options...)
		if err == nil {
			return false, nil
		}

		if hashThisChunk {
			restoreErr := restoreHash(w.hasher, hashCheckpoint)
			if restoreErr != nil {
				err = errors.Join(err, restoreErr)
			}
		}
		w.cleanupFailedChunk(ctx, chunkRemote, chunk)
		chunk = nil
		return shouldRetryChunkWriterPut(ctx, err)
	})
	if err != nil {
		return 0, err
	}

	var commitErr error
	w.mu.Lock()
	switch {
	case w.closed || w.finalizing:
		commitErr = errors.New("chunk writer is closed")
	case w.aborted:
		commitErr = errors.New("chunk writer is aborted")
	}
	if commitErr == nil {
		if _, ok := w.written[chunkNumber]; ok {
			commitErr = fmt.Errorf("duplicate chunk number %d", chunkNumber+w.f.opt.StartFrom)
		}
	}
	if commitErr == nil && chunkNumber < len(w.st.chunks) && w.st.chunks[chunkNumber] != nil {
		commitErr = fmt.Errorf("duplicate chunk number %d", chunkNumber+w.f.opt.StartFrom)
	}
	if commitErr != nil {
		w.mu.Unlock()
		w.cleanupFailedChunk(ctx, chunkRemote, chunk)
		return 0, commitErr
	}
	if chunkNumber == len(w.st.chunks) {
		w.st.chunks = append(w.st.chunks, chunk)
	} else {
		if chunkNumber > len(w.st.chunks) {
			chunks := make([]fs.Object, chunkNumber+1)
			copy(chunks, w.st.chunks)
			w.st.chunks = chunks
		}
		w.st.chunks[chunkNumber] = chunk
	}
	w.written[chunkNumber] = chunk
	w.st.readCount += chunkSize
	if w.sizeKnown {
		w.sizeWritten += chunkSize
	}
	if hashThisChunk {
		w.hashChunk++
	}
	if w.hasher != nil && w.forcedConcurrent && w.concurrentBuffer && buffer != nil {
		w.hashBuffers[chunkNumber] = buffer
		bufferOwned = true
		if err := w.commitHashBuffersLocked(); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	}
	w.mu.Unlock()

	return chunkSize, nil
}

func shouldRetryChunkWriterPut(ctx context.Context, err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if fserrors.IsNoRetryError(err) || fserrors.IsNoLowLevelRetryError(err) || fserrors.IsFatalError(err) {
		return false, err
	}
	if _, ok := pacer.IsRetryAfter(err); ok {
		return true, err
	}
	if retryAfter := fserrors.RetryAfterErrorTime(err); !retryAfter.IsZero() {
		sleep := time.Until(retryAfter)
		if sleep < 0 {
			sleep = 0
		}
		return true, pacer.RetryAfterError(err, sleep)
	}
	if fserrors.IsRetryError(err) || fserrors.ShouldRetry(err) {
		return true, err
	}
	return false, err
}

func (w *chunkerChunkWriter) cleanupFailedChunk(ctx context.Context, chunkRemote string, chunk fs.Object) {
	if chunk != nil {
		if err := chunk.Remove(ctx); err != nil {
			fs.Errorf(chunk, "Failed to remove temporary chunk: %v", err)
		}
	}

	chunk, err := w.f.base.NewObject(ctx, chunkRemote)
	if err != nil {
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			fs.Debugf(w.f.base, "Failed to check temporary chunk %q cleanup: %v", chunkRemote, err)
		}
		return
	}
	if err := chunk.Remove(ctx); err != nil {
		fs.Errorf(chunk, "Failed to remove temporary chunk: %v", err)
	}
}

// Close finalizes a hidden chunk writer upload.
func (w *chunkerChunkWriter) Close(ctx context.Context) error {
	w.mu.Lock()
	switch {
	case w.closed || w.closing || w.finalizing:
		w.mu.Unlock()
		return errors.New("chunk writer is closed")
	case w.aborted || w.aborting:
		w.mu.Unlock()
		return errors.New("chunk writer is aborted")
	}
	w.closing = true
	for w.inflight > 0 {
		w.cond.Wait()
	}

	if len(w.st.chunks) == 0 {
		w.closing = false
		w.mu.Unlock()
		return errors.New("missing chunk 0")
	}

	var sizeTotal int64
	for chunkNumber, chunk := range w.st.chunks {
		if chunk == nil {
			w.closing = false
			w.mu.Unlock()
			return fmt.Errorf("missing chunk %d", chunkNumber+w.f.opt.StartFrom)
		}
		sizeTotal += chunk.Size()
	}
	if sizeTotal != w.st.readCount {
		w.closing = false
		w.mu.Unlock()
		return fmt.Errorf("incorrect chunks size %d != %d", sizeTotal, w.st.readCount)
	}
	if w.sizeKnown && sizeTotal != w.st.sizeTotal {
		w.closing = false
		w.mu.Unlock()
		return fmt.Errorf("incorrect upload size %d != %d", sizeTotal, w.st.sizeTotal)
	}
	if w.sizeKnown && w.sizeWritten != w.st.sizeTotal {
		w.closing = false
		w.mu.Unlock()
		return fmt.Errorf("incorrect written size %d != %d", w.sizeWritten, w.st.sizeTotal)
	}
	readCount := w.st.readCount
	var firstChunk fs.Object
	if len(w.st.chunks) == 1 && readCount <= maxMetadataSize {
		firstChunk = w.st.chunks[0]
	}
	w.mu.Unlock()

	if firstChunk != nil {
		reader, err := firstChunk.Open(ctx)
		if err != nil {
			w.mu.Lock()
			w.closing = false
			w.cond.Broadcast()
			w.mu.Unlock()
			return err
		}
		head, err := io.ReadAll(io.LimitReader(reader, maxMetadataSize+1))
		_ = reader.Close()
		if err != nil {
			w.mu.Lock()
			w.closing = false
			w.cond.Broadcast()
			w.mu.Unlock()
			return err
		}
		w.mu.Lock()
		w.st.smallHead = head
		w.mu.Unlock()
	}

	w.mu.Lock()
	w.finalizing = true
	w.cond.Broadcast()
	w.mu.Unlock()

	_, err := w.f.finalizeUpload(ctx, w.st, w.basePut, func() error {
		return w.updateHashesFromChunks(ctx)
	})
	if err != nil {
		w.mu.Lock()
		w.closing = false
		w.cond.Broadcast()
		w.mu.Unlock()
		return err
	}
	w.mu.Lock()
	w.closed = true
	w.closing = false
	w.cond.Broadcast()
	w.mu.Unlock()
	return nil
}

// Abort rolls back chunks uploaded by a hidden chunk writer.
func (w *chunkerChunkWriter) Abort(ctx context.Context) error {
	w.mu.Lock()
	if w.aborted {
		w.mu.Unlock()
		return nil
	}
	if w.aborting {
		for w.aborting && !w.aborted {
			w.cond.Wait()
		}
		w.mu.Unlock()
		return nil
	}
	for w.closing {
		w.cond.Wait()
	}
	if w.aborted {
		w.mu.Unlock()
		return nil
	}
	if w.closed {
		w.mu.Unlock()
		return errors.New("chunk writer is closed")
	}
	w.aborting = true
	for w.inflight > 0 {
		w.cond.Wait()
	}
	w.mu.Unlock()

	w.st.rollback(ctx)

	w.mu.Lock()
	for chunkNumber, buffer := range w.hashBuffers {
		_ = buffer.Close()
		delete(w.hashBuffers, chunkNumber)
	}
	w.aborted = true
	w.aborting = false
	w.cond.Broadcast()
	w.mu.Unlock()
	return nil
}

type chunkerUploadState struct {
	src        fs.ObjectInfo
	remote     string
	chunks     []fs.Object
	metaObject fs.Object
	readCount  int64
	sizeTotal  int64
	smallHead  []byte
	md5        string
	sha1       string
	xactID     string
}

// rollback removes uploaded temporary chunks
func (st *chunkerUploadState) rollback(ctx context.Context) {
	if st.metaObject != nil {
		st.chunks = append(st.chunks, st.metaObject)
	}
	for _, chunk := range st.chunks {
		if chunk == nil {
			continue
		}
		if err := chunk.Remove(ctx); err != nil {
			fs.Errorf(chunk, "Failed to remove temporary chunk: %v", err)
		}
	}
}

func (f *Fs) removeOldChunks(ctx context.Context, remote string) {
	oldFsObject, err := f.NewObject(ctx, remote)
	if err == nil {
		oldObject := oldFsObject.(*Object)
		for _, chunk := range oldObject.chunks {
			if err := chunk.Remove(ctx); err != nil {
				fs.Errorf(chunk, "Failed to remove old chunk: %v", err)
			}
		}
	}
}

// Put into the remote path with the given modTime and size.
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.putMultipart(ctx, in, src, src.Remote(), options, "put", nil)
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.putMultipart(ctx, in, src, src.Remote(), options, "upload", nil)
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	oNew, err := o.f.putMultipart(ctx, in, src, o.Remote(), options, "update", o)
	if err == nil {
		*o = *oNew.(*Object)
	}
	return err
}

// PutUnchecked uploads the object
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	do := f.base.Features().PutUnchecked
	if do == nil {
		return nil, errors.New("can't PutUnchecked")
	}
	// TODO: handle range/limit options and really chunk stream here!
	o, err := do(ctx, in, f.wrapInfo(src, "", -1))
	if err != nil {
		return nil, err
	}
	return f.newObject("", o, nil), nil
}

// Hashes returns the supported hash sets.
// Chunker advertises a hash type if and only if it can be calculated
// for files of any size, non-chunked or composite.
func (f *Fs) Hashes() hash.Set {
	// composites AND no fallback AND (chunker OR wrapped Fs will hash all non-chunked's)
	if f.useMD5 && !f.hashFallback && (f.hashAll || f.base.Hashes().Contains(hash.MD5)) {
		return hash.NewHashSet(hash.MD5)
	}
	if f.useSHA1 && !f.hashFallback && (f.hashAll || f.base.Hashes().Contains(hash.SHA1)) {
		return hash.NewHashSet(hash.SHA1)
	}
	return hash.NewHashSet() // can't provide strong guarantees
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	if err := f.forbidChunk(dir, dir); err != nil {
		return fmt.Errorf("can't mkdir: %w", err)
	}
	return f.base.Mkdir(ctx, dir)
}

// MkdirMetadata makes the root directory of the Fs object
func (f *Fs) MkdirMetadata(ctx context.Context, dir string, metadata fs.Metadata) (fs.Directory, error) {
	if do := f.base.Features().MkdirMetadata; do != nil {
		return do(ctx, dir, metadata)
	}
	return nil, fs.ErrorNotImplemented
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.base.Rmdir(ctx, dir)
}

// Purge all files in the directory
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist.
//
// This command will chain to `purge` from wrapped remote.
// As a result it removes not only composite chunker files with their
// active chunks but also all hidden temporary chunks in the directory.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	do := f.base.Features().Purge
	if do == nil {
		return fs.ErrorCantPurge
	}
	return do(ctx, dir)
}

// Remove an object (chunks and metadata, if any)
//
// Remove deletes only active chunks of the composite object.
// It does not try to look for temporary chunks because they could belong
// to another command modifying this composite file in parallel.
//
// Commands normally cleanup all temporary chunks in case of a failure.
// However, if rclone dies unexpectedly, it can leave hidden temporary
// chunks, which cannot be discovered using the `list` command.
// Remove does not try to search for such chunks or to delete them.
// Sometimes this can lead to strange results e.g. when `list` shows that
// directory is empty but `rmdir` refuses to remove it because on the
// level of wrapped remote it's actually *not* empty.
// As a workaround users can use `purge` to forcibly remove it.
//
// In future, a flag `--chunker-delete-hidden` may be added which tells
// Remove to search directory for hidden chunks and remove them too
// (at the risk of breaking parallel commands).
//
// Remove is the only operation allowed on the composite files with
// invalid or future metadata format.
// We don't let user copy/move/update unsupported composite files.
// Let's at least let her get rid of them, just complain loudly.
//
// This can litter directory with orphan chunks of unsupported types,
// but as long as we remove meta object, even future releases will
// treat the composite file as removed and refuse to act upon it.
//
// Disclaimer: corruption can still happen if unsupported file is removed
// and then recreated with the same name.
// Unsupported control chunks will get re-picked by a more recent
// rclone version with unexpected results. This can be helped by
// the `delete hidden` flag above or at least the user has been warned.
func (o *Object) Remove(ctx context.Context) (err error) {
	if err := o.f.forbidChunk(o, o.Remote()); err != nil {
		// operations.Move can still call Remove if chunker's Move refuses
		// to corrupt file in hard mode. Hence, refuse to Remove, too.
		return fmt.Errorf("refuse to corrupt: %w", err)
	}
	if err := o.readMetadata(ctx); err == ErrMetaUnknown {
		// Proceed but warn user that unexpected things can happen.
		fs.Errorf(o, "Removing a file with unsupported metadata: %v", err)
	}

	// Remove non-chunked file or meta object of a composite file.
	if o.main != nil {
		err = o.main.Remove(ctx)
	}

	// Remove only active data chunks, ignore any temporary chunks that
	// might probably be created in parallel by other transactions.
	for _, chunk := range o.chunks {
		chunkErr := chunk.Remove(ctx)
		if err == nil {
			err = chunkErr
		}
	}

	// There are no known control chunks to remove atm.
	return err
}

// copyOrMove implements copy or move
func (f *Fs) copyOrMove(ctx context.Context, o *Object, remote string, do copyMoveFn, md5, sha1, opName string) (fs.Object, error) {
	if err := f.forbidChunk(o, remote); err != nil {
		return nil, fmt.Errorf("can't %s: %w", opName, err)
	}
	if err := o.readMetadata(ctx); err != nil {
		// Refuse to copy/move composite files with invalid or future
		// metadata format which might involve unsupported chunk types.
		return nil, fmt.Errorf("can't %s this file: %w", opName, err)
	}
	if !o.isComposite() {
		fs.Debugf(o, "%s non-chunked object...", opName)
		oResult, err := do(ctx, o.mainChunk(), remote) // chain operation to a single wrapped chunk
		if err != nil {
			return nil, err
		}
		return f.newObject("", oResult, nil), nil
	}

	fs.Debugf(o, "%s %d data chunks...", opName, len(o.chunks))
	mainRemote := o.remote
	var newChunks []fs.Object
	var err error

	// Copy/move active data chunks.
	// Ignore possible temporary chunks being created by parallel operations.
	for _, chunk := range o.chunks {
		chunkRemote := chunk.Remote()
		if !strings.HasPrefix(chunkRemote, mainRemote) {
			err = fmt.Errorf("invalid chunk name %q", chunkRemote)
			break
		}
		chunkSuffix := chunkRemote[len(mainRemote):]
		chunkResult, err := do(ctx, chunk, remote+chunkSuffix)
		if err != nil {
			break
		}
		newChunks = append(newChunks, chunkResult)
	}

	// Copy or move old metadata.
	// There are no known control chunks to move/copy atm.
	var metaObject fs.Object
	if err == nil && o.main != nil {
		metaObject, err = do(ctx, o.main, remote)
	}
	if err != nil {
		for _, chunk := range newChunks {
			silentlyRemove(ctx, chunk)
		}
		return nil, err
	}

	// Create wrapping object, calculate and validate total size
	newObj := f.newObject(remote, metaObject, newChunks)
	err = newObj.validate()
	if err != nil {
		silentlyRemove(ctx, newObj)
		return nil, err
	}

	// Update metadata
	var metadata []byte
	switch f.opt.MetaFormat {
	case "simplejson":
		metadata, err = marshalSimpleJSON(ctx, newObj.size, len(newChunks), md5, sha1, o.xactID)
		if err == nil {
			metaInfo := f.wrapInfo(metaObject, "", int64(len(metadata)))
			err = newObj.main.Update(ctx, bytes.NewReader(metadata), metaInfo)
		}
	case "none":
		if newObj.main != nil {
			err = newObj.main.Remove(ctx)
		}
	}

	// Return the composite object
	if err != nil {
		silentlyRemove(ctx, newObj)
		return nil, err
	}
	return newObj, nil
}

type copyMoveFn func(context.Context, fs.Object, string) (fs.Object, error)

func (f *Fs) okForServerSide(ctx context.Context, src fs.Object, opName string) (obj *Object, md5, sha1 string, ok bool) {
	var diff string
	obj, ok = src.(*Object)

	switch {
	case !ok:
		diff = "remote types"
	case !operations.SameConfig(f.base, obj.f.base):
		diff = "wrapped remotes"
	case f.opt.ChunkSize != obj.f.opt.ChunkSize:
		diff = "chunk sizes"
	case f.opt.NameFormat != obj.f.opt.NameFormat:
		diff = "chunk name formats"
	case f.opt.StartFrom != obj.f.opt.StartFrom:
		diff = "chunk numbering"
	case f.opt.MetaFormat != obj.f.opt.MetaFormat:
		diff = "meta formats"
	}
	if diff != "" {
		fs.Debugf(src, "Can't %s - different %s", opName, diff)
		ok = false
		return
	}

	if obj.unsure {
		// ensure object is composite if need to re-read metadata
		_ = obj.readMetadata(ctx)
	}
	requireMetaHash := obj.isComposite() && f.opt.MetaFormat == "simplejson"
	if !requireMetaHash && !f.hashAll {
		ok = true // hash is not required for metadata
		return
	}

	switch {
	case f.useMD5:
		md5, _ = obj.Hash(ctx, hash.MD5)
		ok = md5 != ""
		if !ok && f.hashFallback {
			sha1, _ = obj.Hash(ctx, hash.SHA1)
			ok = sha1 != ""
		}
	case f.useSHA1:
		sha1, _ = obj.Hash(ctx, hash.SHA1)
		ok = sha1 != ""
		if !ok && f.hashFallback {
			md5, _ = obj.Hash(ctx, hash.MD5)
			ok = md5 != ""
		}
	default:
		ok = false
	}
	if !ok {
		fs.Debugf(src, "Can't %s - required hash not found", opName)
	}
	return
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	baseCopy := f.base.Features().Copy
	if baseCopy == nil {
		return nil, fs.ErrorCantCopy
	}
	obj, md5, sha1, ok := f.okForServerSide(ctx, src, "copy")
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	return f.copyOrMove(ctx, obj, remote, baseCopy, md5, sha1, "copy")
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	baseMove := func(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
		return f.baseMove(ctx, src, remote, delNever)
	}
	obj, md5, sha1, ok := f.okForServerSide(ctx, src, "move")
	if !ok {
		return nil, fs.ErrorCantMove
	}
	return f.copyOrMove(ctx, obj, remote, baseMove, md5, sha1, "move")
}

// baseMove chains to the wrapped Move or simulates it by Copy+Delete
func (f *Fs) baseMove(ctx context.Context, src fs.Object, remote string, delMode int) (fs.Object, error) {
	ctx, ci := fs.AddConfig(ctx)
	ci.NameTransform = nil // ensure operations.Move does not double-transform here
	var (
		dest fs.Object
		err  error
	)
	switch delMode {
	case delAlways:
		dest, err = f.base.NewObject(ctx, remote)
	case delFailed:
		dest, err = operations.Move(ctx, f.base, nil, remote, src)
		if err == nil {
			return dest, err
		}
		dest, err = f.base.NewObject(ctx, remote)
	case delNever:
		// fall thru, the default
	}
	if err != nil {
		dest = nil
	}
	return operations.Move(ctx, f.base, dest, remote, src)
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	do := f.base.Features().DirMove
	if do == nil {
		return fs.ErrorCantDirMove
	}
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
	return do(ctx, srcFs.base, srcRemote, dstRemote)
}

// DirSetModTime sets the directory modtime for dir
func (f *Fs) DirSetModTime(ctx context.Context, dir string, modTime time.Time) error {
	if do := f.base.Features().DirSetModTime; do != nil {
		return do(ctx, dir, modTime)
	}
	return fs.ErrorNotImplemented
}

// CleanUp the trash in the Fs
//
// Implement this if you have a way of emptying the trash or
// otherwise cleaning up old versions of files.
func (f *Fs) CleanUp(ctx context.Context) error {
	do := f.base.Features().CleanUp
	if do == nil {
		return errors.New("not supported by underlying remote")
	}
	return do(ctx)
}

// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	do := f.base.Features().About
	if do == nil {
		return nil, errors.New("not supported by underlying remote")
	}
	return do(ctx)
}

// UnWrap returns the Fs that this Fs is wrapping
func (f *Fs) UnWrap() fs.Fs {
	return f.base
}

// WrapFs returns the Fs that is wrapping this Fs
func (f *Fs) WrapFs() fs.Fs {
	return f.wrapper
}

// SetWrapper sets the Fs that is wrapping this Fs
func (f *Fs) SetWrapper(wrapper fs.Fs) {
	f.wrapper = wrapper
}

// ChangeNotify calls the passed function with a path
// that has had changes. If the implementation
// uses polling, it should adhere to the given interval.
//
// Replace data chunk names by the name of composite file.
// Ignore temporary and control chunks.
func (f *Fs) ChangeNotify(ctx context.Context, notifyFunc func(string, fs.EntryType), pollIntervalChan <-chan time.Duration) {
	do := f.base.Features().ChangeNotify
	if do == nil {
		return
	}
	wrappedNotifyFunc := func(path string, entryType fs.EntryType) {
		// fs.Debugf(f, "ChangeNotify: path %q entryType %d", path, entryType)
		if entryType == fs.EntryObject {
			mainPath, _, _, xactID := f.parseChunkName(path)
			metaXactID := ""
			if f.useNoRename {
				metaObject, _ := f.base.NewObject(ctx, mainPath)
				dummyObject := f.newObject("", metaObject, nil)
				metaXactID, _ = dummyObject.readXactID(ctx)
			}
			if mainPath != "" && xactID == metaXactID {
				path = mainPath
			}
		}
		notifyFunc(path, entryType)
	}
	do(ctx, wrappedNotifyFunc, pollIntervalChan)
}

// Shutdown the backend, closing any background tasks and any
// cached connections.
func (f *Fs) Shutdown(ctx context.Context) error {
	do := f.base.Features().Shutdown
	if do == nil {
		return nil
	}
	return do(ctx)
}

// Object represents a composite file wrapping one or more data chunks
type Object struct {
	remote    string
	main      fs.Object   // meta object if file is composite, or wrapped non-chunked file, nil if meta format is 'none'
	chunks    []fs.Object // active data chunks if file is composite, or wrapped file as a single chunk if meta format is 'none'
	size      int64       // cached total size of chunks in a composite file or -1 for non-chunked files
	isFull    bool        // true if metadata has been read
	xIDCached bool        // true if xactID has been read
	unsure    bool        // true if need to read metadata to detect object type
	xactID    string      // transaction ID for "norename" or empty string for "renamed" chunks
	md5       string
	sha1      string
	f         *Fs
}

func (o *Object) addChunk(chunk fs.Object, chunkNo int) error {
	if chunkNo < 0 {
		return fmt.Errorf("invalid chunk number %d", chunkNo+o.f.opt.StartFrom)
	}
	if chunkNo == len(o.chunks) {
		o.chunks = append(o.chunks, chunk)
		return nil
	}
	if chunkNo > maxSafeChunkNumber {
		return ErrChunkOverflow
	}
	if chunkNo > len(o.chunks) {
		newChunks := make([]fs.Object, (chunkNo + 1), (chunkNo+1)*2)
		copy(newChunks, o.chunks)
		o.chunks = newChunks
	}
	if o.chunks[chunkNo] != nil {
		return fmt.Errorf("duplicate chunk number %d", chunkNo+o.f.opt.StartFrom)
	}
	o.chunks[chunkNo] = chunk
	return nil
}

// validate verifies the object internals and updates total size
func (o *Object) validate() error {
	if !o.isComposite() {
		_ = o.mainChunk() // verify that single wrapped chunk exists
		return nil
	}

	metaObject := o.main // this file is composite - o.main refers to meta object (or nil if meta format is 'none')
	if metaObject != nil && metaObject.Size() > maxMetadataSize {
		// metadata of a chunked file must be a tiny piece of json
		o.size = -1
		return fmt.Errorf("%q metadata is too large", o.remote)
	}

	var totalSize int64
	for _, chunk := range o.chunks {
		if chunk == nil {
			o.size = -1
			return fmt.Errorf("%q has missing chunks", o)
		}
		totalSize += chunk.Size()
	}
	o.size = totalSize // cache up the total data size
	return nil
}

func (f *Fs) newObject(remote string, main fs.Object, chunks []fs.Object) *Object {
	var size int64 = -1
	if main != nil {
		size = main.Size()
		if remote == "" {
			remote = main.Remote()
		}
	}
	return &Object{
		remote: remote,
		main:   main,
		size:   size,
		f:      f,
		chunks: chunks,
	}
}

// mainChunk returns:
// - a wrapped object for non-chunked files
// - meta object for chunked files with metadata
// - first chunk for chunked files without metadata
// Never returns nil.
func (o *Object) mainChunk() fs.Object {
	if o.main != nil {
		return o.main // meta object or non-chunked wrapped file
	}
	if o.chunks != nil {
		return o.chunks[0] // first chunk of a chunked composite file
	}
	panic("invalid chunked object") // very unlikely
}

func (o *Object) isComposite() bool {
	return o.chunks != nil
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.f
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	if o.isComposite() {
		return o.size // total size of data chunks in a composite file
	}
	return o.mainChunk().Size() // size of wrapped non-chunked file
}

// Storable returns whether object is storable
func (o *Object) Storable() bool {
	return o.mainChunk().Storable()
}

// ModTime returns the modification time of the file
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.mainChunk().ModTime(ctx)
}

// SetModTime sets the modification time of the file
func (o *Object) SetModTime(ctx context.Context, mtime time.Time) error {
	if err := o.readMetadata(ctx); err != nil {
		return err // refuse to act on unsupported format
	}
	return o.mainChunk().SetModTime(ctx, mtime)
}

// Hash returns the selected checksum of the file.
// If no checksum is available it returns "".
//
// Hash won't fail with `unsupported` error but return empty
// hash string if a particular hashsum type is not supported
//
// Hash takes hashsum from metadata if available or requests it
// from wrapped remote for non-chunked files.
// Metadata (if meta format is not 'none') is by default kept
// only for composite files. In the "All" hashing mode chunker
// will force metadata on all files if particular hashsum type
// is not supported by wrapped remote.
//
// Note that Hash prefers the wrapped hashsum for non-chunked
// file, then tries to read it from metadata. This in theory
// handles the unusual case when a small file has been tampered
// on the level of wrapped remote but chunker is unaware of that.
func (o *Object) Hash(ctx context.Context, hashType hash.Type) (string, error) {
	if err := o.readMetadata(ctx); err != nil {
		return "", err // valid metadata is required to get hash, abort
	}
	if !o.isComposite() {
		// First, chain to the wrapped non-chunked file if possible.
		if value, err := o.mainChunk().Hash(ctx, hashType); err == nil && value != "" {
			return value, nil
		}
	}

	// Try hash from metadata if the file is composite or if wrapped remote fails.
	switch hashType {
	case hash.MD5:
		if o.md5 == "" {
			return "", nil
		}
		return o.md5, nil
	case hash.SHA1:
		if o.sha1 == "" {
			return "", nil
		}
		return o.sha1, nil
	default:
		return "", hash.ErrUnsupported
	}
}

// UnWrap returns the wrapped Object
func (o *Object) UnWrap() fs.Object {
	return o.mainChunk()
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (rc io.ReadCloser, err error) {
	if err := o.readMetadata(ctx); err != nil {
		// refuse to open unsupported format
		return nil, fmt.Errorf("can't open: %w", err)
	}
	if !o.isComposite() {
		return o.mainChunk().Open(ctx, options...) // chain to wrapped non-chunked file
	}

	var openOptions []fs.OpenOption
	var offset, limit int64 = 0, -1

	for _, option := range options {
		switch opt := option.(type) {
		case *fs.SeekOption:
			offset = opt.Offset
		case *fs.RangeOption:
			offset, limit = opt.Decode(o.size)
		default:
			// pass Options on to the wrapped open, if appropriate
			openOptions = append(openOptions, option)
		}
	}

	if offset < 0 {
		return nil, errors.New("invalid offset")
	}
	if limit < 0 {
		limit = o.size - offset
	}

	return o.newLinearReader(ctx, offset, limit, openOptions)
}

// linearReader opens and reads file chunks sequentially, without read-ahead
type linearReader struct {
	ctx     context.Context
	chunks  []fs.Object
	options []fs.OpenOption
	limit   int64
	count   int64
	pos     int
	reader  io.ReadCloser
	err     error
}

func (o *Object) newLinearReader(ctx context.Context, offset, limit int64, options []fs.OpenOption) (io.ReadCloser, error) {
	r := &linearReader{
		ctx:     ctx,
		chunks:  o.chunks,
		options: options,
		limit:   limit,
	}

	// skip to chunk for given offset
	err := io.EOF
	for offset >= 0 && err != nil {
		offset, err = r.nextChunk(offset)
	}
	if err == nil || err == io.EOF {
		r.err = err
		return r, nil
	}
	return nil, err
}

func (r *linearReader) nextChunk(offset int64) (int64, error) {
	if r.err != nil {
		return -1, r.err
	}
	if r.pos >= len(r.chunks) || r.limit <= 0 || offset < 0 {
		return -1, io.EOF
	}

	chunk := r.chunks[r.pos]
	count := chunk.Size()
	r.pos++

	if offset >= count {
		return offset - count, io.EOF
	}
	count -= offset
	if r.limit < count {
		count = r.limit
	}
	options := append(r.options, &fs.RangeOption{Start: offset, End: offset + count - 1})

	if err := r.Close(); err != nil {
		return -1, err
	}

	reader, err := chunk.Open(r.ctx, options...)
	if err != nil {
		return -1, err
	}

	r.reader = reader
	r.count = count
	return offset, nil
}

func (r *linearReader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.limit <= 0 {
		r.err = io.EOF
		return 0, io.EOF
	}

	for r.count <= 0 {
		// current chunk has been read completely or its size is zero
		off, err := r.nextChunk(0)
		if off < 0 {
			r.err = err
			return 0, err
		}
	}

	n, err = r.reader.Read(p)
	if err == nil || err == io.EOF {
		r.count -= int64(n)
		r.limit -= int64(n)
		if r.limit > 0 {
			err = nil // more data to read
		}
	}
	r.err = err
	return
}

func (r *linearReader) Close() (err error) {
	if r.reader != nil {
		err = r.reader.Close()
		r.reader = nil
	}
	return
}

// ObjectInfo describes a wrapped fs.ObjectInfo for being the source
type ObjectInfo struct {
	src     fs.ObjectInfo
	fs      *Fs
	nChunks int    // number of data chunks
	xactID  string // transaction ID for "norename" or empty string for "renamed" chunks
	size    int64  // overrides source size by the total size of data chunks
	remote  string // overrides remote name
	md5     string // overrides MD5 checksum
	sha1    string // overrides SHA1 checksum
}

func (f *Fs) wrapInfo(src fs.ObjectInfo, newRemote string, totalSize int64) *ObjectInfo {
	return &ObjectInfo{
		src:    src,
		fs:     f,
		size:   totalSize,
		remote: newRemote,
	}
}

// Fs returns read only access to the Fs that this object is part of
func (oi *ObjectInfo) Fs() fs.Info {
	if oi.fs == nil {
		panic("stub ObjectInfo")
	}
	return oi.fs
}

// String returns string representation
func (oi *ObjectInfo) String() string {
	return oi.src.String()
}

// Storable returns whether object is storable
func (oi *ObjectInfo) Storable() bool {
	return oi.src.Storable()
}

// Remote returns the remote path
func (oi *ObjectInfo) Remote() string {
	if oi.remote != "" {
		return oi.remote
	}
	return oi.src.Remote()
}

// Size returns the size of the file
func (oi *ObjectInfo) Size() int64 {
	if oi.size != -1 {
		return oi.size
	}
	return oi.src.Size()
}

// ModTime returns the modification time
func (oi *ObjectInfo) ModTime(ctx context.Context) time.Time {
	return oi.src.ModTime(ctx)
}

// Hash returns the selected checksum of the wrapped file
// It returns "" if no checksum is available or if this
// info doesn't wrap the complete file.
func (oi *ObjectInfo) Hash(ctx context.Context, hashType hash.Type) (string, error) {
	var errUnsupported error
	switch hashType {
	case hash.MD5:
		if oi.md5 != "" {
			return oi.md5, nil
		}
	case hash.SHA1:
		if oi.sha1 != "" {
			return oi.sha1, nil
		}
	default:
		errUnsupported = hash.ErrUnsupported
	}
	if oi.Size() != oi.src.Size() {
		// fail if this info wraps only a part of the file
		return "", errUnsupported
	}
	// chain to full source if possible
	value, err := oi.src.Hash(ctx, hashType)
	if err == hash.ErrUnsupported {
		return "", errUnsupported
	}
	return value, err
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	if doer, ok := o.mainChunk().(fs.IDer); ok {
		return doer.ID()
	}
	return ""
}

// Meta format `simplejson`
type metaSimpleJSON struct {
	// required core fields
	Version  *int   `json:"ver"`
	Size     *int64 `json:"size"`    // total size of data chunks
	ChunkNum *int   `json:"nchunks"` // number of data chunks
	// optional extra fields
	MD5    string `json:"md5,omitempty"`
	SHA1   string `json:"sha1,omitempty"`
	XactID string `json:"txn,omitempty"` // transaction ID for norename transactions
}

// marshalSimpleJSON
//
// Current implementation creates metadata in three cases:
// - for files larger than chunk size
// - if file contents can be mistaken as meta object
// - if consistent hashing is On but wrapped remote can't provide given hash
func marshalSimpleJSON(ctx context.Context, size int64, nChunks int, md5, sha1, xactID string) ([]byte, error) {
	version := metadataVersion
	if xactID == "" && version == 2 {
		version = 1
	}
	metadata := metaSimpleJSON{
		// required core fields
		Version:  &version,
		Size:     &size,
		ChunkNum: &nChunks,
		// optional extra fields
		MD5:    md5,
		SHA1:   sha1,
		XactID: xactID,
	}
	data, err := json.Marshal(&metadata)
	if err == nil && data != nil && len(data) >= maxMetadataSizeWritten {
		// be a nitpicker, never produce something you can't consume
		return nil, errors.New("metadata can't be this big, please report to rclone developers")
	}
	return data, err
}

// unmarshalSimpleJSON parses metadata.
//
// In case of errors returns a flag telling whether input has been
// produced by incompatible version of rclone vs wasn't metadata at all.
// Only metadata format version 1 is supported atm.
// Future releases will transparently migrate older metadata objects.
// New format will have a higher version number and cannot be correctly
// handled by current implementation.
// The version check below will then explicitly ask user to upgrade rclone.
func unmarshalSimpleJSON(ctx context.Context, metaObject fs.Object, data []byte) (info *ObjectInfo, madeByChunker bool, err error) {
	// Be strict about JSON format
	// to reduce possibility that a random small file resembles metadata.
	if len(data) > maxMetadataSizeWritten {
		return nil, false, ErrMetaTooBig
	}
	if len(data) < 2 || data[0] != '{' || data[len(data)-1] != '}' {
		return nil, false, errors.New("invalid json")
	}
	var metadata metaSimpleJSON
	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return nil, false, err
	}
	// Basic fields are strictly required
	// to reduce possibility that a random small file resembles metadata.
	if metadata.Version == nil || metadata.Size == nil || metadata.ChunkNum == nil {
		return nil, false, errors.New("missing required field")
	}
	// Perform strict checks, avoid corruption of future metadata formats.
	if *metadata.Version < 1 {
		return nil, false, errors.New("wrong version")
	}
	if *metadata.Size < 0 {
		return nil, false, errors.New("negative file size")
	}
	if *metadata.ChunkNum < 0 {
		return nil, false, errors.New("negative number of chunks")
	}
	if *metadata.ChunkNum > maxSafeChunkNumber {
		return nil, true, ErrChunkOverflow // produced by incompatible version of rclone
	}
	if metadata.MD5 != "" {
		_, err = hex.DecodeString(metadata.MD5)
		if len(metadata.MD5) != 32 || err != nil {
			return nil, false, errors.New("wrong md5 hash")
		}
	}
	if metadata.SHA1 != "" {
		_, err = hex.DecodeString(metadata.SHA1)
		if len(metadata.SHA1) != 40 || err != nil {
			return nil, false, errors.New("wrong sha1 hash")
		}
	}
	// ChunkNum is allowed to be 0 in future versions
	if *metadata.ChunkNum < 1 && *metadata.Version <= metadataVersion {
		return nil, false, errors.New("wrong number of chunks")
	}
	// Non-strict mode also accepts future metadata versions
	if *metadata.Version > metadataVersion {
		return nil, true, ErrMetaUnknown // produced by incompatible version of rclone
	}

	var nilFs *Fs // nil object triggers appropriate type method
	info = nilFs.wrapInfo(metaObject, "", *metadata.Size)
	info.nChunks = *metadata.ChunkNum
	info.md5 = metadata.MD5
	info.sha1 = metadata.SHA1
	info.xactID = metadata.XactID
	return info, true, nil
}

func silentlyRemove(ctx context.Context, o fs.Object) {
	_ = o.Remove(ctx) // ignore error
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("Chunked '%s:%s'", f.name, f.root)
}

// Precision returns the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return f.base.Precision()
}

// CanQuickRename returns true if the Fs supports a quick rename operation
func (f *Fs) CanQuickRename() bool {
	return f.base.Features().Move != nil
}

// Check the interfaces are satisfied
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.DirSetModTimer  = (*Fs)(nil)
	_ fs.MkdirMetadataer = (*Fs)(nil)
	_ fs.PutUncheckeder  = (*Fs)(nil)
	_ fs.PutStreamer     = (*Fs)(nil)
	_ fs.CleanUpper      = (*Fs)(nil)
	_ fs.UnWrapper       = (*Fs)(nil)
	_ fs.ListRer         = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Wrapper         = (*Fs)(nil)
	_ fs.ChangeNotifier  = (*Fs)(nil)
	_ fs.Shutdowner      = (*Fs)(nil)
	_ fs.ObjectInfo      = (*ObjectInfo)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.ObjectUnWrapper = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)
