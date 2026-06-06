package chunker

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/fstests"
	"github.com/rclone/rclone/lib/random"
	"github.com/rclone/rclone/lib/readers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Command line flags
var (
	UploadKilobytes = flag.Int("upload-kilobytes", 0, "Upload size in Kilobytes, set this to test large uploads")
)

// test that chunking does not break large uploads
func testPutLarge(t *testing.T, f *Fs, kilobytes int) {
	t.Run(fmt.Sprintf("PutLarge%dk", kilobytes), func(t *testing.T) {
		fstests.TestPutLarge(context.Background(), t, f, &fstest.Item{
			ModTime: fstest.Time("2001-02-03T04:05:06.499999999Z"),
			Path:    fmt.Sprintf("chunker-upload-%dk", kilobytes),
			Size:    int64(kilobytes) * int64(fs.Kibi),
		})
	})
}

type settings map[string]any

type failPutFs struct {
	fs.Fs
	failRemote string
	failErr    error
	enabled    bool
	features   *fs.Features
}

func newFailPutFs(base fs.Fs, failRemote string, failErr error) *failPutFs {
	f := &failPutFs{
		Fs:         base,
		failRemote: failRemote,
		failErr:    failErr,
	}
	f.features = base.Features().Wrap(f)
	return f
}

func (f *failPutFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if f.enabled && src.Remote() == f.failRemote {
		return nil, f.failErr
	}
	return f.Fs.Put(ctx, in, src, options...)
}

func (f *failPutFs) Features() *fs.Features {
	return f.features
}

func (f *failPutFs) UnWrap() fs.Fs {
	return f.Fs
}

type failMoveCopyFs struct {
	fs.Fs
	failRemote string
	failErr    error
	enabled    bool
	features   *fs.Features
	moves      []string
	copies     []string
	failedOp   string
}

func newFailMoveCopyFs(base fs.Fs, failRemote string, failErr error) *failMoveCopyFs {
	f := &failMoveCopyFs{
		Fs:         base,
		failRemote: failRemote,
		failErr:    failErr,
	}
	baseFeatures := base.Features()
	f.features = baseFeatures.Wrap(f)
	if baseFeatures.Move != nil {
		f.features.Move = f.Move
	}
	if baseFeatures.Copy != nil {
		f.features.Copy = f.Copy
	}
	return f
}

func (f *failMoveCopyFs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	f.moves = append(f.moves, remote)
	if f.enabled && remote == f.failRemote {
		f.failedOp = "move"
		return nil, f.failErr
	}
	doMove := f.Fs.Features().Move
	if doMove == nil {
		return nil, fs.ErrorCantMove
	}
	return doMove(ctx, src, remote)
}

func (f *failMoveCopyFs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	f.copies = append(f.copies, remote)
	if f.enabled && remote == f.failRemote {
		f.failedOp = "copy"
		return nil, f.failErr
	}
	doCopy := f.Fs.Features().Copy
	if doCopy == nil {
		return nil, fs.ErrorCantCopy
	}
	return doCopy(ctx, src, remote)
}

func (f *failMoveCopyFs) Features() *fs.Features {
	return f.features
}

func (f *failMoveCopyFs) UnWrap() fs.Fs {
	return f.Fs
}

func deriveFs(ctx context.Context, t *testing.T, f fs.Fs, path string, opts settings) fs.Fs {
	fsName := strings.Split(f.Name(), "{")[0] // strip off hash
	configMap := configmap.Simple{}
	for key, val := range opts {
		configMap[key] = fmt.Sprintf("%v", val)
	}
	rpath := fspath.JoinRootPath(f.Root(), path)
	remote := fmt.Sprintf("%s,%s:%s", fsName, configMap.String(), rpath)
	fixFs, err := fs.NewFs(ctx, remote)
	require.NoError(t, err)
	return fixFs
}

var mtime1 = fstest.Time("2001-02-03T04:05:06.499999999Z")

func testPutFile(ctx context.Context, t *testing.T, f fs.Fs, name, contents, message string, check bool) fs.Object {
	item := fstest.Item{Path: name, ModTime: mtime1}
	obj := fstests.PutTestContents(ctx, t, f, &item, contents, check)
	assert.NotNil(t, obj, message)
	return obj
}

// test chunk name parser
func testChunkNameFormat(t *testing.T, f *Fs) {
	saveOpt := f.opt
	defer func() {
		// restore original settings (f is pointer, f.opt is struct)
		f.opt = saveOpt
		_ = f.setChunkNameFormat(f.opt.NameFormat)
	}()

	assertFormat := func(pattern, wantDataFormat, wantCtrlFormat, wantNameRegexp string) {
		err := f.setChunkNameFormat(pattern)
		assert.NoError(t, err)
		assert.Equal(t, wantDataFormat, f.dataNameFmt)
		assert.Equal(t, wantCtrlFormat, f.ctrlNameFmt)
		assert.Equal(t, wantNameRegexp, f.nameRegexp.String())
	}

	assertFormatValid := func(pattern string) {
		err := f.setChunkNameFormat(pattern)
		assert.NoError(t, err)
	}

	assertFormatInvalid := func(pattern string) {
		err := f.setChunkNameFormat(pattern)
		assert.Error(t, err)
	}

	assertMakeName := func(wantChunkName, mainName string, chunkNo int, ctrlType, xactID string) {
		gotChunkName := ""
		assert.NotPanics(t, func() {
			gotChunkName = f.makeChunkName(mainName, chunkNo, ctrlType, xactID)
		}, "makeChunkName(%q,%d,%q,%q) must not panic", mainName, chunkNo, ctrlType, xactID)
		if gotChunkName != "" {
			assert.Equal(t, wantChunkName, gotChunkName)
		}
	}

	assertMakeNamePanics := func(mainName string, chunkNo int, ctrlType, xactID string) {
		assert.Panics(t, func() {
			_ = f.makeChunkName(mainName, chunkNo, ctrlType, xactID)
		}, "makeChunkName(%q,%d,%q,%q) should panic", mainName, chunkNo, ctrlType, xactID)
	}

	assertParseName := func(fileName, wantMainName string, wantChunkNo int, wantCtrlType, wantXactID string) {
		gotMainName, gotChunkNo, gotCtrlType, gotXactID := f.parseChunkName(fileName)
		assert.Equal(t, wantMainName, gotMainName)
		assert.Equal(t, wantChunkNo, gotChunkNo)
		assert.Equal(t, wantCtrlType, gotCtrlType)
		assert.Equal(t, wantXactID, gotXactID)
	}

	const newFormatSupported = false // support for patterns not starting with base name (*)

	// valid formats
	assertFormat(`*.rclone_chunk.###`, `%s.rclone_chunk.%03d`, `%s.rclone_chunk._%s`, `^(.+?)\.rclone_chunk\.(?:([0-9]{3,})|_([a-z][a-z0-9]{2,6}))(?:_([0-9a-z]{4,9})|\.\.tmp_([0-9]{10,13}))?$`)
	assertFormat(`*.rclone_chunk.#`, `%s.rclone_chunk.%d`, `%s.rclone_chunk._%s`, `^(.+?)\.rclone_chunk\.(?:([0-9]+)|_([a-z][a-z0-9]{2,6}))(?:_([0-9a-z]{4,9})|\.\.tmp_([0-9]{10,13}))?$`)
	assertFormat(`*_chunk_#####`, `%s_chunk_%05d`, `%s_chunk__%s`, `^(.+?)_chunk_(?:([0-9]{5,})|_([a-z][a-z0-9]{2,6}))(?:_([0-9a-z]{4,9})|\.\.tmp_([0-9]{10,13}))?$`)
	assertFormat(`*-chunk-#`, `%s-chunk-%d`, `%s-chunk-_%s`, `^(.+?)-chunk-(?:([0-9]+)|_([a-z][a-z0-9]{2,6}))(?:_([0-9a-z]{4,9})|\.\.tmp_([0-9]{10,13}))?$`)
	assertFormat(`*-chunk-#-%^$()[]{}.+-!?:\`, `%s-chunk-%d-%%^$()[]{}.+-!?:\`, `%s-chunk-_%s-%%^$()[]{}.+-!?:\`, `^(.+?)-chunk-(?:([0-9]+)|_([a-z][a-z0-9]{2,6}))-%\^\$\(\)\[\]\{\}\.\+-!\?:\\(?:_([0-9a-z]{4,9})|\.\.tmp_([0-9]{10,13}))?$`)
	if newFormatSupported {
		assertFormat(`_*-chunk-##,`, `_%s-chunk-%02d,`, `_%s-chunk-_%s,`, `^_(.+?)-chunk-(?:([0-9]{2,})|_([a-z][a-z0-9]{2,6})),(?:_([0-9a-z]{4,9})|\.\.tmp_([0-9]{10,13}))?$`)
	}

	// invalid formats
	assertFormatInvalid(`chunk-#`)
	assertFormatInvalid(`*-chunk`)
	assertFormatInvalid(`*-*-chunk-#`)
	assertFormatInvalid(`*-chunk-#-#`)
	assertFormatInvalid(`#-chunk-*`)
	assertFormatInvalid(`*/#`)

	assertFormatValid(`*#`)
	assertFormatInvalid(`**#`)
	assertFormatInvalid(`#*`)
	assertFormatInvalid(``)
	assertFormatInvalid(`-`)

	// quick tests
	if newFormatSupported {
		assertFormat(`part_*_#`, `part_%s_%d`, `part_%s__%s`, `^part_(.+?)_(?:([0-9]+)|_([a-z][a-z0-9]{2,6}))(?:_([0-9][0-9a-z]{3,8})\.\.tmp_([0-9]{10,13}))?$`)
		f.opt.StartFrom = 1

		assertMakeName(`part_fish_1`, "fish", 0, "", "")
		assertParseName(`part_fish_43`, "fish", 42, "", "")
		assertMakeName(`part_fish__locks`, "fish", -2, "locks", "")
		assertParseName(`part_fish__locks`, "fish", -1, "locks", "")
		assertMakeName(`part_fish__x2y`, "fish", -2, "x2y", "")
		assertParseName(`part_fish__x2y`, "fish", -1, "x2y", "")
		assertMakeName(`part_fish_3_0004`, "fish", 2, "", "4")
		assertParseName(`part_fish_4_0005`, "fish", 3, "", "0005")
		assertMakeName(`part_fish__blkinfo_jj5fvo3wr`, "fish", -3, "blkinfo", "jj5fvo3wr")
		assertParseName(`part_fish__blkinfo_zz9fvo3wr`, "fish", -1, "blkinfo", "zz9fvo3wr")

		// old-style temporary suffix (parse only)
		assertParseName(`part_fish_4..tmp_0000000011`, "fish", 3, "", "000b")
		assertParseName(`part_fish__blkinfo_jj5fvo3wr`, "fish", -1, "blkinfo", "jj5fvo3wr")
	}

	// prepare format for long tests
	assertFormat(`*.chunk.###`, `%s.chunk.%03d`, `%s.chunk._%s`, `^(.+?)\.chunk\.(?:([0-9]{3,})|_([a-z][a-z0-9]{2,6}))(?:_([0-9a-z]{4,9})|\.\.tmp_([0-9]{10,13}))?$`)
	f.opt.StartFrom = 2

	// valid data chunks
	assertMakeName(`fish.chunk.003`, "fish", 1, "", "")
	assertParseName(`fish.chunk.003`, "fish", 1, "", "")
	assertMakeName(`fish.chunk.021`, "fish", 19, "", "")
	assertParseName(`fish.chunk.021`, "fish", 19, "", "")

	// valid temporary data chunks
	assertMakeName(`fish.chunk.011_4321`, "fish", 9, "", "4321")
	assertParseName(`fish.chunk.011_4321`, "fish", 9, "", "4321")
	assertMakeName(`fish.chunk.011_00bc`, "fish", 9, "", "00bc")
	assertParseName(`fish.chunk.011_00bc`, "fish", 9, "", "00bc")
	assertMakeName(`fish.chunk.1916_5jjfvo3wr`, "fish", 1914, "", "5jjfvo3wr")
	assertParseName(`fish.chunk.1916_5jjfvo3wr`, "fish", 1914, "", "5jjfvo3wr")
	assertMakeName(`fish.chunk.1917_zz9fvo3wr`, "fish", 1915, "", "zz9fvo3wr")
	assertParseName(`fish.chunk.1917_zz9fvo3wr`, "fish", 1915, "", "zz9fvo3wr")

	// valid temporary data chunks (old temporary suffix, only parse)
	assertParseName(`fish.chunk.004..tmp_0000000047`, "fish", 2, "", "001b")
	assertParseName(`fish.chunk.323..tmp_9994567890123`, "fish", 321, "", "3jjfvo3wr")

	// parsing invalid data chunk names
	assertParseName(`fish.chunk.3`, "", -1, "", "")
	assertParseName(`fish.chunk.001`, "", -1, "", "")
	assertParseName(`fish.chunk.21`, "", -1, "", "")
	assertParseName(`fish.chunk.-21`, "", -1, "", "")

	assertParseName(`fish.chunk.004abcd`, "", -1, "", "")        // missing underscore delimiter
	assertParseName(`fish.chunk.004__1234`, "", -1, "", "")      // extra underscore delimiter
	assertParseName(`fish.chunk.004_123`, "", -1, "", "")        // too short temporary suffix
	assertParseName(`fish.chunk.004_1234567890`, "", -1, "", "") // too long temporary suffix
	assertParseName(`fish.chunk.004_-1234`, "", -1, "", "")      // temporary suffix must be positive
	assertParseName(`fish.chunk.004_123E`, "", -1, "", "")       // uppercase not allowed
	assertParseName(`fish.chunk.004_12.3`, "", -1, "", "")       // punctuation not allowed

	// parsing invalid data chunk names (old temporary suffix)
	assertParseName(`fish.chunk.004.tmp_0000000021`, "", -1, "", "")
	assertParseName(`fish.chunk.003..tmp_123456789`, "", -1, "", "")
	assertParseName(`fish.chunk.003..tmp_012345678901234567890123456789`, "", -1, "", "")
	assertParseName(`fish.chunk.323..tmp_12345678901234`, "", -1, "", "")
	assertParseName(`fish.chunk.003..tmp_-1`, "", -1, "", "")

	// valid control chunks
	assertMakeName(`fish.chunk._info`, "fish", -1, "info", "")
	assertMakeName(`fish.chunk._locks`, "fish", -2, "locks", "")
	assertMakeName(`fish.chunk._blkinfo`, "fish", -3, "blkinfo", "")
	assertMakeName(`fish.chunk._x2y`, "fish", -4, "x2y", "")

	assertParseName(`fish.chunk._info`, "fish", -1, "info", "")
	assertParseName(`fish.chunk._locks`, "fish", -1, "locks", "")
	assertParseName(`fish.chunk._blkinfo`, "fish", -1, "blkinfo", "")
	assertParseName(`fish.chunk._x2y`, "fish", -1, "x2y", "")

	// valid temporary control chunks
	assertMakeName(`fish.chunk._info_0001`, "fish", -1, "info", "1")
	assertMakeName(`fish.chunk._locks_4321`, "fish", -2, "locks", "4321")
	assertMakeName(`fish.chunk._uploads_abcd`, "fish", -3, "uploads", "abcd")
	assertMakeName(`fish.chunk._blkinfo_xyzabcdef`, "fish", -4, "blkinfo", "xyzabcdef")
	assertMakeName(`fish.chunk._x2y_1aaa`, "fish", -5, "x2y", "1aaa")

	assertParseName(`fish.chunk._info_0001`, "fish", -1, "info", "0001")
	assertParseName(`fish.chunk._locks_4321`, "fish", -1, "locks", "4321")
	assertParseName(`fish.chunk._uploads_9abc`, "fish", -1, "uploads", "9abc")
	assertParseName(`fish.chunk._blkinfo_xyzabcdef`, "fish", -1, "blkinfo", "xyzabcdef")
	assertParseName(`fish.chunk._x2y_1aaa`, "fish", -1, "x2y", "1aaa")

	// valid temporary control chunks (old temporary suffix, parse only)
	assertParseName(`fish.chunk._info..tmp_0000000047`, "fish", -1, "info", "001b")
	assertParseName(`fish.chunk._locks..tmp_0000054321`, "fish", -1, "locks", "15wx")
	assertParseName(`fish.chunk._uploads..tmp_0000000000`, "fish", -1, "uploads", "0000")
	assertParseName(`fish.chunk._blkinfo..tmp_9994567890123`, "fish", -1, "blkinfo", "3jjfvo3wr")
	assertParseName(`fish.chunk._x2y..tmp_0000000000`, "fish", -1, "x2y", "0000")

	// parsing invalid control chunk names
	assertParseName(`fish.chunk.metadata`, "", -1, "", "") // must be prepended by underscore
	assertParseName(`fish.chunk.info`, "", -1, "", "")
	assertParseName(`fish.chunk.locks`, "", -1, "", "")
	assertParseName(`fish.chunk.uploads`, "", -1, "", "")

	assertParseName(`fish.chunk._os`, "", -1, "", "")        // too short
	assertParseName(`fish.chunk._metadata`, "", -1, "", "")  // too long
	assertParseName(`fish.chunk._blockinfo`, "", -1, "", "") // way too long
	assertParseName(`fish.chunk._4me`, "", -1, "", "")       // cannot start with digit
	assertParseName(`fish.chunk._567`, "", -1, "", "")       // cannot be all digits
	assertParseName(`fish.chunk._me_ta`, "", -1, "", "")     // punctuation not allowed
	assertParseName(`fish.chunk._in-fo`, "", -1, "", "")
	assertParseName(`fish.chunk._.bin`, "", -1, "", "")
	assertParseName(`fish.chunk._.2xy`, "", -1, "", "")

	// parsing invalid temporary control chunks
	assertParseName(`fish.chunk._blkinfo1234`, "", -1, "", "")     // missing underscore delimiter
	assertParseName(`fish.chunk._info__1234`, "", -1, "", "")      // extra underscore delimiter
	assertParseName(`fish.chunk._info_123`, "", -1, "", "")        // too short temporary suffix
	assertParseName(`fish.chunk._info_1234567890`, "", -1, "", "") // too long temporary suffix
	assertParseName(`fish.chunk._info_-1234`, "", -1, "", "")      // temporary suffix must be positive
	assertParseName(`fish.chunk._info_123E`, "", -1, "", "")       // uppercase not allowed
	assertParseName(`fish.chunk._info_12.3`, "", -1, "", "")       // punctuation not allowed

	assertParseName(`fish.chunk._locks..tmp_123456789`, "", -1, "", "")
	assertParseName(`fish.chunk._meta..tmp_-1`, "", -1, "", "")
	assertParseName(`fish.chunk._blockinfo..tmp_012345678901234567890123456789`, "", -1, "", "")

	// short control chunk names: 3 letters ok, 1-2 letters not allowed
	assertMakeName(`fish.chunk._ext`, "fish", -1, "ext", "")
	assertParseName(`fish.chunk._int`, "fish", -1, "int", "")

	assertMakeNamePanics("fish", -1, "in", "")
	assertMakeNamePanics("fish", -1, "up", "4")
	assertMakeNamePanics("fish", -1, "x", "")
	assertMakeNamePanics("fish", -1, "c", "1z")

	assertMakeName(`fish.chunk._ext_0000`, "fish", -1, "ext", "0")
	assertMakeName(`fish.chunk._ext_0026`, "fish", -1, "ext", "26")
	assertMakeName(`fish.chunk._int_0abc`, "fish", -1, "int", "abc")
	assertMakeName(`fish.chunk._int_9xyz`, "fish", -1, "int", "9xyz")
	assertMakeName(`fish.chunk._out_jj5fvo3wr`, "fish", -1, "out", "jj5fvo3wr")
	assertMakeName(`fish.chunk._out_jj5fvo3wr`, "fish", -1, "out", "jj5fvo3wr")

	assertParseName(`fish.chunk._ext_0000`, "fish", -1, "ext", "0000")
	assertParseName(`fish.chunk._ext_0026`, "fish", -1, "ext", "0026")
	assertParseName(`fish.chunk._int_0abc`, "fish", -1, "int", "0abc")
	assertParseName(`fish.chunk._int_9xyz`, "fish", -1, "int", "9xyz")
	assertParseName(`fish.chunk._out_jj5fvo3wr`, "fish", -1, "out", "jj5fvo3wr")
	assertParseName(`fish.chunk._out_jj5fvo3wr`, "fish", -1, "out", "jj5fvo3wr")

	// base file name can sometimes look like a valid chunk name
	assertParseName(`fish.chunk.003.chunk.004`, "fish.chunk.003", 2, "", "")
	assertParseName(`fish.chunk.003.chunk._info`, "fish.chunk.003", -1, "info", "")
	assertParseName(`fish.chunk.003.chunk._Meta`, "", -1, "", "")

	assertParseName(`fish.chunk._info.chunk.004`, "fish.chunk._info", 2, "", "")
	assertParseName(`fish.chunk._info.chunk._info`, "fish.chunk._info", -1, "info", "")
	assertParseName(`fish.chunk._info.chunk._info.chunk._Meta`, "", -1, "", "")

	// base file name looking like a valid chunk name (old temporary suffix)
	assertParseName(`fish.chunk.003.chunk.005..tmp_0000000022`, "fish.chunk.003", 3, "", "000m")
	assertParseName(`fish.chunk.003.chunk._x..tmp_0000054321`, "", -1, "", "")
	assertParseName(`fish.chunk._info.chunk.005..tmp_0000000023`, "fish.chunk._info", 3, "", "000n")
	assertParseName(`fish.chunk._info.chunk._info.chunk._x..tmp_0000054321`, "", -1, "", "")

	assertParseName(`fish.chunk.003.chunk._blkinfo..tmp_9994567890123`, "fish.chunk.003", -1, "blkinfo", "3jjfvo3wr")
	assertParseName(`fish.chunk._info.chunk._blkinfo..tmp_9994567890123`, "fish.chunk._info", -1, "blkinfo", "3jjfvo3wr")

	assertParseName(`fish.chunk.004..tmp_0000000021.chunk.004`, "fish.chunk.004..tmp_0000000021", 2, "", "")
	assertParseName(`fish.chunk.004..tmp_0000000021.chunk.005..tmp_0000000025`, "fish.chunk.004..tmp_0000000021", 3, "", "000p")
	assertParseName(`fish.chunk.004..tmp_0000000021.chunk._info`, "fish.chunk.004..tmp_0000000021", -1, "info", "")
	assertParseName(`fish.chunk.004..tmp_0000000021.chunk._blkinfo..tmp_9994567890123`, "fish.chunk.004..tmp_0000000021", -1, "blkinfo", "3jjfvo3wr")
	assertParseName(`fish.chunk.004..tmp_0000000021.chunk._Meta`, "", -1, "", "")
	assertParseName(`fish.chunk.004..tmp_0000000021.chunk._x..tmp_0000054321`, "", -1, "", "")

	assertParseName(`fish.chunk._blkinfo..tmp_9994567890123.chunk.004`, "fish.chunk._blkinfo..tmp_9994567890123", 2, "", "")
	assertParseName(`fish.chunk._blkinfo..tmp_9994567890123.chunk.005..tmp_0000000026`, "fish.chunk._blkinfo..tmp_9994567890123", 3, "", "000q")
	assertParseName(`fish.chunk._blkinfo..tmp_9994567890123.chunk._info`, "fish.chunk._blkinfo..tmp_9994567890123", -1, "info", "")
	assertParseName(`fish.chunk._blkinfo..tmp_9994567890123.chunk._blkinfo..tmp_9994567890123`, "fish.chunk._blkinfo..tmp_9994567890123", -1, "blkinfo", "3jjfvo3wr")
	assertParseName(`fish.chunk._blkinfo..tmp_9994567890123.chunk._info.chunk._Meta`, "", -1, "", "")
	assertParseName(`fish.chunk._blkinfo..tmp_9994567890123.chunk._info.chunk._x..tmp_0000054321`, "", -1, "", "")

	assertParseName(`fish.chunk._blkinfo..tmp_1234567890123456789.chunk.004`, "fish.chunk._blkinfo..tmp_1234567890123456789", 2, "", "")
	assertParseName(`fish.chunk._blkinfo..tmp_1234567890123456789.chunk.005..tmp_0000000022`, "fish.chunk._blkinfo..tmp_1234567890123456789", 3, "", "000m")
	assertParseName(`fish.chunk._blkinfo..tmp_1234567890123456789.chunk._info`, "fish.chunk._blkinfo..tmp_1234567890123456789", -1, "info", "")
	assertParseName(`fish.chunk._blkinfo..tmp_1234567890123456789.chunk._blkinfo..tmp_9994567890123`, "fish.chunk._blkinfo..tmp_1234567890123456789", -1, "blkinfo", "3jjfvo3wr")
	assertParseName(`fish.chunk._blkinfo..tmp_1234567890123456789.chunk._info.chunk._Meta`, "", -1, "", "")
	assertParseName(`fish.chunk._blkinfo..tmp_1234567890123456789.chunk._info.chunk._x..tmp_0000054321`, "", -1, "", "")

	// attempts to make invalid chunk names
	assertMakeNamePanics("fish", -1, "", "")          // neither data nor control
	assertMakeNamePanics("fish", 0, "info", "")       // both data and control
	assertMakeNamePanics("fish", -1, "metadata", "")  // control type too long
	assertMakeNamePanics("fish", -1, "blockinfo", "") // control type way too long
	assertMakeNamePanics("fish", -1, "2xy", "")       // first digit not allowed
	assertMakeNamePanics("fish", -1, "123", "")       // all digits not allowed
	assertMakeNamePanics("fish", -1, "Meta", "")      // only lower case letters allowed
	assertMakeNamePanics("fish", -1, "in-fo", "")     // punctuation not allowed
	assertMakeNamePanics("fish", -1, "_info", "")
	assertMakeNamePanics("fish", -1, "info_", "")
	assertMakeNamePanics("fish", -2, ".bind", "")
	assertMakeNamePanics("fish", -2, "bind.", "")

	assertMakeNamePanics("fish", -1, "", "1")          // neither data nor control
	assertMakeNamePanics("fish", 0, "info", "23")      // both data and control
	assertMakeNamePanics("fish", -1, "metadata", "45") // control type too long
	assertMakeNamePanics("fish", -1, "blockinfo", "7") // control type way too long
	assertMakeNamePanics("fish", -1, "2xy", "abc")     // first digit not allowed
	assertMakeNamePanics("fish", -1, "123", "def")     // all digits not allowed
	assertMakeNamePanics("fish", -1, "Meta", "mnk")    // only lower case letters allowed
	assertMakeNamePanics("fish", -1, "in-fo", "xyz")   // punctuation not allowed
	assertMakeNamePanics("fish", -1, "_info", "5678")
	assertMakeNamePanics("fish", -1, "info_", "999")
	assertMakeNamePanics("fish", -2, ".bind", "0")
	assertMakeNamePanics("fish", -2, "bind.", "0")

	assertMakeNamePanics("fish", 0, "", "1234567890") // temporary suffix too long
	assertMakeNamePanics("fish", 0, "", "123F4")      // uppercase not allowed
	assertMakeNamePanics("fish", 0, "", "123.")       // punctuation not allowed
	assertMakeNamePanics("fish", 0, "", "_123")
}

func testSmallFileInternals(t *testing.T, f *Fs) {
	const dir = "small"
	ctx := context.Background()
	saveOpt := f.opt
	defer func() {
		f.opt.FailHard = false
		_ = operations.Purge(ctx, f.base, dir)
		f.opt = saveOpt
	}()
	f.opt.FailHard = false

	modTime := fstest.Time("2001-02-03T04:05:06.499999999Z")

	checkSmallFileInternals := func(obj fs.Object) {
		assert.NotNil(t, obj)
		o, ok := obj.(*Object)
		assert.True(t, ok)
		assert.NotNil(t, o)
		if o == nil {
			return
		}
		switch {
		case !f.useMeta:
			// If meta format is "none", non-chunked file (even empty)
			// internally is a single chunk without meta object.
			assert.Nil(t, o.main)
			assert.True(t, o.isComposite()) // sorry, sometimes a name is misleading
			assert.Equal(t, 1, len(o.chunks))
		case f.hashAll:
			// Consistent hashing forces meta object on small files too
			assert.NotNil(t, o.main)
			assert.True(t, o.isComposite())
			assert.Equal(t, 1, len(o.chunks))
		default:
			// normally non-chunked file is kept in the Object's main field
			assert.NotNil(t, o.main)
			assert.False(t, o.isComposite())
			assert.Equal(t, 0, len(o.chunks))
		}
	}

	checkContents := func(obj fs.Object, contents string) {
		assert.NotNil(t, obj)
		assert.Equal(t, int64(len(contents)), obj.Size())

		r, err := obj.Open(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, r)
		if r == nil {
			return
		}
		data, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, contents, string(data))
		_ = r.Close()
	}

	checkHashsum := func(obj fs.Object) {
		var ht hash.Type
		switch {
		case !f.hashAll:
			return
		case f.useMD5:
			ht = hash.MD5
		case f.useSHA1:
			ht = hash.SHA1
		default:
			return
		}
		// even empty files must have hashsum in consistent mode
		sum, err := obj.Hash(ctx, ht)
		assert.NoError(t, err)
		assert.NotEqual(t, sum, "")
	}

	checkSmallFile := func(name, contents string) {
		filename := path.Join(dir, name)
		item := fstest.Item{Path: filename, ModTime: modTime}
		put := fstests.PutTestContents(ctx, t, f, &item, contents, false)
		assert.NotNil(t, put)
		checkSmallFileInternals(put)
		checkContents(put, contents)
		checkHashsum(put)

		// objects returned by Put and NewObject must have similar structure
		obj, err := f.NewObject(ctx, filename)
		assert.NoError(t, err)
		assert.NotNil(t, obj)
		checkSmallFileInternals(obj)
		checkContents(obj, contents)
		checkHashsum(obj)

		_ = obj.Remove(ctx)
		_ = put.Remove(ctx) // for good
	}

	checkSmallFile("emptyfile", "")
	checkSmallFile("smallfile", "Ok")
}

func testPreventCorruption(t *testing.T, f *Fs) {
	if f.opt.ChunkSize > 50 {
		t.Skip("this test requires small chunks")
	}
	const dir = "corrupted"
	ctx := context.Background()
	saveOpt := f.opt
	defer func() {
		f.opt.FailHard = false
		_ = operations.Purge(ctx, f.base, dir)
		f.opt = saveOpt
	}()
	f.opt.FailHard = true

	contents := random.String(250)
	modTime := fstest.Time("2001-02-03T04:05:06.499999999Z")
	const overlapMessage = "chunk overlap"

	assertOverlapError := func(err error) {
		assert.Error(t, err)
		if err != nil {
			assert.Contains(t, err.Error(), overlapMessage)
		}
	}

	newFile := func(name string) fs.Object {
		item := fstest.Item{Path: path.Join(dir, name), ModTime: modTime}
		obj := fstests.PutTestContents(ctx, t, f, &item, contents, true)
		require.NotNil(t, obj)
		return obj
	}
	billyObj := newFile("billy")
	billyTxn := billyObj.(*Object).xactID
	if f.useNoRename {
		require.True(t, billyTxn != "")
	} else {
		require.True(t, billyTxn == "")
	}

	billyChunkName := func(chunkNo int) string {
		return f.makeChunkName(billyObj.Remote(), chunkNo, "", billyTxn)
	}

	err := f.Mkdir(ctx, billyChunkName(1))
	assertOverlapError(err)

	_, err = f.Move(ctx, newFile("silly1"), billyChunkName(2))
	assert.Error(t, err)
	assert.True(t, err == fs.ErrorCantMove || (err != nil && strings.Contains(err.Error(), overlapMessage)))

	_, err = f.Copy(ctx, newFile("silly2"), billyChunkName(3))
	assert.Error(t, err)
	assert.True(t, err == fs.ErrorCantCopy || (err != nil && strings.Contains(err.Error(), overlapMessage)))

	// accessing chunks in strict mode is prohibited
	f.opt.FailHard = true
	billyChunk4Name := billyChunkName(4)
	_, err = f.base.NewObject(ctx, billyChunk4Name)
	require.NoError(t, err)
	_, err = f.NewObject(ctx, billyChunk4Name)
	assertOverlapError(err)

	f.opt.FailHard = false
	billyChunk4, err := f.NewObject(ctx, billyChunk4Name)
	assert.NoError(t, err)
	require.NotNil(t, billyChunk4)

	f.opt.FailHard = true
	_, err = f.Put(ctx, bytes.NewBufferString(contents), billyChunk4)
	assertOverlapError(err)

	// you can freely read chunks (if you have an object)
	r, err := billyChunk4.Open(ctx)
	assert.NoError(t, err)
	var chunkContents []byte
	assert.NotPanics(t, func() {
		chunkContents, err = io.ReadAll(r)
		_ = r.Close()
	})
	assert.NoError(t, err)
	assert.NotEqual(t, contents, string(chunkContents))

	// but you can't change them
	err = billyChunk4.Update(ctx, bytes.NewBufferString(contents), newFile("silly3"))
	assertOverlapError(err)

	// Remove isn't special, you can't corrupt files even if you have an object
	err = billyChunk4.Remove(ctx)
	assertOverlapError(err)

	// recreate billy in case it was anyhow corrupted
	willyObj := newFile("willy")
	willyTxn := willyObj.(*Object).xactID
	willyChunkName := f.makeChunkName(willyObj.Remote(), 1, "", willyTxn)
	f.opt.FailHard = false
	willyChunk, err := f.NewObject(ctx, willyChunkName)
	f.opt.FailHard = true
	assert.NoError(t, err)
	require.NotNil(t, willyChunk)

	_, err = operations.Copy(ctx, f, willyChunk, willyChunkName, newFile("silly4"))
	assertOverlapError(err)

	// operations.Move will return error when chunker's Move refused
	// to corrupt target file, but reverts to copy/delete method
	// still trying to delete target chunk. Chunker must come to rescue.
	_, err = operations.Move(ctx, f, willyChunk, willyChunkName, newFile("silly5"))
	assertOverlapError(err)
	r, err = willyChunk.Open(ctx)
	assert.NoError(t, err)
	assert.NotPanics(t, func() {
		_, err = io.ReadAll(r)
		_ = r.Close()
	})
	assert.NoError(t, err)
}

func testChunkNumberOverflow(t *testing.T, f *Fs) {
	if f.opt.ChunkSize > 50 {
		t.Skip("this test requires small chunks")
	}
	const dir = "wreaked"
	const wreakNumber = 10200300
	ctx := context.Background()
	saveOpt := f.opt
	defer func() {
		f.opt.FailHard = false
		_ = operations.Purge(ctx, f.base, dir)
		f.opt = saveOpt
	}()

	modTime := fstest.Time("2001-02-03T04:05:06.499999999Z")
	contents := random.String(100)

	newFile := func(f fs.Fs, name string) (obj fs.Object, filename string, txnID string) {
		filename = path.Join(dir, name)
		item := fstest.Item{Path: filename, ModTime: modTime}
		obj = fstests.PutTestContents(ctx, t, f, &item, contents, true)
		require.NotNil(t, obj)
		if chunkObj, isChunkObj := obj.(*Object); isChunkObj {
			txnID = chunkObj.xactID
		}
		return
	}

	f.opt.FailHard = false
	file, fileName, fileTxn := newFile(f, "wreaker")
	wreak, _, _ := newFile(f.base, f.makeChunkName("wreaker", wreakNumber, "", fileTxn))

	f.opt.FailHard = false
	fstest.CheckListingWithRoot(t, f, dir, nil, nil, f.Precision())
	_, err := f.NewObject(ctx, fileName)
	assert.Error(t, err)

	f.opt.FailHard = true
	_, err = f.List(ctx, dir)
	assert.Error(t, err)
	_, err = f.NewObject(ctx, fileName)
	assert.Error(t, err)

	f.opt.FailHard = false
	_ = wreak.Remove(ctx)
	_ = file.Remove(ctx)
}

func testMissingChunk(t *testing.T, f *Fs) {
	if !f.useMeta {
		t.Skip("this test requires metadata support")
	}
	const dir = "missing"
	ctx := context.Background()
	saveOpt := f.opt
	defer func() {
		f.opt.FailHard = false
		_ = operations.Purge(ctx, f.base, dir)
		f.opt = saveOpt
	}()
	f.opt.FailHard = false

	modTime := fstest.Time("2001-02-03T04:05:06.499999999Z")
	chunks := []string{"abc", "def", "ghi"}
	var fullSize int64
	for _, chunk := range chunks {
		fullSize += int64(len(chunk))
	}

	assertErrorContains := func(t *testing.T, err error, want string) {
		t.Helper()
		assert.Error(t, err)
		if err != nil {
			assert.Contains(t, err.Error(), want)
		}
	}

	putBase := func(t *testing.T, name, contents string) {
		t.Helper()
		item := fstest.Item{Path: name, ModTime: modTime}
		obj := fstests.PutTestContents(ctx, t, f.base, &item, contents, true)
		require.NotNil(t, obj)
	}

	putMissingChunkObject := func(t *testing.T, caseName string, missingChunkNo int) (filename string, presentSize int64) {
		t.Helper()
		filename = path.Join(dir, caseName, "file")
		metadata, err := marshalSimpleJSON(ctx, fullSize, len(chunks), "", "", "")
		require.NoError(t, err)
		putBase(t, filename, string(metadata))
		for chunkNo, contents := range chunks {
			if chunkNo == missingChunkNo {
				continue
			}
			putBase(t, f.makeChunkName(filename, chunkNo, "", ""), contents)
			presentSize += int64(len(contents))
		}
		return filename, presentSize
	}

	assertListedObject := func(t *testing.T, filename string, wantSize int64) {
		t.Helper()
		entries, err := f.List(ctx, path.Dir(filename))
		require.NoError(t, err)
		require.Equal(t, 1, len(entries))
		obj, ok := entries[0].(fs.Object)
		require.True(t, ok)
		assert.Equal(t, filename, obj.Remote())
		assert.Equal(t, wantSize, obj.Size())
	}

	t.Run("Middle", func(t *testing.T) {
		filename, _ := putMissingChunkObject(t, "middle", 1)

		f.opt.FailHard = false
		entries, err := f.List(ctx, path.Dir(filename))
		require.NoError(t, err)
		assert.Empty(t, entries)
		_, err = f.NewObject(ctx, filename)
		assertErrorContains(t, err, "has missing chunks")

		f.opt.FailHard = true
		_, err = f.List(ctx, path.Dir(filename))
		assertErrorContains(t, err, "has missing chunks")
		_, err = f.NewObject(ctx, filename)
		assertErrorContains(t, err, "has missing chunks")
	})

	t.Run("Last", func(t *testing.T) {
		filename, presentSize := putMissingChunkObject(t, "last", 2)

		f.opt.FailHard = false
		assertListedObject(t, filename, presentSize)
		f.opt.FailHard = true
		assertListedObject(t, filename, presentSize)

		obj, err := f.NewObject(ctx, filename)
		require.NoError(t, err)
		require.NotNil(t, obj)
		assert.Equal(t, filename, obj.Remote())
		assert.Equal(t, presentSize, obj.Size())

		reader, err := obj.Open(ctx)
		assert.Nil(t, reader)
		assertErrorContains(t, err, "metadata doesn't match file size")
		sum, err := obj.Hash(ctx, hash.MD5)
		assert.Empty(t, sum)
		assertErrorContains(t, err, "metadata doesn't match file size")
	})
}

func testMetadataInput(t *testing.T, f *Fs) {
	const minChunkForTest = 50
	if f.opt.ChunkSize < minChunkForTest {
		t.Skip("this test requires chunks that fit metadata")
	}

	const dir = "usermeta"
	ctx := context.Background()
	saveOpt := f.opt
	defer func() {
		f.opt.FailHard = false
		_ = operations.Purge(ctx, f.base, dir)
		f.opt = saveOpt
	}()
	f.opt.FailHard = false

	runSubtest := func(contents, name string) {
		description := fmt.Sprintf("file with %s metadata", name)
		filename := path.Join(dir, name)
		require.True(t, len(contents) > 2 && len(contents) < minChunkForTest, description+" test data is correct")

		part := testPutFile(ctx, t, f.base, f.makeChunkName(filename, 0, "", ""), "oops", "", true)
		require.NotNil(t, part)
		_ = testPutFile(ctx, t, f, filename, contents, "upload "+description, false)

		obj, err := f.NewObject(ctx, filename)
		require.NoError(t, err, "access "+description)
		require.NotNil(t, obj)
		assert.Equal(t, int64(len(contents)), obj.Size(), "size "+description)

		o, ok := obj.(*Object)
		assert.NotNil(t, ok)
		if o != nil {
			assert.True(t, o.isComposite() && len(o.chunks) == 1, description+" is forced composite")
			o = nil
		}

		defer func() {
			_ = obj.Remove(ctx)
			_ = part.Remove(ctx)
		}()

		metaObj, err := f.base.NewObject(ctx, filename)
		assert.NoError(t, err, description+" has metadata object")
		assert.NotNil(t, metaObj)
		if metaObj != nil {
			assert.NotEqual(t, int64(len(contents)), metaObj.Size(), description+" metadata is not user data")
		}

		chunkObj, err := f.base.NewObject(ctx, f.makeChunkName(filename, 0, "", ""))
		require.NoError(t, err, description+" has data chunk")
		assert.Equal(t, int64(len(contents)), chunkObj.Size(), description+" data chunk size")
		chunkReader, err := chunkObj.Open(ctx)
		require.NoError(t, err, description+" open data chunk")
		chunkData, err := io.ReadAll(chunkReader)
		assert.NoError(t, err, description+" read data chunk")
		assert.Equal(t, contents, string(chunkData), description+" data chunk contents")
		_ = chunkReader.Close()

		r, err := obj.Open(ctx)
		assert.NoError(t, err, "open "+description)
		assert.NotNil(t, r, "open stream of "+description)
		if err == nil && r != nil {
			data, err := io.ReadAll(r)
			assert.NoError(t, err, "read all of "+description)
			assert.Equal(t, contents, string(data), description+" contents is ok")
			_ = r.Close()
		}
	}

	metaData, err := marshalSimpleJSON(ctx, 3, 1, "", "", "")
	require.NoError(t, err)
	todaysMeta := string(metaData)
	runSubtest(todaysMeta, "today")

	pastMeta := regexp.MustCompile(`"ver":[0-9]+`).ReplaceAllLiteralString(todaysMeta, `"ver":1`)
	pastMeta = regexp.MustCompile(`"size":[0-9]+`).ReplaceAllLiteralString(pastMeta, `"size":0`)
	runSubtest(pastMeta, "past")

	futureMeta := regexp.MustCompile(`"ver":[0-9]+`).ReplaceAllLiteralString(todaysMeta, `"ver":999`)
	futureMeta = regexp.MustCompile(`"nchunks":[0-9]+`).ReplaceAllLiteralString(futureMeta, `"nchunks":0,"x":"y"`)
	runSubtest(futureMeta, "future")
}

func TestSimpleJSONMetadata(t *testing.T) {
	ctx := context.Background()
	const (
		md5sum  = "d41d8cd98f00b204e9800998ecf8427e"
		sha1sum = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
		txnID   = "tx123"
	)

	t.Run("MarshalFieldShape", func(t *testing.T) {
		for _, tt := range []struct {
			name    string
			size    int64
			nChunks int
			md5     string
			sha1    string
			xactID  string
			want    string
		}{
			{
				name:    "required fields",
				size:    0,
				nChunks: 0,
				want:    `{"ver":1,"size":0,"nchunks":0}`,
			},
			{
				name:    "hashes without transaction",
				size:    123,
				nChunks: 4,
				md5:     md5sum,
				sha1:    sha1sum,
				want:    `{"ver":1,"size":123,"nchunks":4,"md5":"d41d8cd98f00b204e9800998ecf8427e","sha1":"da39a3ee5e6b4b0d3255bfef95601890afd80709"}`,
			},
			{
				name:    "transaction without hashes",
				size:    123,
				nChunks: 4,
				xactID:  txnID,
				want:    `{"ver":2,"size":123,"nchunks":4,"txn":"tx123"}`,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				data, err := marshalSimpleJSON(ctx, tt.size, tt.nChunks, tt.md5, tt.sha1, tt.xactID)
				require.NoError(t, err)
				assert.Equal(t, tt.want, string(data))
			})
		}
	})

	t.Run("UnmarshalFieldShape", func(t *testing.T) {
		metadata := []byte(`{"ver":2,"size":123,"nchunks":4,"md5":"d41d8cd98f00b204e9800998ecf8427e","sha1":"da39a3ee5e6b4b0d3255bfef95601890afd80709","txn":"tx123"}`)
		info, madeByChunker, err := unmarshalSimpleJSON(ctx, nil, metadata)
		require.NoError(t, err)
		require.NotNil(t, info)
		assert.True(t, madeByChunker)
		assert.Equal(t, int64(123), info.Size())
		assert.Equal(t, 4, info.nChunks)
		assert.Equal(t, md5sum, info.md5)
		assert.Equal(t, sha1sum, info.sha1)
		assert.Equal(t, txnID, info.xactID)
	})

	t.Run("MarshalSizeLimit", func(t *testing.T) {
		txnForEncodedLen := func(want int) string {
			t.Helper()
			data, err := marshalSimpleJSON(ctx, 1, 1, "", "", "x")
			require.NoError(t, err)
			require.GreaterOrEqual(t, want, len(data))
			return strings.Repeat("x", want-len(data)+1)
		}

		data, err := marshalSimpleJSON(ctx, 1, 1, "", "", txnForEncodedLen(maxMetadataSizeWritten-1))
		require.NoError(t, err)
		assert.Len(t, data, maxMetadataSizeWritten-1)

		data, err = marshalSimpleJSON(ctx, 1, 1, "", "", txnForEncodedLen(maxMetadataSizeWritten))
		assert.Nil(t, data)
		assert.EqualError(t, err, "metadata can't be this big, please report to rclone developers")
	})

	t.Run("UnmarshalSizeAndVersionLimits", func(t *testing.T) {
		paddedFutureMetadata := func(want int) []byte {
			t.Helper()
			prefix := `{"ver":999,"size":1,"nchunks":1,"pad":"`
			suffix := `"}`
			require.GreaterOrEqual(t, want, len(prefix)+len(suffix))
			return []byte(prefix + strings.Repeat("x", want-len(prefix)-len(suffix)) + suffix)
		}

		info, madeByChunker, err := unmarshalSimpleJSON(ctx, nil, []byte(`{"ver":0,"size":1,"nchunks":1}`))
		assert.Nil(t, info)
		assert.False(t, madeByChunker)
		assert.EqualError(t, err, "wrong version")

		exactLimit := paddedFutureMetadata(maxMetadataSizeWritten)
		require.Len(t, exactLimit, maxMetadataSizeWritten)
		info, madeByChunker, err = unmarshalSimpleJSON(ctx, nil, exactLimit)
		assert.Nil(t, info)
		assert.True(t, madeByChunker)
		assert.Equal(t, ErrMetaUnknown, err)

		tooBig := append(exactLimit, ' ')
		info, madeByChunker, err = unmarshalSimpleJSON(ctx, nil, tooBig)
		assert.Nil(t, info)
		assert.False(t, madeByChunker)
		assert.Equal(t, ErrMetaTooBig, err)
	})
}

// Test that chunker refuses to change on objects with future/unknown metadata
func testFutureProof(t *testing.T, f *Fs) {
	if !f.useMeta {
		t.Skip("this test requires metadata support")
	}

	saveOpt := f.opt
	ctx := context.Background()
	f.opt.FailHard = true
	const dir = "future"
	const file = dir + "/test"
	defer func() {
		f.opt.FailHard = false
		_ = operations.Purge(ctx, f.base, dir)
		f.opt = saveOpt
	}()

	modTime := fstest.Time("2001-02-03T04:05:06.499999999Z")
	putPart := func(name string, part int, data, msg string) {
		if part > 0 {
			name = f.makeChunkName(name, part-1, "", "")
		}
		item := fstest.Item{Path: name, ModTime: modTime}
		obj := fstests.PutTestContents(ctx, t, f.base, &item, data, true)
		assert.NotNil(t, obj, msg)
	}

	// simulate chunked object from future
	meta := `{"ver":999,"nchunks":3,"size":9,"garbage":"litter","sha1":"0707f2970043f9f7c22029482db27733deaec029"}`
	putPart(file, 0, meta, "metaobject")
	putPart(file, 1, "abc", "chunk1")
	putPart(file, 2, "def", "chunk2")
	putPart(file, 3, "ghi", "chunk3")

	assertFutureObjectShape := func(msg string) fs.Object {
		// List should succeed
		ls, err := f.List(ctx, dir)
		assert.NoError(t, err, msg)
		require.Equal(t, 1, len(ls), msg)
		assert.Equal(t, file, ls[0].Remote(), msg)
		assert.Equal(t, int64(9), ls[0].Size(), msg)

		// NewObject should succeed
		obj, err := f.NewObject(ctx, file)
		assert.NoError(t, err, msg)
		require.NotNil(t, obj, msg)
		assert.Equal(t, file, obj.Remote(), msg)
		assert.Equal(t, int64(9), obj.Size(), msg)
		return obj
	}

	assertFutureParts := func(msg string) {
		checkPart := func(name, want string) {
			obj, err := f.base.NewObject(ctx, name)
			require.NoError(t, err, msg)
			require.NotNil(t, obj, msg)
			assert.Equal(t, int64(len(want)), obj.Size(), msg)
			assert.Equal(t, want, fstests.ReadObject(ctx, t, obj, -1), msg)
		}

		checkPart(file, meta)
		checkPart(f.makeChunkName(file, 0, "", ""), "abc")
		checkPart(f.makeChunkName(file, 1, "", ""), "def")
		checkPart(f.makeChunkName(file, 2, "", ""), "ghi")
	}

	obj := assertFutureObjectShape("future object before replacement")
	assertFutureParts("future parts before replacement")

	// Hash must fail
	_, err := obj.Hash(ctx, hash.SHA1)
	assert.Equal(t, ErrMetaUnknown, err)

	// Move must fail
	mobj, err := operations.Move(ctx, f, nil, file+"2", obj)
	assert.Nil(t, mobj)
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "please upgrade rclone")
	}

	// Put must fail
	oi := object.NewStaticObjectInfo(file, modTime, 3, true, nil, nil)
	buf := bytes.NewBufferString("abc")
	_, err = f.Put(ctx, buf, oi)
	assert.Error(t, err)
	assertFutureObjectShape("future object after Put replacement refusal")
	assertFutureParts("future parts after Put replacement refusal")

	// Rcat must fail
	in := io.NopCloser(bytes.NewBufferString("abc"))
	robj, err := operations.Rcat(ctx, f, file, in, modTime, nil)
	assert.Nil(t, robj)
	assert.NotNil(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "please upgrade rclone")
	}

	obj = assertFutureObjectShape("future object after Rcat replacement refusal")
	assertFutureParts("future parts after Rcat replacement refusal")
	assert.Equal(t, ErrMetaUnknown, func() error {
		_, err := obj.Hash(ctx, hash.SHA1)
		return err
	}())
}

func testReplacementFailureOrdering(t *testing.T, f *Fs) {
	const dir = "replacefail"
	ctx := context.Background()
	saveOpt := f.opt
	defer func() {
		f.opt.FailHard = false
		_ = operations.Purge(ctx, f.base, dir)
		f.opt = saveOpt
	}()
	_ = operations.Purge(ctx, f.base, dir)
	f.opt.FailHard = false
	f.opt.ChunkSize = fs.SizeSuffix(10)

	const filename = dir + "/file"
	chunkSize := int(f.opt.ChunkSize)
	oldContents := strings.Repeat("o", chunkSize*2+1)
	oldObject := testPutFile(ctx, t, f, filename, oldContents, "upload old chunked file", true)
	oldChunkerObject, ok := oldObject.(*Object)
	require.True(t, ok)
	require.Greater(t, len(oldChunkerObject.chunks), 1)

	var oldMetadataRemote string
	if oldChunkerObject.main != nil {
		oldMetadataRemote = oldChunkerObject.main.Remote()
		metaObj, err := f.base.NewObject(ctx, oldMetadataRemote)
		require.NoError(t, err, "old metadata exists before failed replacement")
		require.NotNil(t, metaObj, "old metadata exists before failed replacement")
	}

	oldChunkRemotes := make([]string, 0, len(oldChunkerObject.chunks))
	for _, chunk := range oldChunkerObject.chunks {
		remote := chunk.Remote()
		oldChunkRemotes = append(oldChunkRemotes, remote)
		baseChunk, err := f.base.NewObject(ctx, remote)
		require.NoError(t, err, "old active chunk exists before failed replacement")
		require.NotNil(t, baseChunk, "old active chunk exists before failed replacement")
	}

	sentinelErr := errors.New("replacement upload failed in second chunk")
	replacementPrefix := strings.Repeat("n", chunkSize)
	replacementReader := io.MultiReader(
		bytes.NewBufferString(replacementPrefix),
		readers.ErrorReader{Err: sentinelErr},
	)
	replacementInfo := object.NewStaticObjectInfo(filename, mtime1, int64(chunkSize+1), true, nil, nil)

	err := oldChunkerObject.Update(ctx, replacementReader, replacementInfo)
	require.Error(t, err)
	assert.True(t, errors.Is(err, sentinelErr) || strings.Contains(err.Error(), sentinelErr.Error()))

	restoredObject, err := f.NewObject(ctx, filename)
	require.NoError(t, err)
	require.NotNil(t, restoredObject)
	assert.Equal(t, oldContents, fstests.ReadObject(ctx, t, restoredObject, -1))

	if oldMetadataRemote != "" {
		metaObj, err := f.base.NewObject(ctx, oldMetadataRemote)
		require.NoError(t, err, "old metadata exists after failed replacement")
		require.NotNil(t, metaObj, "old metadata exists after failed replacement")
	}
	for _, remote := range oldChunkRemotes {
		baseChunk, err := f.base.NewObject(ctx, remote)
		require.NoError(t, err, "old active chunk exists after failed replacement")
		require.NotNil(t, baseChunk, "old active chunk exists after failed replacement")
	}
}

func testMetadataWriteFailurePreservesNonChunkedObject(t *testing.T, f *Fs) {
	ctx := context.Background()
	const (
		dir         = "metawritefail"
		filename    = "file"
		oldContents = "old non-chunked contents"
	)
	_ = operations.Purge(ctx, f.base, dir)
	defer func() {
		_ = operations.Purge(ctx, f.base, dir)
	}()

	fsResult := deriveFs(ctx, t, f, dir, settings{
		"chunk_size":   "1P",
		"name_format":  "*.#",
		"hash_type":    "none",
		"transactions": "rename",
		"meta_format":  "simplejson",
	})
	chunkFs, ok := fsResult.(*Fs)
	require.True(t, ok, "fs must be a chunker remote")

	oldObject := testPutFile(ctx, t, chunkFs, filename, oldContents, "upload old non-chunked file", true)
	oldChunkerObject, ok := oldObject.(*Object)
	require.True(t, ok)
	require.Nil(t, oldChunkerObject.chunks, "old object must be non-chunked")

	sentinelErr := errors.New("metadata write failed")
	failBase := newFailPutFs(chunkFs.base, filename, sentinelErr)
	chunkFs.base = failBase
	chunkFs.features = chunkFs.features.WrapsFs(chunkFs, failBase)

	replacementContents := `{"ver":1,"size":1,"nchunks":1}`
	replacementInfo := object.NewStaticObjectInfo(filename, mtime1, int64(len(replacementContents)), true, nil, nil)
	failBase.enabled = true
	_, err := chunkFs.Put(ctx, bytes.NewBufferString(replacementContents), replacementInfo)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinelErr)

	restoredObject, err := chunkFs.NewObject(ctx, filename)
	require.NoError(t, err)
	require.NotNil(t, restoredObject)
	assert.Equal(t, oldContents, fstests.ReadObject(ctx, t, restoredObject, -1))
}

func testRenameBaseMoveFailureCurrentBehavior(t *testing.T, f *Fs) {
	ctx := context.Background()
	const (
		dir      = "renamebasefail"
		filename = "file"
	)
	_ = operations.Purge(ctx, f.base, dir)
	defer func() {
		_ = operations.Purge(ctx, f.base, dir)
	}()

	fsResult := deriveFs(ctx, t, f, dir, settings{
		"chunk_size":   "10",
		"name_format":  "*.#",
		"hash_type":    "none",
		"transactions": "rename",
		"meta_format":  "simplejson",
	})
	chunkFs, ok := fsResult.(*Fs)
	require.True(t, ok, "fs must be a chunker remote")

	baseFeatures := chunkFs.base.Features()
	if baseFeatures.Move == nil && baseFeatures.Copy == nil {
		t.Skip("wrapped remote has neither Move nor Copy feature")
	}
	expectedOp := "copy"
	if baseFeatures.Move != nil {
		expectedOp = "move"
	}

	chunkSize := int(chunkFs.opt.ChunkSize)
	oldContents := strings.Repeat("o", chunkSize*2+1)
	oldObject := testPutFile(ctx, t, chunkFs, filename, oldContents, "upload old chunked file", true)
	oldChunkerObject, ok := oldObject.(*Object)
	require.True(t, ok)
	require.NotNil(t, oldChunkerObject.main, "old object must have metadata")
	require.Greater(t, len(oldChunkerObject.chunks), 1, "old object must be chunked")

	oldMetadataRemote := oldChunkerObject.main.Remote()
	oldMetadataObject, err := chunkFs.base.NewObject(ctx, oldMetadataRemote)
	require.NoError(t, err, "old metadata exists before failed replacement")
	oldMetadata, err := operations.ReadFile(ctx, oldMetadataObject)
	require.NoError(t, err, "old metadata can be read before failed replacement")

	oldChunkRemotes := make([]string, 0, len(oldChunkerObject.chunks))
	for _, chunk := range oldChunkerObject.chunks {
		remote := chunk.Remote()
		oldChunkRemotes = append(oldChunkRemotes, remote)
		baseChunk, err := chunkFs.base.NewObject(ctx, remote)
		require.NoError(t, err, "old active chunk exists before failed replacement")
		require.NotNil(t, baseChunk, "old active chunk exists before failed replacement")
	}

	sentinelErr := errors.New("final chunk rename failed")
	failRemote := chunkFs.makeChunkName(filename, 0, "", "")
	require.Contains(t, oldChunkRemotes, failRemote, "first permanent chunk remote must be an old active chunk")
	failBase := newFailMoveCopyFs(chunkFs.base, failRemote, sentinelErr)
	chunkFs.base = failBase
	chunkFs.features = chunkFs.features.WrapsFs(chunkFs, failBase)

	replacementContents := strings.Repeat("n", chunkSize*3+1)
	replacementInfo := object.NewStaticObjectInfo(filename, mtime1, int64(len(replacementContents)), true, nil, nil)
	failBase.enabled = true
	err = oldChunkerObject.Update(ctx, bytes.NewBufferString(replacementContents), replacementInfo)
	require.Error(t, err)
	assert.True(t, errors.Is(err, sentinelErr) || strings.Contains(err.Error(), sentinelErr.Error()))
	assert.Equal(t, expectedOp, failBase.failedOp)
	assert.Contains(t, append(failBase.moves, failBase.copies...), failRemote)

	for _, remote := range oldChunkRemotes {
		_, err := chunkFs.base.NewObject(ctx, remote)
		assert.ErrorIs(t, err, fs.ErrorObjectNotFound, "old active chunk must have been removed before final rename failure")
	}

	metadataObject, err := chunkFs.base.NewObject(ctx, oldMetadataRemote)
	require.NoError(t, err, "old metadata remains after failed replacement")
	metadataAfter, err := operations.ReadFile(ctx, metadataObject)
	require.NoError(t, err, "old metadata can be read after failed replacement")
	assert.Equal(t, oldMetadata, metadataAfter, "new metadata must not be committed after final rename failure")

	postObject, err := chunkFs.NewObject(ctx, filename)
	require.NoError(t, err, "current behavior exposes the remaining old metadata object")
	assert.Equal(t, string(oldMetadata), fstests.ReadObject(ctx, t, postObject, -1))
	assert.NotEqual(t, oldContents, fstests.ReadObject(ctx, t, postObject, -1))
}

// The newer method of doing transactions without renaming should still be able to correctly process chunks that were created with renaming
// If you attempt to do the inverse, however, the data chunks will be ignored causing commands to perform incorrectly
func testBackwardsCompatibility(t *testing.T, f *Fs) {
	if !f.useMeta {
		t.Skip("Can't do norename transactions without metadata")
	}
	const dir = "backcomp"
	ctx := context.Background()
	saveOpt := f.opt
	saveUseNoRename := f.useNoRename
	defer func() {
		f.opt.FailHard = false
		_ = operations.Purge(ctx, f.base, dir)
		f.opt = saveOpt
		f.useNoRename = saveUseNoRename
	}()
	f.opt.ChunkSize = fs.SizeSuffix(10)

	modTime := fstest.Time("2001-02-03T04:05:06.499999999Z")
	contents := random.String(250)
	chunkCount := (len(contents) + int(f.opt.ChunkSize) - 1) / int(f.opt.ChunkSize)
	require.Greater(t, chunkCount, 1)
	newFile := func(f fs.Fs, name string) (*Object, string) {
		filename := path.Join(dir, name)
		item := fstest.Item{Path: filename, ModTime: modTime}
		obj := fstests.PutTestContents(ctx, t, f, &item, contents, true)
		require.NotNil(t, obj)
		chunkerObj, ok := obj.(*Object)
		require.True(t, ok)
		return chunkerObj, filename
	}
	assertBaseObjectNames := func(filename, xactID string) {
		entries, err := f.base.List(ctx, dir)
		require.NoError(t, err)

		var got []string
		for _, entry := range entries {
			obj, ok := entry.(fs.Object)
			if !ok {
				continue
			}
			remote := obj.Remote()
			mainRemote, _, _, _ := f.parseChunkName(remote)
			if remote == filename || mainRemote == filename {
				got = append(got, remote)
			}
		}
		sort.Strings(got)

		want := []string{filename}
		for chunkNo := range chunkCount {
			want = append(want, f.makeChunkName(filename, chunkNo, "", xactID))
		}
		sort.Strings(want)
		assert.Equal(t, want, got)

		for _, remote := range got {
			mainRemote, _, ctrlType, gotXactID := f.parseChunkName(remote)
			if mainRemote == filename && ctrlType == "" {
				assert.Equal(t, xactID, gotXactID)
			}
		}
	}
	assertMetadataXactID := func(filename, xactID string) {
		metaObj, err := f.base.NewObject(ctx, filename)
		require.NoError(t, err)
		reader, err := metaObj.Open(ctx)
		require.NoError(t, err)
		metadata, err := io.ReadAll(reader)
		_ = reader.Close()
		require.NoError(t, err)

		info, madeByChunker, err := unmarshalSimpleJSON(ctx, metaObj, metadata)
		require.NoError(t, err)
		require.True(t, madeByChunker)
		assert.Equal(t, int64(len(contents)), info.Size())
		assert.Equal(t, chunkCount, info.nChunks)
		assert.Equal(t, xactID, info.xactID)
	}
	assertReadable := func(filename string) {
		obj, err := f.NewObject(ctx, filename)
		require.NoError(t, err)
		require.NotNil(t, obj)
		reader, err := obj.Open(ctx)
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		require.NoError(t, err)
		assert.Equal(t, contents, string(data))
	}

	f.opt.FailHard = false
	f.useNoRename = false
	file, fileName := newFile(f, "renamefile")
	require.Empty(t, file.xactID)
	assertBaseObjectNames(fileName, "")
	assertMetadataXactID(fileName, "")

	f.opt.FailHard = false
	item := fstest.NewItem(fileName, contents, modTime)

	var items []fstest.Item
	items = append(items, item)

	f.useNoRename = true
	fstest.CheckListingWithRoot(t, f, dir, items, nil, f.Precision())
	assertReadable(fileName)

	f.opt.FailHard = true
	_, err := f.List(ctx, dir)
	assert.NoError(t, err)

	f.opt.FailHard = false
	norenameFile, norenameFileName := newFile(f, "norenamefile")
	require.NotEmpty(t, norenameFile.xactID)
	assertBaseObjectNames(norenameFileName, norenameFile.xactID)
	assertMetadataXactID(norenameFileName, norenameFile.xactID)
	assertReadable(norenameFileName)
}

func testChunkerServerSideMove(t *testing.T, f *Fs) {
	if !f.useMeta {
		t.Skip("Can't test norename transactions without metadata")
	}

	ctx := context.Background()
	const dir = "servermovetest"
	subRemote := fmt.Sprintf("%s:%s/%s", f.Name(), f.Root(), dir)

	subFs1, err := fs.NewFs(ctx, subRemote+"/subdir1")
	assert.NoError(t, err)
	fs1, isChunkerFs := subFs1.(*Fs)
	assert.True(t, isChunkerFs)
	fs1.useNoRename = false
	fs1.opt.ChunkSize = fs.SizeSuffix(3)

	subFs2, err := fs.NewFs(ctx, subRemote+"/subdir2")
	assert.NoError(t, err)
	fs2, isChunkerFs := subFs2.(*Fs)
	assert.True(t, isChunkerFs)
	fs2.useNoRename = true
	fs2.opt.ChunkSize = fs.SizeSuffix(3)

	modTime := fstest.Time("2001-02-03T04:05:06.499999999Z")
	item := fstest.Item{Path: "movefile", ModTime: modTime}
	contents := "abcdef"
	file := fstests.PutTestContents(ctx, t, fs1, &item, contents, true)

	dstOverwritten, _ := fs2.NewObject(ctx, "movefile")
	dstFile, err := operations.Move(ctx, fs2, dstOverwritten, "movefile", file)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(contents)), dstFile.Size())

	r, err := dstFile.Open(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, contents, string(data))
	_ = r.Close()
	_ = operations.Purge(ctx, f.base, dir)
}

// Test that md5all creates metadata even for small and zero-byte files
func testMD5AllSlow(t *testing.T, f *Fs) {
	ctx := context.Background()
	for _, test := range []struct {
		name    string
		content string
		md5     string
	}{
		{
			name:    "zero-byte",
			content: "",
			md5:     "d41d8cd98f00b204e9800998ecf8427e",
		}, {
			name:    "small",
			content: "-",
			md5:     "336d5ebc5436534e61d16e63ddfca327",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fsResult := deriveFs(ctx, t, f, path.Join("md5all", test.name), settings{
				"chunk_size":   "1P",
				"name_format":  "*.#",
				"hash_type":    "md5all",
				"transactions": "rename",
				"meta_format":  "simplejson",
			})
			chunkFs, ok := fsResult.(*Fs)
			require.True(t, ok, "fs must be a chunker remote")
			baseFs := chunkFs.base
			if !baseFs.Features().SlowHash {
				t.Skipf("this test needs a base fs with slow hash, e.g. local")
			}

			assert.True(t, chunkFs.useMD5, "must use md5")
			assert.True(t, chunkFs.hashAll, "must hash all files")

			_ = testPutFile(ctx, t, chunkFs, "file", test.content, "error", true)
			obj, err := chunkFs.NewObject(ctx, "file")
			require.NoError(t, err)
			sum, err := obj.Hash(ctx, hash.MD5)
			assert.NoError(t, err)
			assert.Equal(t, test.md5, sum)

			list, err := baseFs.List(ctx, "")
			require.NoError(t, err)
			assert.Equal(t, 2, len(list))
			_, err = baseFs.NewObject(ctx, "file")
			assert.NoError(t, err, "metadata must be created")
			chunk, err := baseFs.NewObject(ctx, "file.1")
			require.NoError(t, err, "first chunk must be created")
			assert.Equal(t, int64(len(test.content)), chunk.Size())

			require.NoError(t, operations.Purge(ctx, baseFs, ""))
		})
	}
}

// InternalTest dispatches all internal tests
func (f *Fs) InternalTest(t *testing.T) {
	t.Run("PutLarge", func(t *testing.T) {
		if *UploadKilobytes <= 0 {
			t.Skip("-upload-kilobytes is not set")
		}
		testPutLarge(t, f, *UploadKilobytes)
	})
	t.Run("ChunkNameFormat", func(t *testing.T) {
		testChunkNameFormat(t, f)
	})
	t.Run("SmallFileInternals", func(t *testing.T) {
		testSmallFileInternals(t, f)
	})
	t.Run("PreventCorruption", func(t *testing.T) {
		testPreventCorruption(t, f)
	})
	t.Run("ChunkNumberOverflow", func(t *testing.T) {
		testChunkNumberOverflow(t, f)
	})
	t.Run("MissingChunk", func(t *testing.T) {
		testMissingChunk(t, f)
	})
	t.Run("MetadataInput", func(t *testing.T) {
		testMetadataInput(t, f)
	})
	t.Run("FutureProof", func(t *testing.T) {
		testFutureProof(t, f)
	})
	t.Run("ReplacementFailureOrdering", func(t *testing.T) {
		testReplacementFailureOrdering(t, f)
	})
	t.Run("MetadataWriteFailurePreservesNonChunkedObject", func(t *testing.T) {
		testMetadataWriteFailurePreservesNonChunkedObject(t, f)
	})
	t.Run("RenameBaseMoveFailureCurrentBehavior", func(t *testing.T) {
		testRenameBaseMoveFailureCurrentBehavior(t, f)
	})
	t.Run("BackwardsCompatibility", func(t *testing.T) {
		testBackwardsCompatibility(t, f)
	})
	t.Run("ChunkerServerSideMove", func(t *testing.T) {
		testChunkerServerSideMove(t, f)
	})
	t.Run("MD5AllSlow", func(t *testing.T) {
		testMD5AllSlow(t, f)
	})
}

var _ fstests.InternalTester = (*Fs)(nil)
