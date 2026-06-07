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
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
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

type trackingPutFs struct {
	fs.Fs
	mu   sync.Mutex
	puts []trackingPutCall
}

type blockingPutFs struct {
	fs.Fs
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	puts    []trackingPutCall
}

type trackingPutCall struct {
	remote  string
	content string
	size    int64
}

type chunkWriterPutResult struct {
	err          error
	returnObject bool
	storeObject  bool
}

type chunkWriterRetryPutFs struct {
	fs.Fs
	puts     []trackingPutCall
	results  []chunkWriterPutResult
	objects  map[string]*chunkWriterTestObject
	removes  []string
	features *fs.Features
}

type chunkWriterTestObject struct {
	fs.Object
	parent *chunkWriterRetryPutFs
}

type multipartTrackingFs struct {
	fs.Fs
	mu       sync.Mutex
	puts     []multipartTrackingPutCall
	features *fs.Features
}

type multipartTrackingPutCall struct {
	remote   string
	content  string
	size     int64
	seekable bool
}

type slowHashFs struct {
	fs.Fs
	features *fs.Features
}

type hashTrackingObjectInfo struct {
	fs.Object
	mu    sync.Mutex
	calls []hash.Type
	sums  map[hash.Type]string
}

type hashTrackingObject struct {
	fs.Object
	info *hashTrackingObjectInfo
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

func newSlowHashFs(base fs.Fs) *slowHashFs {
	f := &slowHashFs{
		Fs: base,
	}
	f.features = base.Features().Wrap(f)
	f.features.SlowHash = true
	return f
}

func (f *slowHashFs) Features() *fs.Features {
	return f.features
}

func (f *slowHashFs) UnWrap() fs.Fs {
	return f.Fs
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

func (f *trackingPutFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	buf, err := io.ReadAll(in)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, trackingPutCall{
		remote:  src.Remote(),
		content: string(buf),
		size:    src.Size(),
	})
	return object.NewMemoryObject(src.Remote(), src.ModTime(ctx), buf).SetFs(f), nil
}

func newHashTrackingObjectInfo(remote string, size int64, sourceFs fs.Fs, sums map[hash.Type]string) *hashTrackingObjectInfo {
	base := object.NewMemoryObject(remote, mtime1, make([]byte, int(size))).SetFs(sourceFs)
	info := &hashTrackingObjectInfo{
		sums: sums,
	}
	info.Object = &hashTrackingObject{
		Object: base,
		info:   info,
	}
	return info
}

func (i *hashTrackingObjectInfo) Hash(ctx context.Context, h hash.Type) (string, error) {
	i.mu.Lock()
	i.calls = append(i.calls, h)
	i.mu.Unlock()
	if sum, ok := i.sums[h]; ok {
		return sum, nil
	}
	return "", hash.ErrUnsupported
}

func (i *hashTrackingObjectInfo) HashCalls() []hash.Type {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]hash.Type(nil), i.calls...)
}

func (i *hashTrackingObjectInfo) UnWrap() fs.Object {
	return i.Object
}

func (o *hashTrackingObject) Hash(ctx context.Context, h hash.Type) (string, error) {
	return o.info.Hash(ctx, h)
}

func (o *hashTrackingObject) UnWrap() fs.Object {
	return o.Object
}

func newBlockingPutFs() *blockingPutFs {
	return &blockingPutFs{
		Fs:      object.MemoryFs,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *blockingPutFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	f.once.Do(func() {
		close(f.started)
	})
	select {
	case <-f.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	buf, err := io.ReadAll(in)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, trackingPutCall{
		remote:  src.Remote(),
		content: string(buf),
		size:    src.Size(),
	})
	return object.NewMemoryObject(src.Remote(), src.ModTime(ctx), buf).SetFs(f), nil
}

func newChunkWriterRetryPutFs(results ...chunkWriterPutResult) *chunkWriterRetryPutFs {
	f := &chunkWriterRetryPutFs{
		Fs:      object.MemoryFs,
		results: append([]chunkWriterPutResult(nil), results...),
		objects: make(map[string]*chunkWriterTestObject),
	}
	f.features = object.MemoryFs.Features().Wrap(f)
	return f
}

func (f *chunkWriterRetryPutFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	buf, err := io.ReadAll(in)
	if err != nil {
		return nil, err
	}
	f.puts = append(f.puts, trackingPutCall{
		remote:  src.Remote(),
		content: string(buf),
		size:    src.Size(),
	})

	var result chunkWriterPutResult
	if len(f.results) > 0 {
		result = f.results[0]
		f.results = f.results[1:]
	}

	obj := f.newObject(src.Remote(), buf)
	if result.storeObject || result.err == nil {
		f.objects[src.Remote()] = obj
	}
	if result.err != nil {
		if result.returnObject {
			return obj, result.err
		}
		return nil, result.err
	}
	return obj, nil
}

func (f *chunkWriterRetryPutFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	obj, ok := f.objects[remote]
	if !ok {
		return nil, fs.ErrorObjectNotFound
	}
	return obj, nil
}

func (f *chunkWriterRetryPutFs) Features() *fs.Features {
	return f.features
}

func (f *chunkWriterRetryPutFs) UnWrap() fs.Fs {
	return f.Fs
}

func (f *chunkWriterRetryPutFs) newObject(remote string, content []byte) *chunkWriterTestObject {
	return &chunkWriterTestObject{
		Object: object.NewMemoryObject(remote, mtime1, append([]byte(nil), content...)).SetFs(f),
		parent: f,
	}
}

func (o *chunkWriterTestObject) Remove(ctx context.Context) error {
	o.parent.removes = append(o.parent.removes, o.Remote())
	delete(o.parent.objects, o.Remote())
	return nil
}

func newMultipartTrackingFs(base fs.Fs) *multipartTrackingFs {
	f := &multipartTrackingFs{
		Fs: base,
	}
	f.features = base.Features().Wrap(f)
	return f
}

func (f *multipartTrackingFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	buf, err := io.ReadAll(in)
	if err != nil {
		return nil, err
	}
	_, seekable := in.(io.Seeker)
	f.mu.Lock()
	f.puts = append(f.puts, multipartTrackingPutCall{
		remote:   src.Remote(),
		content:  string(buf),
		size:     src.Size(),
		seekable: seekable,
	})
	f.mu.Unlock()
	return f.Fs.Put(ctx, bytes.NewReader(buf), src, options...)
}

func (f *multipartTrackingFs) Features() *fs.Features {
	return f.features
}

func (f *multipartTrackingFs) UnWrap() fs.Fs {
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

type failRemoveFs struct {
	fs.Fs
	failRemote  string
	failErr     error
	enabled     bool
	features    *fs.Features
	removeCalls []string
}

type failRemoveObject struct {
	fs.Object
	parent *failRemoveFs
}

type failOpenFs struct {
	fs.Fs
	failErr    error
	enabled    bool
	shouldFail func(string) bool
	features   *fs.Features
}

type failOpenObject struct {
	fs.Object
	parent *failOpenFs
}

func newFailRemoveFs(base fs.Fs, failRemote string, failErr error) *failRemoveFs {
	f := &failRemoveFs{
		Fs:         base,
		failRemote: failRemote,
		failErr:    failErr,
	}
	f.features = base.Features().Wrap(f)
	return f
}

func (f *failRemoveFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	obj, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return f.wrapObject(obj), nil
}

func (f *failRemoveFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.Fs.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	wrappedEntries := make(fs.DirEntries, len(entries))
	for i, entry := range entries {
		if obj, ok := entry.(fs.Object); ok {
			wrappedEntries[i] = f.wrapObject(obj)
		} else {
			wrappedEntries[i] = entry
		}
	}
	return wrappedEntries, nil
}

func (f *failRemoveFs) Features() *fs.Features {
	return f.features
}

func (f *failRemoveFs) UnWrap() fs.Fs {
	return f.Fs
}

func (f *failRemoveFs) wrapObject(obj fs.Object) fs.Object {
	if f.enabled && obj.Remote() == f.failRemote {
		return &failRemoveObject{
			Object: obj,
			parent: f,
		}
	}
	return obj
}

func (o *failRemoveObject) Remove(ctx context.Context) error {
	o.parent.removeCalls = append(o.parent.removeCalls, o.Remote())
	return o.parent.failErr
}

func (o *failRemoveObject) UnWrap() fs.Object {
	return o.Object
}

func newFailOpenFs(base fs.Fs, failErr error, shouldFail func(string) bool) *failOpenFs {
	f := &failOpenFs{
		Fs:         base,
		failErr:    failErr,
		shouldFail: shouldFail,
	}
	f.features = base.Features().Wrap(f)
	return f
}

func (f *failOpenFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	obj, err := f.Fs.Put(ctx, in, src, options...)
	if err != nil {
		return nil, err
	}
	return f.wrapObject(obj), nil
}

func (f *failOpenFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	obj, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return f.wrapObject(obj), nil
}

func (f *failOpenFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.Fs.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	wrappedEntries := make(fs.DirEntries, len(entries))
	for i, entry := range entries {
		if obj, ok := entry.(fs.Object); ok {
			wrappedEntries[i] = f.wrapObject(obj)
		} else {
			wrappedEntries[i] = entry
		}
	}
	return wrappedEntries, nil
}

func (f *failOpenFs) Features() *fs.Features {
	return f.features
}

func (f *failOpenFs) UnWrap() fs.Fs {
	return f.Fs
}

func (f *failOpenFs) wrapObject(obj fs.Object) fs.Object {
	if f.enabled && f.shouldFail(obj.Remote()) {
		return &failOpenObject{
			Object: obj,
			parent: f,
		}
	}
	return obj
}

func (o *failOpenObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	return nil, o.parent.failErr
}

func (o *failOpenObject) UnWrap() fs.Object {
	return o.Object
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

func newTestChunkerChunkWriter(t *testing.T, remote string, size int64) (*chunkerChunkWriter, *trackingPutFs) {
	t.Helper()
	ctx := context.Background()
	base := &trackingPutFs{Fs: object.MemoryFs}
	f := newTestChunkerFsWithBase(t, ctx, base, "none")

	src := object.NewStaticObjectInfo(remote, mtime1, size, true, nil, nil)
	writer, err := f.newChunkerChunkWriter(ctx, remote, src, false)
	require.NoError(t, err)
	return writer, base
}

func newTestChunkerFsWithBase(t *testing.T, ctx context.Context, base fs.Fs, hashType string) *Fs {
	t.Helper()
	f := &Fs{
		base: base,
		name: "chunker",
		opt: Options{
			ChunkSize:    fs.SizeSuffix(4),
			NameFormat:   "*.rclone_chunk.###",
			StartFrom:    1,
			MetaFormat:   "simplejson",
			HashType:     hashType,
			Transactions: "rename",
		},
	}
	f.dirSort = true
	require.NoError(t, f.configure(f.opt.NameFormat, f.opt.MetaFormat, f.opt.HashType, f.opt.Transactions))
	f.features = (&fs.Features{
		CaseInsensitive:          true,
		DuplicateFiles:           true,
		WriteMimeType:            true,
		BucketBased:              true,
		CanHaveEmptyDirectories:  true,
		ServerSideAcrossConfigs:  true,
		ReadDirMetadata:          true,
		WriteDirMetadata:         true,
		WriteDirSetModTime:       true,
		UserDirMetadata:          true,
		DirModTimeUpdatesOnWrite: true,
	}).Fill(ctx, f).Mask(ctx, base).WrapsFs(f, base)
	f.features.ListR = nil
	f.features.ListP = nil
	f.features.OpenChunkWriter = f.OpenChunkWriter
	f.features.ChunkWriterDoesntSeek = true
	f.features.PutStream = f.PutStream
	f.features.DisableList(fs.GetConfig(ctx).DisableFeatures)
	return f
}

func newTestChunkerChunkWriterWithBase(t *testing.T, base fs.Fs, remote string, size int64) *chunkerChunkWriter {
	t.Helper()
	ctx := context.Background()
	f := newTestChunkerFsWithBase(t, ctx, base, "none")

	src := object.NewStaticObjectInfo(remote, mtime1, size, true, nil, nil)
	writer, err := f.newChunkerChunkWriter(ctx, remote, src, false)
	require.NoError(t, err)
	return writer
}

func newTestChunkerChunkWriterWithSource(t *testing.T, base fs.Fs, hashType, remote string, src fs.ObjectInfo) *chunkerChunkWriter {
	t.Helper()
	ctx := context.Background()
	f := newTestChunkerFsWithBase(t, ctx, base, hashType)

	writer, err := f.newChunkerChunkWriter(ctx, remote, src, false)
	require.NoError(t, err)
	return writer
}

func newChunkWriterRetryTestContext(retries int) context.Context {
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = retries
	return ctx
}

func newTestLocalChunkerChunkWriter(t *testing.T, remote string, size int64) (*chunkerChunkWriter, fs.Fs) {
	t.Helper()
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	return newTestChunkerChunkWriterWithBase(t, base, remote, size), base
}

var mtime1 = fstest.Time("2001-02-03T04:05:06.499999999Z")

func newTestOpenChunkWriterFs(t *testing.T, hashType string) (*Fs, fs.Fs) {
	t.Helper()
	return newTestOpenChunkWriterFsWithContext(t, context.Background(), hashType)
}

func newTestOpenChunkWriterFsWithContext(t *testing.T, ctx context.Context, hashType string) (*Fs, fs.Fs) {
	t.Helper()
	return newTestOpenChunkWriterFsWithOptions(t, ctx, hashType, "4B")
}

func newTestOpenChunkWriterFsWithOptions(t *testing.T, ctx context.Context, hashType, chunkSize string) (*Fs, fs.Fs) {
	t.Helper()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	require.NoError(t, base.Mkdir(ctx, ""))
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})

	configMap := configmap.Simple{
		"remote":     fs.ConfigStringFull(base),
		"chunk_size": chunkSize,
		"hash_type":  hashType,
	}
	chunkerFs, err := fs.NewFs(ctx, fmt.Sprintf(":chunker,%s:", configMap.String()))
	require.NoError(t, err)
	f, ok := chunkerFs.(*Fs)
	require.True(t, ok)
	return f, base
}

func testPutFile(ctx context.Context, t *testing.T, f fs.Fs, name, contents, message string, check bool) fs.Object {
	item := fstest.Item{Path: name, ModTime: mtime1}
	obj := fstests.PutTestContents(ctx, t, f, &item, contents, check)
	assert.NotNil(t, obj, message)
	return obj
}

func checkMultipartWriterPuts(t *testing.T, f *Fs, puts []multipartTrackingPutCall, remote string, dataChunks int, requireSeekable bool) {
	t.Helper()
	require.Len(t, puts, dataChunks+1)
	for i, call := range puts[:dataChunks] {
		if requireSeekable {
			assert.True(t, call.seekable, "data chunk %d should be uploaded from multipart buffer", i)
		}
		parentPath, chunkNo, ctrlType, xactID := f.parseChunkName(call.remote)
		assert.Equal(t, remote, parentPath)
		assert.Equal(t, i, chunkNo)
		assert.Empty(t, ctrlType)
		assert.NotEmpty(t, xactID)
	}
	assert.Equal(t, remote, puts[dataChunks].remote)
}

func TestChunkerOpenChunkWriterAdvertisedForSupportedHashTypes(t *testing.T) {
	for _, hashType := range []string{"none", "md5", "sha1", "md5all", "sha1all", "md5quick", "sha1quick"} {
		t.Run(hashType, func(t *testing.T) {
			f, _ := newTestOpenChunkWriterFs(t, hashType)
			require.NotNil(t, f.Features().OpenChunkWriter)
			require.NotNil(t, f.Features().PutStream)
		})
	}
}

func TestChunkerOpenChunkWriterRespectsDisableFeatures(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.DisableFeatures = []string{"OpenChunkWriter"}

	f, _ := newTestOpenChunkWriterFsWithContext(t, ctx, "none")
	assert.Nil(t, f.Features().OpenChunkWriter)

	ctx, ci = fs.AddConfig(context.Background())
	ci.DisableFeatures = []string{"ChunkWriterDoesntSeek"}

	f, _ = newTestOpenChunkWriterFsWithContext(t, ctx, "none")
	assert.False(t, f.Features().ChunkWriterDoesntSeek)
}

func TestChunkerConcurrentBufferConfig(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name  string
		extra string
		want  fs.Tristate
	}{
		{name: "unset", want: fs.Tristate{}},
		{name: "true", extra: ",concurrent_buffer=true", want: fs.Tristate{Valid: true, Value: true}},
		{name: "false", extra: ",concurrent_buffer=false", want: fs.Tristate{Valid: true, Value: false}},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, err := fs.TemporaryLocalFs(ctx)
			require.NoError(t, err)
			require.NoError(t, base.Mkdir(ctx, ""))
			t.Cleanup(func() {
				assert.NoError(t, operations.Purge(ctx, base, ""))
			})

			chunkerFs, err := fs.NewFs(ctx, fmt.Sprintf(":chunker,remote='%s',chunk_size=4B,hash_type=md5%s:", fs.ConfigStringFull(base), test.extra))
			require.NoError(t, err)
			f, ok := chunkerFs.(*Fs)
			require.True(t, ok)
			assert.Equal(t, test.want, f.opt.ConcurrentBuffer)
		})
	}
}

func TestChunkerOpenChunkWriterDirectSupportsHashModes(t *testing.T) {
	for _, hashType := range []string{"md5", "md5quick"} {
		t.Run(hashType, func(t *testing.T) {
			f, _ := newTestOpenChunkWriterFs(t, hashType)
			require.NotNil(t, f.Features().OpenChunkWriter)
			assert.True(t, f.Features().ChunkWriterDoesntSeek)

			src := object.NewStaticObjectInfo("file.txt", mtime1, 4, true, nil, nil)
			info, writer, err := f.OpenChunkWriter(context.Background(), src.Remote(), src)
			require.NoError(t, err)
			assert.Equal(t, int64(4), info.ChunkSize)
			assert.NotNil(t, writer)
		})
	}
}

func TestChunkerOpenChunkWriterDirectSuccess(t *testing.T) {
	f, _ := newTestOpenChunkWriterFs(t, "none")
	src := object.NewStaticObjectInfo("dir/file.txt", mtime1, 8, true, nil, nil)

	info, writer, err := f.OpenChunkWriter(context.Background(), src.Remote(), src)
	require.NoError(t, err)
	require.NotNil(t, writer)
	assert.Equal(t, int64(4), info.ChunkSize)
	assert.Equal(t, 1, info.Concurrency)
	assert.False(t, info.LeavePartsOnError)
	_, ok := writer.(*chunkerChunkWriter)
	assert.True(t, ok)
}

func TestChunkerOpenChunkWriterDirectWritesOutOfOrder(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestOpenChunkWriterFs(t, "none")
	src := object.NewStaticObjectInfo("dir/file.txt", mtime1, 10, true, nil, nil)

	info, writer, err := f.OpenChunkWriter(ctx, src.Remote(), src, &fs.ChunkOption{ChunkSize: 2})
	require.NoError(t, err)
	require.NotNil(t, writer)
	assert.Equal(t, int64(4), info.ChunkSize)
	assert.Equal(t, 1, info.Concurrency)
	assert.False(t, info.LeavePartsOnError)

	n, err := writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)
	n, err = writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)
	n, err = writer.WriteChunk(ctx, 2, strings.NewReader("ij"))
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	require.NoError(t, writer.Close(ctx))

	obj, err := f.NewObject(ctx, "dir/file.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(10), obj.Size())
	assert.Equal(t, "abcdefghij", fstests.ReadObject(ctx, t, obj, -1))
}

func TestChunkerOpenChunkWriterRejectsChunkRemote(t *testing.T) {
	f, _ := newTestOpenChunkWriterFs(t, "none")
	chunkRemote := f.makeChunkName("parent.txt", 0, "", "")
	src := object.NewStaticObjectInfo(chunkRemote, mtime1, 4, true, nil, nil)

	_, writer, err := f.OpenChunkWriter(context.Background(), chunkRemote, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk overlap")
	assert.Nil(t, writer)
}

func TestChunkerOpenChunkWriterRejectsFutureMetadata(t *testing.T) {
	f, _ := newTestOpenChunkWriterFs(t, "none")
	ctx := context.Background()
	const file = "future/test"
	meta := `{"ver":999,"nchunks":3,"size":9,"garbage":"litter"}`
	putPart := func(name string, part int, data string) {
		if part > 0 {
			name = f.makeChunkName(name, part-1, "", "")
		}
		item := fstest.Item{Path: name, ModTime: mtime1}
		obj := fstests.PutTestContents(ctx, t, f.base, &item, data, true)
		require.NotNil(t, obj)
	}
	putPart(file, 0, meta)
	putPart(file, 1, "abc")
	putPart(file, 2, "def")
	putPart(file, 3, "ghi")

	src := object.NewStaticObjectInfo(file, mtime1, 9, true, nil, nil)
	_, writer, err := f.OpenChunkWriter(ctx, file, src)
	assert.ErrorIs(t, err, ErrMetaUnknown)
	assert.Nil(t, writer)
}

func TestChunkerHashNonePutUsesMultipartWriterPath(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	trackingBase := newMultipartTrackingFs(base)
	f := newTestChunkerFsWithBase(t, ctx, trackingBase, "none")

	src := object.NewStaticObjectInfo("dir/file.txt", mtime1, 10, true, nil, nil)
	obj, err := f.Put(ctx, strings.NewReader("abcdefghij"), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "abcdefghij", fstests.ReadObject(ctx, t, obj, -1))

	trackingBase.mu.Lock()
	puts := append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	checkMultipartWriterPuts(t, f, puts, src.Remote(), 3, true)
}

func TestChunkerHashNoneUpdateUsesMultipartWriterPath(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	trackingBase := newMultipartTrackingFs(base)
	f := newTestChunkerFsWithBase(t, ctx, trackingBase, "none")

	oldSrc := object.NewStaticObjectInfo("dir/file.txt", mtime1, 3, true, nil, nil)
	oldObj, err := f.Put(ctx, strings.NewReader("old"), oldSrc)
	require.NoError(t, err)
	obj, ok := oldObj.(*Object)
	require.True(t, ok)
	trackingBase.mu.Lock()
	trackingBase.puts = nil
	trackingBase.mu.Unlock()

	updateSrc := object.NewStaticObjectInfo("source-name.txt", mtime1, 10, true, nil, nil)
	require.NoError(t, obj.Update(ctx, strings.NewReader("abcdefghij"), updateSrc))
	assert.Equal(t, "dir/file.txt", obj.Remote())
	assert.Equal(t, "abcdefghij", fstests.ReadObject(ctx, t, obj, -1))

	trackingBase.mu.Lock()
	puts := append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	checkMultipartWriterPuts(t, f, puts, "dir/file.txt", 3, true)
}

func TestChunkerHashNonePutStreamUnknownSizeUsesMultipartWriterPath(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	trackingBase := newMultipartTrackingFs(base)
	trackingBase.features.PutStream = nil
	f := newTestChunkerFsWithBase(t, ctx, trackingBase, "none")
	require.NotNil(t, f.Features().PutStream)

	src := object.NewStaticObjectInfo("stream/file.txt", mtime1, -1, true, nil, nil)
	obj, err := f.PutStream(ctx, strings.NewReader("abcdefghij"), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, int64(10), obj.Size())
	assert.Equal(t, "abcdefghij", fstests.ReadObject(ctx, t, obj, -1))

	trackingBase.mu.Lock()
	puts := append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	checkMultipartWriterPuts(t, f, puts, src.Remote(), 3, true)
	assert.Equal(t, int64(10), obj.Size())
}

func TestChunkerHashNonePutStreamZeroByteUsesMultipartWriterPath(t *testing.T) {
	ctx := context.Background()
	f, _ := newTestOpenChunkWriterFs(t, "none")
	src := object.NewStaticObjectInfo("empty.txt", mtime1, -1, true, nil, nil)

	obj, err := f.PutStream(ctx, strings.NewReader(""), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, int64(0), obj.Size())
	assert.Equal(t, "", fstests.ReadObject(ctx, t, obj, -1))

	baseObj, err := f.base.NewObject(ctx, "empty.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(0), baseObj.Size())
}

func TestChunkerHashMD5PutUsesMultipartWriterPath(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	trackingBase := newMultipartTrackingFs(base)
	f := newTestChunkerFsWithBase(t, ctx, trackingBase, "md5")

	src := object.NewStaticObjectInfo("dir/file.txt", mtime1, 10, true, nil, nil)
	obj, err := f.Put(ctx, strings.NewReader("abcdefghij"), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "abcdefghij", fstests.ReadObject(ctx, t, obj, -1))
	sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "a925576942e94b2ef57a066101b48876", sum)

	trackingBase.mu.Lock()
	puts := append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	checkMultipartWriterPuts(t, f, puts, src.Remote(), 3, false)
}

func TestChunkerHashMD5PutStreamUnknownSizeUsesMultipartWriterPath(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	trackingBase := newMultipartTrackingFs(base)
	trackingBase.features.PutStream = nil
	f := newTestChunkerFsWithBase(t, ctx, trackingBase, "md5")
	require.NotNil(t, f.Features().PutStream)

	src := object.NewStaticObjectInfo("stream/md5.txt", mtime1, -1, true, nil, nil)
	obj, err := f.PutStream(ctx, strings.NewReader("abcdefghij"), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, int64(10), obj.Size())
	assert.Equal(t, "abcdefghij", fstests.ReadObject(ctx, t, obj, -1))
	sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "a925576942e94b2ef57a066101b48876", sum)

	trackingBase.mu.Lock()
	puts := append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	checkMultipartWriterPuts(t, f, puts, src.Remote(), 3, false)
}

func TestChunkerHashMD5PutStreamConcurrentBufferFalseForwardOnly(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	trackingBase := newMultipartTrackingFs(base)
	trackingBase.features.PutStream = nil
	f := newTestChunkerFsWithBase(t, ctx, trackingBase, "md5")
	f.opt.ConcurrentBuffer = fs.Tristate{Valid: true, Value: false}

	src := object.NewStaticObjectInfo("stream/md5-forward.txt", mtime1, -1, true, nil, nil)
	obj, err := f.PutStream(ctx, readers.NoSeeker{Reader: strings.NewReader("abcdefghij")}, src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, int64(10), obj.Size())

	trackingBase.mu.Lock()
	puts := append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	checkMultipartWriterPuts(t, f, puts, src.Remote(), 3, false)
}

func TestChunkerHashMD5UpdateUnknownSizeUsesMultipartWriterPath(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	trackingBase := newMultipartTrackingFs(base)
	trackingBase.features.PutStream = nil
	f := newTestChunkerFsWithBase(t, ctx, trackingBase, "md5")

	oldSrc := object.NewStaticObjectInfo("dir/md5-update.txt", mtime1, 3, true, nil, nil)
	oldObj, err := f.Put(ctx, strings.NewReader("old"), oldSrc)
	require.NoError(t, err)
	obj, ok := oldObj.(*Object)
	require.True(t, ok)
	trackingBase.mu.Lock()
	trackingBase.puts = nil
	trackingBase.mu.Unlock()

	updateSrc := object.NewStaticObjectInfo("source-name.txt", mtime1, -1, true, nil, nil)
	require.NoError(t, obj.Update(ctx, strings.NewReader("abcdefghij"), updateSrc))
	assert.Equal(t, "dir/md5-update.txt", obj.Remote())
	assert.Equal(t, int64(10), obj.Size())
	assert.Equal(t, "abcdefghij", fstests.ReadObject(ctx, t, obj, -1))
	sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "a925576942e94b2ef57a066101b48876", sum)

	trackingBase.mu.Lock()
	puts := append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	checkMultipartWriterPuts(t, f, puts, "dir/md5-update.txt", 3, false)
}

func TestChunkerHashNonePutChunkRemoteKeepsSoftFailHardBehavior(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	trackingBase := newMultipartTrackingFs(base)
	f := newTestChunkerFsWithBase(t, ctx, trackingBase, "none")

	chunkRemote := f.makeChunkName("parent.txt", 0, "", "")
	src := object.NewStaticObjectInfo(chunkRemote, mtime1, 4, true, nil, nil)
	obj, err := f.Put(ctx, strings.NewReader("data"), src)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, "data", fstests.ReadObject(ctx, t, obj, -1))

	trackingBase.mu.Lock()
	puts := append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	require.Len(t, puts, 1)
	assert.True(t, puts[0].seekable)

	trackingBase.mu.Lock()
	trackingBase.puts = nil
	trackingBase.mu.Unlock()
	f.opt.FailHard = true

	strictChunkRemote := f.makeChunkName("strict-parent.txt", 0, "", "")
	strictSrc := object.NewStaticObjectInfo(strictChunkRemote, mtime1, 4, true, nil, nil)
	obj, err = f.Put(ctx, strings.NewReader("data"), strictSrc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk overlap")
	assert.Nil(t, obj)

	trackingBase.mu.Lock()
	puts = append([]multipartTrackingPutCall(nil), trackingBase.puts...)
	trackingBase.mu.Unlock()
	assert.Empty(t, puts)
}

func TestChunkerHashNoneUploadMultipartFinalizerFailureAborts(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	sentinelErr := errors.New("metadata write failed")
	failBase := newFailPutFs(base, "file.txt", sentinelErr)
	f := newTestChunkerFsWithBase(t, ctx, failBase, "none")

	src := object.NewStaticObjectInfo("file.txt", mtime1, 8, true, nil, nil)
	failBase.enabled = true
	obj, err := f.Put(ctx, strings.NewReader("abcdefgh"), src)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinelErr)
	assert.Nil(t, obj)

	for i := range 2 {
		tempRemote := f.makeChunkName(src.Remote(), i, "", "")
		_, err := base.NewObject(ctx, tempRemote)
		assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	}
	_, err = base.NewObject(ctx, src.Remote())
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

func TestChunkerChunkWriterWriteChunkTempChunkName(t *testing.T) {
	ctx := context.Background()
	writer, base := newTestChunkerChunkWriter(t, "dir/file.txt", 12)

	n, err := writer.WriteChunk(ctx, 2, strings.NewReader("data"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)

	require.Len(t, base.puts, 1)
	wantRemote := writer.f.makeChunkName("dir/file.txt", 2, "", writer.st.xactID)
	assert.Equal(t, wantRemote, base.puts[0].remote)
	assert.Equal(t, "data", base.puts[0].content)
	assert.Equal(t, int64(4), base.puts[0].size)

	require.Len(t, writer.st.chunks, 3)
	assert.Nil(t, writer.st.chunks[0])
	assert.Nil(t, writer.st.chunks[1])
	require.NotNil(t, writer.st.chunks[2])
	assert.Equal(t, wantRemote, writer.st.chunks[2].Remote())
	assert.Equal(t, writer.st.chunks[2], writer.written[2])
	assert.Equal(t, int64(4), writer.st.readCount)
	assert.Equal(t, int64(4), writer.sizeWritten)
}

func TestChunkerChunkWriterWriteChunkRejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	writer, base := newTestChunkerChunkWriter(t, "file.txt", 8)

	n, err := writer.WriteChunk(ctx, 0, strings.NewReader("first"))
	require.NoError(t, err)
	assert.Equal(t, int64(5), n)

	n, err = writer.WriteChunk(ctx, 0, strings.NewReader("second"))
	require.Error(t, err)
	assert.Equal(t, int64(0), n)
	assert.Contains(t, err.Error(), "duplicate chunk number")

	require.Len(t, base.puts, 1)
	assert.Equal(t, "first", base.puts[0].content)
	assert.Equal(t, int64(5), writer.st.readCount)
	assert.Equal(t, int64(5), writer.sizeWritten)
}

func TestChunkerChunkWriterWriteChunkRejectsConcurrentDuplicate(t *testing.T) {
	ctx := context.Background()
	base := newBlockingPutFs()
	writer := newTestChunkerChunkWriterWithBase(t, base, "file.txt", 8)

	firstDone := make(chan error, 1)
	go func() {
		_, err := writer.WriteChunk(ctx, 0, strings.NewReader("first"))
		firstDone <- err
	}()
	<-base.started

	secondDone := make(chan error, 1)
	go func() {
		_, err := writer.WriteChunk(ctx, 0, strings.NewReader("second"))
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate chunk number")
	case <-time.After(5 * time.Second):
		t.Fatal("duplicate chunk write did not return while first upload was in flight")
	}

	close(base.release)
	require.NoError(t, <-firstDone)
	base.mu.Lock()
	defer base.mu.Unlock()
	require.Len(t, base.puts, 1)
	assert.Equal(t, "first", base.puts[0].content)
}

func TestChunkerChunkWriterWriteChunkAllowsConcurrentDifferentChunks(t *testing.T) {
	ctx := context.Background()
	writer, _ := newTestChunkerChunkWriter(t, "file.txt", 8)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for chunkNumber, content := range []string{"abcd", "efgh"} {
		wg.Add(1)
		go func(chunkNumber int, content string) {
			defer wg.Done()
			n, err := writer.WriteChunk(ctx, chunkNumber, strings.NewReader(content))
			if err == nil {
				assert.Equal(t, int64(len(content)), n)
			}
			errs <- err
		}(chunkNumber, content)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, writer.st.chunks, 2)
	require.NotNil(t, writer.st.chunks[0])
	require.NotNil(t, writer.st.chunks[1])
	assert.Equal(t, int64(8), writer.st.readCount)
	assert.Equal(t, int64(8), writer.sizeWritten)
	assert.Len(t, writer.written, 2)
}

func TestChunkerChunkWriterWriteChunkRejectsInvalidChunkNumber(t *testing.T) {
	ctx := context.Background()

	for _, chunkNumber := range []int{-1, maxSafeChunkNumber + 1} {
		t.Run(fmt.Sprint(chunkNumber), func(t *testing.T) {
			writer, base := newTestChunkerChunkWriter(t, "file.txt", 4)

			n, err := writer.WriteChunk(ctx, chunkNumber, strings.NewReader("data"))
			require.Error(t, err)
			assert.Equal(t, int64(0), n)
			assert.Empty(t, base.puts)
			assert.Empty(t, writer.written)
			assert.Empty(t, writer.st.chunks)
			assert.Equal(t, int64(0), writer.st.readCount)
		})
	}
}

func TestChunkerChunkWriterWriteChunkRetriesSameChunk(t *testing.T) {
	ctx := newChunkWriterRetryTestContext(2)
	uploadErr := errors.New("temporary upload failure")
	base := newChunkWriterRetryPutFs(chunkWriterPutResult{err: fserrors.RetryError(uploadErr)})
	writer := newTestChunkerChunkWriterWithBase(t, base, "file.txt", 4)
	wantRemote := writer.f.makeChunkName("file.txt", 0, "", writer.st.xactID)

	n, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)

	require.Len(t, base.puts, 2)
	for _, put := range base.puts {
		assert.Equal(t, wantRemote, put.remote)
		assert.Equal(t, "abcd", put.content)
		assert.Equal(t, int64(4), put.size)
	}

	require.Len(t, writer.st.chunks, 1)
	require.NotNil(t, writer.st.chunks[0])
	assert.Equal(t, wantRemote, writer.st.chunks[0].Remote())
	assert.Equal(t, writer.st.chunks[0], writer.written[0])
	assert.Equal(t, int64(4), writer.st.readCount)
	assert.Equal(t, int64(4), writer.sizeWritten)
}

func TestChunkerChunkWriterWriteChunkCleansFailedReturnedObject(t *testing.T) {
	ctx := newChunkWriterRetryTestContext(2)
	uploadErr := errors.New("temporary upload failure")
	base := newChunkWriterRetryPutFs(chunkWriterPutResult{
		err:          fserrors.RetryError(uploadErr),
		returnObject: true,
		storeObject:  true,
	})
	writer := newTestChunkerChunkWriterWithBase(t, base, "file.txt", 4)
	wantRemote := writer.f.makeChunkName("file.txt", 0, "", writer.st.xactID)

	n, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)

	require.Len(t, base.puts, 2)
	assert.Equal(t, []string{wantRemote}, base.removes)
	require.Len(t, writer.st.chunks, 1)
	require.NotNil(t, writer.st.chunks[0])
	assert.Same(t, base.objects[wantRemote], writer.st.chunks[0])
	assert.Same(t, base.objects[wantRemote], writer.written[0])
}

func TestChunkerChunkWriterWriteChunkCleansFailedStoredObject(t *testing.T) {
	ctx := newChunkWriterRetryTestContext(2)
	uploadErr := errors.New("temporary upload failure")
	base := newChunkWriterRetryPutFs(chunkWriterPutResult{
		err:         fserrors.RetryError(uploadErr),
		storeObject: true,
	})
	writer := newTestChunkerChunkWriterWithBase(t, base, "file.txt", 4)
	wantRemote := writer.f.makeChunkName("file.txt", 0, "", writer.st.xactID)

	n, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)

	require.Len(t, base.puts, 2)
	assert.Equal(t, []string{wantRemote}, base.removes)
	require.Len(t, writer.st.chunks, 1)
	require.NotNil(t, writer.st.chunks[0])
	assert.Same(t, base.objects[wantRemote], writer.st.chunks[0])
	assert.Same(t, base.objects[wantRemote], writer.written[0])
}

func TestChunkerChunkWriterWriteChunkDoesNotRetryNoRetryErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "NoRetry",
			err:  fserrors.NoRetryError(fserrors.RetryError(errors.New("do not retry"))),
		},
		{
			name: "NoLowLevelRetry",
			err:  fserrors.NoLowLevelRetryError(io.ErrUnexpectedEOF),
		},
		{
			name: "Fatal",
			err:  fserrors.FatalError(fserrors.RetryError(errors.New("fatal"))),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := newChunkWriterRetryTestContext(3)
			base := newChunkWriterRetryPutFs(chunkWriterPutResult{err: test.err})
			writer := newTestChunkerChunkWriterWithBase(t, base, "file.txt", 4)

			_, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
			require.Error(t, err)
			assert.ErrorIs(t, err, test.err)
			assert.Len(t, base.puts, 1)
			assert.Empty(t, writer.st.chunks)
			assert.Empty(t, writer.written)
			assert.Zero(t, writer.st.readCount)
			assert.Zero(t, writer.sizeWritten)
		})
	}
}

func TestChunkerChunkWriterWriteChunkDoesNotRetryContextCancellation(t *testing.T) {
	ctx := newChunkWriterRetryTestContext(3)
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	uploadErr := errors.New("temporary upload failure")
	base := newChunkWriterRetryPutFs(chunkWriterPutResult{err: fserrors.RetryError(uploadErr)})
	writer := newTestChunkerChunkWriterWithBase(t, base, "file.txt", 4)

	_, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.ErrorIs(t, err, uploadErr)
	assert.Len(t, base.puts, 1)
	assert.Empty(t, writer.st.chunks)
	assert.Empty(t, writer.written)
	assert.Zero(t, writer.st.readCount)
	assert.Zero(t, writer.sizeWritten)
}

func TestChunkerChunkWriterCloseCommitsMultiChunkObject(t *testing.T) {
	ctx := context.Background()
	writer, _ := newTestLocalChunkerChunkWriter(t, "dir/file.txt", 10)

	n, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)
	n, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)
	n, err = writer.WriteChunk(ctx, 2, strings.NewReader("ij"))
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	require.NoError(t, writer.Close(ctx))
	assert.True(t, writer.closed)

	obj, err := writer.f.NewObject(ctx, "dir/file.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(10), obj.Size())
	assert.Equal(t, "abcdefghij", fstests.ReadObject(ctx, t, obj, -1))
}

func TestChunkerChunkWriterHashMD5UsesSourceHash(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})

	src := newHashTrackingObjectInfo("file.txt", 8, object.MemoryFs, map[hash.Type]string{
		hash.MD5: "e2fc714c4727ee9395f324cd2e7f331f",
	})
	writer := newTestChunkerChunkWriterWithSource(t, base, "md5", "file.txt", src)

	_, err = writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	obj, err := writer.f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "e2fc714c4727ee9395f324cd2e7f331f", sum)
	assert.Equal(t, []hash.Type{hash.MD5}, src.HashCalls())
}

func TestChunkerChunkWriterHashMD5SlowSourceSkipsPreHash(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})

	srcFs := newSlowHashFs(object.MemoryFs)
	src := newHashTrackingObjectInfo("file.txt", 8, srcFs, map[hash.Type]string{
		hash.MD5: "00000000000000000000000000000000",
	})
	chunkerFs := newTestChunkerFsWithBase(t, ctx, base, "md5")
	writer, err := chunkerFs.newChunkerChunkWriter(ctx, "file.txt", chunkerFs.wrapInfo(src, "file.txt", src.Size()), false)
	require.NoError(t, err)

	_, err = writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	obj, err := writer.f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "e8dc4081b13434b45189a720b77b6818", sum)
	assert.Empty(t, src.HashCalls())
}

func TestChunkerChunkWriterHashCalculatedFallback(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name     string
		hashType string
		sumType  hash.Type
		sum      string
	}{
		{
			name:     "MD5",
			hashType: "md5",
			sumType:  hash.MD5,
			sum:      "e8dc4081b13434b45189a720b77b6818",
		}, {
			name:     "SHA1",
			hashType: "sha1",
			sumType:  hash.SHA1,
			sum:      "425af12a0743502b322e93a015bcf868e324d56a",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, err := fs.TemporaryLocalFs(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				assert.NoError(t, operations.Purge(ctx, base, ""))
			})

			src := newHashTrackingObjectInfo("file.txt", 8, object.MemoryFs, nil)
			writer := newTestChunkerChunkWriterWithSource(t, base, test.hashType, "file.txt", src)

			_, err = writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
			require.NoError(t, err)
			_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
			require.NoError(t, err)
			require.NoError(t, writer.Close(ctx))

			obj, err := writer.f.NewObject(ctx, "file.txt")
			require.NoError(t, err)
			sum, err := obj.Hash(ctx, test.sumType)
			require.NoError(t, err)
			assert.Equal(t, test.sum, sum)
			assert.Equal(t, []hash.Type{test.sumType}, src.HashCalls())
		})
	}
}

func TestChunkerChunkWriterHashRetryRestoresCheckpoint(t *testing.T) {
	ctx := newChunkWriterRetryTestContext(2)
	uploadErr := errors.New("temporary upload failure")
	base := newChunkWriterRetryPutFs(chunkWriterPutResult{err: fserrors.RetryError(uploadErr)})
	writer := newTestChunkerChunkWriterWithSource(t, base, "md5", "file.txt", newHashTrackingObjectInfo("file.txt", 8, newSlowHashFs(object.MemoryFs), nil))

	_, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	require.GreaterOrEqual(t, len(base.puts), 3)
	assert.Equal(t, "abcd", base.puts[0].content)
	assert.Equal(t, "abcd", base.puts[1].content)
	assert.Equal(t, "efgh", base.puts[2].content)
	assert.Equal(t, "e8dc4081b13434b45189a720b77b6818", writer.st.md5)
}

func TestChunkerChunkWriterHashReadFailurePreservesOldChunkedObject(t *testing.T) {
	ctx := context.Background()
	const filename = "dir/file.txt"
	sentinelErr := errors.New("temp chunk open failed")

	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	failBase := newFailOpenFs(base, sentinelErr, func(remote string) bool {
		return strings.Contains(remote, ".rclone_chunk.") && strings.Contains(remote, "_")
	})
	f := newTestChunkerFsWithBase(t, ctx, newSlowHashFs(failBase), "md5")

	oldContents := "abcdefghij"
	oldSrc := object.NewStaticObjectInfo(filename, mtime1, int64(len(oldContents)), true, nil, nil)
	oldObj, err := f.Put(ctx, strings.NewReader(oldContents), oldSrc)
	require.NoError(t, err)
	oldChunkerObject, ok := oldObj.(*Object)
	require.True(t, ok)
	require.Greater(t, len(oldChunkerObject.chunks), 1)

	oldChunkRemotes := make([]string, 0, len(oldChunkerObject.chunks))
	for _, chunk := range oldChunkerObject.chunks {
		oldChunkRemotes = append(oldChunkRemotes, chunk.Remote())
	}

	replacementContents := "klmnopqrstuv"
	replacementSrc := object.NewStaticObjectInfo(filename, mtime1, int64(len(replacementContents)), true, nil, nil)
	failBase.enabled = true
	writer, err := f.newChunkerChunkWriter(ctx, filename, replacementSrc, false)
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader(replacementContents[4:8]))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 0, strings.NewReader(replacementContents[:4]))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 2, strings.NewReader(replacementContents[8:]))
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinelErr)

	failBase.enabled = false
	restoredObject, err := f.NewObject(ctx, filename)
	require.NoError(t, err)
	assert.Equal(t, oldContents, fstests.ReadObject(ctx, t, restoredObject, -1))

	for _, remote := range oldChunkRemotes {
		_, err := base.NewObject(ctx, remote)
		require.NoError(t, err, "old active chunk exists after failed hash read")
	}
}

func TestChunkerChunkWriterHashForcedConcurrentOrdered(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})

	src := newHashTrackingObjectInfo("file.txt", 8, object.MemoryFs, nil)
	writer := newTestChunkerChunkWriterWithSource(t, base, "md5", "file.txt", src)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
		errs <- err
	}()
	go func() {
		defer wg.Done()
		_, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
		errs <- err
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close(ctx))

	obj, err := writer.f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "e8dc4081b13434b45189a720b77b6818", sum)
	assert.Equal(t, []hash.Type{hash.MD5}, src.HashCalls())
}

func TestChunkerOpenChunkWriterForcedConcurrentBufferHashesInOrder(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.MultiThreadSet = true
	ci.MultiThreadStreams = 2

	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	f := newTestChunkerFsWithBase(t, ctx, base, "md5")
	f.opt.ConcurrentBuffer = fs.Tristate{Valid: true, Value: true}
	src := newHashTrackingObjectInfo("file.txt", 8, object.MemoryFs, nil)

	_, writer, err := f.OpenChunkWriter(ctx, "file.txt", src)
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	obj, err := f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "e8dc4081b13434b45189a720b77b6818", sum)
}

func TestChunkerOpenChunkWriterForcedConcurrentSeekHashesFromChunks(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.MultiThreadSet = true
	ci.MultiThreadStreams = 2

	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	f := newTestChunkerFsWithBase(t, ctx, base, "md5")
	f.opt.ConcurrentBuffer = fs.Tristate{Valid: true, Value: false}
	src := newHashTrackingObjectInfo("file.txt", 8, object.MemoryFs, nil)

	_, writer, err := f.OpenChunkWriter(ctx, "file.txt", src)
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	obj, err := f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "e8dc4081b13434b45189a720b77b6818", sum)
}

func TestChunkerChunkWriterHashMD5QuickFallbackWritesSHA1(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})

	src := newHashTrackingObjectInfo("file.txt", 8, object.MemoryFs, map[hash.Type]string{
		hash.SHA1: "425af12a0743502b322e93a015bcf868e324d56a",
	})
	writer := newTestChunkerChunkWriterWithSource(t, base, "md5quick", "file.txt", src)

	_, err = writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	obj, err := writer.f.NewObject(ctx, "file.txt")
	require.NoError(t, err)
	md5sum, err := obj.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Empty(t, md5sum)
	sha1sum, err := obj.Hash(ctx, hash.SHA1)
	require.NoError(t, err)
	assert.Equal(t, "425af12a0743502b322e93a015bcf868e324d56a", sha1sum)
	assert.Equal(t, []hash.Type{hash.MD5, hash.SHA1}, src.HashCalls())
}

func TestChunkerChunkWriterHashMD5AllSmallAndZeroByte(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name    string
		content string
		md5     string
	}{
		{
			name:    "ZeroByte",
			content: "",
			md5:     "d41d8cd98f00b204e9800998ecf8427e",
		}, {
			name:    "Small",
			content: "-",
			md5:     "336d5ebc5436534e61d16e63ddfca327",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, err := fs.TemporaryLocalFs(ctx)
			require.NoError(t, err)
			t.Cleanup(func() {
				assert.NoError(t, operations.Purge(ctx, base, ""))
			})

			slowBase := newSlowHashFs(base)
			src := object.NewStaticObjectInfo("file.txt", mtime1, int64(len(test.content)), true, nil, nil)
			writer := newTestChunkerChunkWriterWithSource(t, slowBase, "md5all", "file.txt", src)

			_, err = writer.WriteChunk(ctx, 0, strings.NewReader(test.content))
			require.NoError(t, err)
			require.NoError(t, writer.Close(ctx))

			obj, err := writer.f.NewObject(ctx, "file.txt")
			require.NoError(t, err)
			sum, err := obj.Hash(ctx, hash.MD5)
			require.NoError(t, err)
			assert.Equal(t, test.md5, sum)

			entries, err := base.List(ctx, "")
			require.NoError(t, err)
			assert.Len(t, entries, 2)
			_, err = base.NewObject(ctx, "file.txt")
			assert.NoError(t, err, "metadata must be created")
			chunk, err := base.NewObject(ctx, "file.txt.rclone_chunk.001")
			require.NoError(t, err, "first chunk must be created")
			assert.Equal(t, int64(len(test.content)), chunk.Size())
		})
	}
}

func TestChunkerChunkWriterCloseRejectsMissingChunk(t *testing.T) {
	ctx := context.Background()
	writer, base := newTestChunkerChunkWriter(t, "file.txt", 8)

	n, err := writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)

	err = writer.Close(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing chunk")
	assert.False(t, writer.closed)
	assert.False(t, writer.aborted)
	require.Len(t, base.puts, 1)
}

func TestChunkerChunkWriterFinalizerErrorAllowsAbortOnly(t *testing.T) {
	ctx := context.Background()
	base, err := fs.TemporaryLocalFs(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, operations.Purge(ctx, base, ""))
	})
	sentinelErr := errors.New("metadata write failed")
	failBase := newFailPutFs(base, "file.txt", sentinelErr)
	writer := newTestChunkerChunkWriterWithBase(t, failBase, "file.txt", 8)

	_, err = writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)

	failBase.enabled = true
	err = writer.Close(ctx)
	require.ErrorIs(t, err, sentinelErr)
	assert.False(t, writer.closed)
	assert.False(t, writer.aborted)
	assert.True(t, writer.finalizing)

	n, err := writer.WriteChunk(ctx, 2, strings.NewReader("ij"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk writer is closed")
	assert.Equal(t, int64(0), n)

	err = writer.Close(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk writer is closed")

	tempRemotes := make([]string, 0, len(writer.st.chunks))
	for _, chunk := range writer.st.chunks {
		require.NotNil(t, chunk)
		tempRemotes = append(tempRemotes, chunk.Remote())
		_, err := base.NewObject(ctx, chunk.Remote())
		require.NoError(t, err)
	}

	require.NoError(t, writer.Abort(ctx))
	assert.True(t, writer.aborted)
	for _, remote := range tempRemotes {
		_, err := base.NewObject(ctx, remote)
		assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	}
}

func TestChunkerChunkWriterAbortRemovesUploadedTempChunks(t *testing.T) {
	ctx := context.Background()
	writer, base := newTestLocalChunkerChunkWriter(t, "file.txt", 8)

	_, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	_, err = writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.NoError(t, err)

	var tempRemotes []string
	for _, chunk := range writer.st.chunks {
		tempRemotes = append(tempRemotes, chunk.Remote())
		_, err := base.NewObject(ctx, chunk.Remote())
		require.NoError(t, err)
	}

	require.NoError(t, writer.Abort(ctx))
	assert.True(t, writer.aborted)
	assert.False(t, writer.closed)

	for _, remote := range tempRemotes {
		_, err := base.NewObject(ctx, remote)
		assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
	}
}

func TestChunkerChunkWriterAbortRepeatedStable(t *testing.T) {
	ctx := context.Background()
	writer, base := newTestLocalChunkerChunkWriter(t, "file.txt", 8)

	_, err := writer.WriteChunk(ctx, 0, strings.NewReader("abcd"))
	require.NoError(t, err)
	tempRemote := writer.st.chunks[0].Remote()

	require.NoError(t, writer.Abort(ctx))
	require.NoError(t, writer.Abort(ctx))
	assert.True(t, writer.aborted)

	_, err = base.NewObject(ctx, tempRemote)
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)

	n, err := writer.WriteChunk(ctx, 1, strings.NewReader("efgh"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk writer is aborted")
	assert.Equal(t, int64(0), n)

	err = writer.Close(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chunk writer is aborted")
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
		// This only needs to be larger than the test contents to keep the old object non-chunked.
		"chunk_size":   "1Ki",
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

func testRemoveOldChunksFailureCurrentBehavior(t *testing.T, f *Fs) {
	ctx := context.Background()
	const (
		dir      = "removeoldchunksfail"
		filename = "file"
	)

	fsResult := deriveFs(ctx, t, f, dir, settings{
		"chunk_size":   "10",
		"name_format":  "*.#",
		"hash_type":    "none",
		"transactions": "norename",
		"meta_format":  "simplejson",
	})
	chunkFs, ok := fsResult.(*Fs)
	require.True(t, ok, "fs must be a chunker remote")
	baseFs := chunkFs.base
	_ = operations.Purge(ctx, baseFs, "")
	defer func() {
		_ = operations.Purge(ctx, baseFs, "")
	}()

	chunkSize := int(chunkFs.opt.ChunkSize)
	oldContents := strings.Repeat("o", chunkSize*2+1)
	oldObject := testPutFile(ctx, t, chunkFs, filename, oldContents, "upload old chunked file", true)
	oldChunkerObject, ok := oldObject.(*Object)
	require.True(t, ok)
	require.NotNil(t, oldChunkerObject.main, "old object must have metadata")
	require.Greater(t, len(oldChunkerObject.chunks), 1, "old object must be chunked")
	require.NotEmpty(t, oldChunkerObject.xactID, "old object must use norename transaction chunks")

	oldChunkRemotes := make([]string, 0, len(oldChunkerObject.chunks))
	for _, chunk := range oldChunkerObject.chunks {
		remote := chunk.Remote()
		oldChunkRemotes = append(oldChunkRemotes, remote)
		baseChunk, err := baseFs.NewObject(ctx, remote)
		require.NoError(t, err, "old active chunk exists before replacement")
		require.NotNil(t, baseChunk, "old active chunk exists before replacement")
	}
	failRemote := oldChunkRemotes[0]

	sentinelErr := errors.New("old chunk remove failed")
	failBase := newFailRemoveFs(baseFs, failRemote, sentinelErr)
	chunkFs.base = failBase
	chunkFs.features = chunkFs.features.WrapsFs(chunkFs, failBase)
	failBase.enabled = true

	replacementContents := strings.Repeat("n", chunkSize*3+1)
	replacementInfo := object.NewStaticObjectInfo(filename, mtime1, int64(len(replacementContents)), true, nil, nil)
	err := oldChunkerObject.Update(ctx, bytes.NewBufferString(replacementContents), replacementInfo)
	require.NoError(t, err, "removeOldChunks must not fail the successful replacement")
	assert.Contains(t, failBase.removeCalls, failRemote)

	currentObject, err := chunkFs.NewObject(ctx, filename)
	require.NoError(t, err)
	assert.Equal(t, replacementContents, fstests.ReadObject(ctx, t, currentObject, -1))

	currentChunkerObject, ok := currentObject.(*Object)
	require.True(t, ok)
	require.NotEmpty(t, currentChunkerObject.xactID, "replacement object must use norename transaction chunks")
	currentChunkRemotes := make([]string, 0, len(currentChunkerObject.chunks))
	for _, chunk := range currentChunkerObject.chunks {
		currentChunkRemotes = append(currentChunkRemotes, chunk.Remote())
	}
	assert.NotContains(t, currentChunkRemotes, failRemote, "stale old chunk must not be active in the replacement object")

	entries, err := chunkFs.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, entries, 1, "normal listing must not expose the stale old chunk as an active object")
	assert.Equal(t, filename, entries[0].Remote())

	staleChunk, err := failBase.NewObject(ctx, failRemote)
	require.NoError(t, err, "failed old chunk remove leaves a stale orphan in the base remote")
	assert.Equal(t, oldContents[:chunkSize], fstests.ReadObject(ctx, t, staleChunk, -1))
	for _, remote := range oldChunkRemotes[1:] {
		_, err := failBase.NewObject(ctx, remote)
		assert.ErrorIs(t, err, fs.ErrorObjectNotFound, "old chunk removed successfully when Remove did not fail")
	}
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
				// This only needs to be larger than the test contents to keep a single data chunk.
				"chunk_size":   "1Ki",
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
	t.Run("RemoveOldChunksFailureCurrentBehavior", func(t *testing.T) {
		testRemoveOldChunksFailureCurrentBehavior(t, f)
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
