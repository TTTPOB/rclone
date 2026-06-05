package readers

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpillableReplayReaderMemory(t *testing.T) {
	in := bytes.NewBufferString("Testbuffer")
	r := NewSpillableReplayReader(in, 1024, t.TempDir())
	defer func() {
		require.NoError(t, r.Close())
	}()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("Testbuffer"), got)
	assert.Nil(t, r.file)

	pos, err := r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	got, err = io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("Testbuffer"), got)
	assert.Nil(t, r.file)
}

func TestSpillableReplayReaderSpillToDisk(t *testing.T) {
	tempDir := t.TempDir()
	in := bytes.NewBufferString("Testbuffer")
	r := NewSpillableReplayReader(in, 4, tempDir)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("Testbuffer"), got)
	require.NotNil(t, r.file)
	tempName := r.file.Name()
	assert.DirExists(t, tempDir)
	assert.FileExists(t, tempName)

	pos, err := r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	got, err = io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("Testbuffer"), got)

	require.NoError(t, r.Close())
	_, err = os.Stat(tempName)
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestSpillableReplayReaderPartialReadSeekReplay(t *testing.T) {
	in := bytes.NewBufferString("Testbuffer")
	r := NewSpillableReplayReader(in, 1024, t.TempDir())
	defer func() {
		require.NoError(t, r.Close())
	}()

	buf := make([]byte, 4)
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("Test"), buf)

	pos, err := r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	buf = make([]byte, 10)
	n, err = r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("Test"), buf[:n])

	n, err = r.Read(buf[4:])
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, []byte("Testbuffer"), buf)
}

func TestSpillableReplayReaderMultipleSeekReplay(t *testing.T) {
	in := bytes.NewBufferString("Testbuffer")
	r := NewSpillableReplayReader(in, 4, t.TempDir())
	defer func() {
		require.NoError(t, r.Close())
	}()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("Testbuffer"), got)
	require.NotNil(t, r.file)

	for _, offset := range []int64{4, 0, 6, 2} {
		pos, err := r.Seek(offset, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, offset, pos)

		buf := make([]byte, 2)
		n, err := r.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		assert.Equal(t, []byte("Testbuffer")[offset:offset+2], buf)
	}

	pos, err := r.Seek(-3, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(7), pos)

	got, err = io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, []byte("fer"), got)
}

func TestSpillableReplayReaderSeekPastCacheLeavesPosition(t *testing.T) {
	in := bytes.NewBufferString("Testbuffer")
	r := NewSpillableReplayReader(in, 1024, t.TempDir())
	defer func() {
		require.NoError(t, r.Close())
	}()

	buf := make([]byte, 4)
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("Test"), buf)

	pos, err := r.Seek(8, io.SeekStart)
	require.Error(t, err)
	assert.Equal(t, int64(4), pos)

	pos, err = r.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(4), pos)

	n, err = r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("buff"), buf)
}

func TestSpillableReplayReaderNonPositiveThresholdSpillsImmediately(t *testing.T) {
	for _, threshold := range []int64{0, -1} {
		in := bytes.NewBufferString("Testbuffer")
		r := NewSpillableReplayReader(in, threshold, t.TempDir())

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, []byte("Testbuffer"), got)
		require.NotNil(t, r.file)
		assert.Empty(t, r.mem)

		pos, err := r.Seek(0, io.SeekStart)
		require.NoError(t, err)
		assert.Equal(t, int64(0), pos)

		got, err = io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, []byte("Testbuffer"), got)
		require.NoError(t, r.Close())
	}
}

func TestSpillableReplayReaderReadError(t *testing.T) {
	errTest := errors.New("test read error")
	in := &errorAfterReader{
		data: []byte("Test"),
		err:  errTest,
	}
	r := NewSpillableReplayReader(in, 1024, t.TempDir())
	defer func() {
		require.NoError(t, r.Close())
	}()

	buf := make([]byte, 4)
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("Test"), buf)

	n, err = r.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, err, errTest)

	pos, err := r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	n, err = r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("Test"), buf)
}

func TestSpillableReplayReaderSpillCreateErrorIsSticky(t *testing.T) {
	r := NewSpillableReplayReader(bytes.NewBufferString("Testbuffer"), 0, t.TempDir()+"/missing")

	buf := make([]byte, 4)
	n, err := r.Read(buf)
	require.Error(t, err)
	assert.Equal(t, 0, n)

	n, nextErr := r.Read(buf)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, nextErr, err)

	pos, seekErr := r.Seek(0, io.SeekStart)
	assert.Equal(t, int64(0), pos)
	assert.ErrorIs(t, seekErr, err)

	require.NoError(t, r.Close())
}

func TestSpillableReplayReaderCloseDeletesTempFile(t *testing.T) {
	in := bytes.NewBufferString("Testbuffer")
	r := NewSpillableReplayReader(in, 1, t.TempDir())

	_, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NotNil(t, r.file)
	tempName := r.file.Name()
	assert.FileExists(t, tempName)

	require.NoError(t, r.Close())
	_, err = os.Stat(tempName)
	assert.True(t, errors.Is(err, os.ErrNotExist))
	require.NoError(t, r.Close())
}

func TestSpillableReplayReaderCloseJoinsErrors(t *testing.T) {
	in := bytes.NewBufferString("Testbuffer")
	r := NewSpillableReplayReader(in, 1, t.TempDir())

	_, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NotNil(t, r.file)
	tempName := r.file.Name()

	require.NoError(t, r.file.Close())
	require.NoError(t, os.Remove(tempName))

	err = r.Close()
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrClosed)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

type errorAfterReader struct {
	data []byte
	err  error
	done bool
}

func (r *errorAfterReader) Read(b []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	return copy(b, r.data), nil
}
