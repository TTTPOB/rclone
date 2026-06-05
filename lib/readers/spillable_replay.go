package readers

import (
	"errors"
	"io"
	"os"
	"sync"
)

// SpillableReplayReader implements io.ReadSeekCloser by caching data read from
// an underlying reader. It keeps the cache in memory until memoryThreshold is
// exceeded, then spills the complete cache to a temporary file in tempDir.
// A memoryThreshold of 0 or less spills immediately without keeping a memory
// cache.
type SpillableReplayReader struct {
	mu              sync.Mutex
	in              io.Reader
	i               int64
	cacheLen        int64
	memoryThreshold int64
	tempDir         string
	mem             []byte
	file            *os.File
	closed          bool
	err             error
}

var _ io.ReadSeekCloser = (*SpillableReplayReader)(nil)

// NewSpillableReplayReader creates a reader that can replay already-read data.
func NewSpillableReplayReader(r io.Reader, memoryThreshold int64, tempDir string) *SpillableReplayReader {
	return &SpillableReplayReader{
		in:              r,
		memoryThreshold: memoryThreshold,
		tempDir:         tempDir,
	}
}

// Seek implements the io.Seeker interface.
// It only allows seeking within data that has already been cached.
func (r *SpillableReplayReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, errors.New("readers.SpillableReplayReader.Seek: reader is closed")
	}
	if r.err != nil {
		return 0, r.err
	}

	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.i + offset
	case io.SeekEnd:
		abs = r.cacheLen + offset
	default:
		return 0, errors.New("readers.SpillableReplayReader.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("readers.SpillableReplayReader.Seek: negative position")
	}
	if abs > r.cacheLen {
		return offset - (abs - r.cacheLen), errors.New("readers.SpillableReplayReader.Seek: offset is unavailable")
	}
	r.i = abs
	return abs, nil
}

// Read data from the original reader into b, or replays cached data after Seek.
func (r *SpillableReplayReader) Read(b []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, errors.New("readers.SpillableReplayReader.Read: reader is closed")
	}
	if r.err != nil {
		return 0, r.err
	}

	if len(b) == 0 {
		return 0, nil
	}

	if r.i < r.cacheLen {
		n, err = r.readCacheAt(b, r.i)
		if err != nil {
			r.err = err
			return n, err
		}
		r.i += int64(n)
		return n, err
	}

	n, err = r.in.Read(b)
	if n > 0 {
		cacheErr := r.appendCache(b[:n])
		if cacheErr != nil {
			r.err = cacheErr
			return 0, cacheErr
		}
		r.i += int64(n)
	}
	return n, err
}

// Close removes any temporary file created for spilled data.
func (r *SpillableReplayReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	r.mem = nil

	if r.file == nil {
		return nil
	}

	name := r.file.Name()
	err := r.file.Close()
	removeErr := os.Remove(name)
	r.file = nil
	return errors.Join(err, removeErr)
}

func (r *SpillableReplayReader) appendCache(b []byte) error {
	if r.file == nil && r.memoryThreshold > 0 && r.cacheLen+int64(len(b)) <= r.memoryThreshold {
		r.mem = append(r.mem, b...)
		r.cacheLen += int64(len(b))
		return nil
	}

	if r.file == nil {
		if err := r.spill(); err != nil {
			return err
		}
	}

	n, err := r.file.WriteAt(b, r.cacheLen)
	if n != len(b) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		return errors.Join(err, r.file.Truncate(r.cacheLen))
	}
	r.cacheLen += int64(len(b))
	return nil
}

func (r *SpillableReplayReader) spill() error {
	file, err := os.CreateTemp(r.tempDir, "rclone-spillable-replay-*")
	if err != nil {
		return err
	}
	if len(r.mem) > 0 {
		n, err := file.WriteAt(r.mem, 0)
		if n != len(r.mem) && err == nil {
			err = io.ErrShortWrite
		}
		if err != nil {
			closeErr := file.Close()
			removeErr := os.Remove(file.Name())
			return errors.Join(err, closeErr, removeErr)
		}
	}
	r.file = file
	r.mem = nil
	return nil
}

func (r *SpillableReplayReader) readCacheAt(b []byte, off int64) (int, error) {
	if r.file != nil {
		limit := r.cacheLen - off
		if int64(len(b)) > limit {
			b = b[:limit]
		}
		return r.file.ReadAt(b, off)
	}
	return copy(b, r.mem[off:]), nil
}
