package ftspersist

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	defaultBufferSize     = 1 << 20 // 1 MiB
	defaultFlushThreshold = 256 << 10
)

func saveAtomicWithOptions(path string, opts SaveOptions, write func(w io.Writer) error) error {
	if path == "" {
		return fmt.Errorf("ftspersist: save atomic: empty path")
	}
	if write == nil {
		return fmt.Errorf("ftspersist: save atomic: nil writer callback")
	}

	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultBufferSize
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = defaultFlushThreshold
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("ftspersist: save atomic: mkdir %q: %w", dir, err)
	}

	tmpFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("ftspersist: save atomic: create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	cleanupTmp := func() {
		_ = os.Remove(tmpPath)
	}

	bw := newThresholdBufferedWriter(tmpFile, opts.BufferSize, opts.FlushThreshold)

	if err = write(bw); err != nil {
		_ = bw.Flush()
		_ = tmpFile.Close()
		cleanupTmp()
		return fmt.Errorf("ftspersist: save atomic: write temp file: %w", err)
	}

	if err = bw.Flush(); err != nil {
		_ = tmpFile.Close()
		cleanupTmp()
		return fmt.Errorf("ftspersist: save atomic: flush temp file: %w", err)
	}

	if opts.SyncFile {
		if err = tmpFile.Sync(); err != nil {
			_ = tmpFile.Close()
			cleanupTmp()
			return fmt.Errorf("ftspersist: save atomic: sync temp file: %w", err)
		}
	}

	if err = tmpFile.Close(); err != nil {
		cleanupTmp()
		return fmt.Errorf("ftspersist: save atomic: close temp file: %w", err)
	}

	if err = os.Rename(tmpPath, path); err != nil {
		cleanupTmp()
		return fmt.Errorf("ftspersist: save atomic: rename temp file: %w", err)
	}
	if opts.SyncFile {
		if err = syncDir(dir); err != nil {
			return fmt.Errorf("ftspersist: save atomic: sync directory %q: %w", dir, err)
		}
	}

	return nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func readFileLimited(path string, configuredMax, defaultMax int64) ([]byte, error) {
	maxBytes := configuredMax
	if maxBytes <= 0 {
		maxBytes = defaultMax
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > maxBytes {
		return nil, fmt.Errorf("file size %d exceeds limit %d", info.Size(), maxBytes)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > maxBytes {
		return nil, fmt.Errorf("file exceeds limit %d", maxBytes)
	}
	return payload, nil
}

type thresholdBufferedWriter struct {
	bw             *bufio.Writer
	flushThreshold int
	pending        int
}

func newThresholdBufferedWriter(w io.Writer, bufferSize int, flushThreshold int) *thresholdBufferedWriter {
	return &thresholdBufferedWriter{
		bw:             bufio.NewWriterSize(w, bufferSize),
		flushThreshold: flushThreshold,
	}
}

func (w *thresholdBufferedWriter) Write(p []byte) (int, error) {
	n, err := w.bw.Write(p)
	w.pending += n
	if err != nil {
		return n, err
	}

	if w.pending >= w.flushThreshold {
		if flushErr := w.bw.Flush(); flushErr != nil {
			return n, flushErr
		}
		w.pending = 0
	}

	return n, nil
}

func (w *thresholdBufferedWriter) Flush() error {
	w.pending = 0
	return w.bw.Flush()
}
