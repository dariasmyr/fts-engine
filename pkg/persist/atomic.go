package persist

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	DefaultBufferSize     = 1 << 20
	DefaultFlushThreshold = 256 << 10
)

type AtomicWriteOptions struct {
	BufferSize     int
	FlushThreshold int
	SyncFile       bool
}

// WriteAtomic writes one file through a temporary sibling and renames it into
// place only after the callback and optional file sync succeed.
func WriteAtomic(path string, options AtomicWriteOptions, write func(io.Writer) error) error {
	if path == "" {
		return fmt.Errorf("persist: empty path")
	}
	if write == nil {
		return fmt.Errorf("persist: nil writer callback")
	}
	if options.BufferSize <= 0 {
		options.BufferSize = DefaultBufferSize
	}
	if options.FlushThreshold <= 0 {
		options.FlushThreshold = DefaultFlushThreshold
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	cleanup := func() {
		_ = os.Remove(tmpPath)
	}

	bw := &thresholdBufferedWriter{
		writer:    bufio.NewWriterSize(tmpFile, options.BufferSize),
		threshold: options.FlushThreshold,
	}
	if err := write(bw); err != nil {
		_ = bw.Flush()
		_ = tmpFile.Close()
		cleanup()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = tmpFile.Close()
		cleanup()
		return err
	}
	if options.SyncFile {
		if err := tmpFile.Sync(); err != nil {
			_ = tmpFile.Close()
			cleanup()
			return err
		}
	}
	if err := tmpFile.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return err
	}
	return nil
}

type thresholdBufferedWriter struct {
	writer    *bufio.Writer
	threshold int
	pending   int
}

func (w *thresholdBufferedWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.pending += n
	if err != nil {
		return n, err
	}
	if w.pending >= w.threshold {
		if err := w.writer.Flush(); err != nil {
			return n, err
		}
		w.pending = 0
	}
	return n, nil
}

func (w *thresholdBufferedWriter) Flush() error {
	w.pending = 0
	return w.writer.Flush()
}
