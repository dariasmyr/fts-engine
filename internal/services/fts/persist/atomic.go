package persist

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
)

const (
	defaultBufferSize     = 1 << 20 // 1 MiB
	defaultFlushThreshold = 256 << 10
)

type SaveOptions struct {
	BufferSize     int
	FlushThreshold int
	SyncFile       bool
}

func DefaultSaveOptions() SaveOptions {
	return SaveOptions{
		BufferSize:     defaultBufferSize,
		FlushThreshold: defaultFlushThreshold,
		SyncFile:       true,
	}
}

func SaveAtomic(path string, write func(w io.Writer) error) error {
	return SaveAtomicWithOptions(path, DefaultSaveOptions(), write)
}

func SaveAtomicWithOptions(path string, opts SaveOptions, write func(w io.Writer) error) error {
	if path == "" {
		return fmt.Errorf("persist: save atomic: empty path")
	}
	if write == nil {
		return fmt.Errorf("persist: save atomic: nil writer callback")
	}

	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultBufferSize
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = defaultFlushThreshold
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("persist: save atomic: mkdir %q: %w", dir, err)
	}

	tmpFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("persist: save atomic: create temp file: %w", err)
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
		return fmt.Errorf("persist: save atomic: write temp file: %w", err)
	}

	if err = bw.Flush(); err != nil {
		_ = tmpFile.Close()
		cleanupTmp()
		return fmt.Errorf("persist: save atomic: flush temp file: %w", err)
	}

	if opts.SyncFile {
		if err = tmpFile.Sync(); err != nil {
			_ = tmpFile.Close()
			cleanupTmp()
			return fmt.Errorf("persist: save atomic: sync temp file: %w", err)
		}
	}

	if err = tmpFile.Close(); err != nil {
		cleanupTmp()
		return fmt.Errorf("persist: save atomic: close temp file: %w", err)
	}

	if err = os.Rename(tmpPath, path); err != nil {
		cleanupTmp()
		return fmt.Errorf("persist: save atomic: rename temp file: %w", err)
	}

	return nil
}

func SaveFTSSnapshotAtomic(path string, svc *pkgfts.Service, indexName string, filterName string) error {
	return SaveFTSSnapshotAtomicWithOptions(path, svc, indexName, filterName, DefaultSaveOptions())
}

func SaveFTSSnapshotAtomicWithOptions(path string, svc *pkgfts.Service, indexName string, filterName string, opts SaveOptions) error {
	if svc == nil {
		return fmt.Errorf("persist: save fts snapshot atomic: nil service")
	}

	return SaveAtomicWithOptions(path, opts, func(w io.Writer) error {
		return svc.SaveSnapshot(w, indexName, filterName)
	})
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
