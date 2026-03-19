package fts

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	defaultSnapshotBufferSize     = 1 << 20 // 1 MiB
	defaultSnapshotFlushThreshold = 256 << 10
)

type SnapshotFileOptions struct {
	BufferSize     int
	FlushThreshold int
	SyncFile       bool
}

func DefaultSnapshotFileOptions() SnapshotFileOptions {
	return SnapshotFileOptions{
		BufferSize:     defaultSnapshotBufferSize,
		FlushThreshold: defaultSnapshotFlushThreshold,
		SyncFile:       true,
	}
}

func (s *Service) SaveSnapshotFile(path string, indexName string, filterName string) error {
	return s.SaveSnapshotFileWithOptions(path, indexName, filterName, DefaultSnapshotFileOptions())
}

func (s *Service) SaveSnapshotFileWithOptions(path string, indexName string, filterName string, opts SnapshotFileOptions) error {
	if s == nil {
		return fmt.Errorf("fts: save snapshot file: nil service")
	}
	if path == "" {
		return fmt.Errorf("fts: save snapshot file: empty path")
	}

	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultSnapshotBufferSize
	}
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = defaultSnapshotFlushThreshold
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("fts: save snapshot file: mkdir %q: %w", dir, err)
	}

	tmpFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("fts: save snapshot file: create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	cleanupTmp := func() {
		_ = os.Remove(tmpPath)
	}

	bw := newSnapshotThresholdWriter(tmpFile, opts.BufferSize, opts.FlushThreshold)
	if err = s.SaveSnapshot(bw, indexName, filterName); err != nil {
		_ = bw.Flush()
		_ = tmpFile.Close()
		cleanupTmp()
		return fmt.Errorf("fts: save snapshot file: write temp file: %w", err)
	}

	if err = bw.Flush(); err != nil {
		_ = tmpFile.Close()
		cleanupTmp()
		return fmt.Errorf("fts: save snapshot file: flush temp file: %w", err)
	}

	if opts.SyncFile {
		if err = tmpFile.Sync(); err != nil {
			_ = tmpFile.Close()
			cleanupTmp()
			return fmt.Errorf("fts: save snapshot file: sync temp file: %w", err)
		}
	}

	if err = tmpFile.Close(); err != nil {
		cleanupTmp()
		return fmt.Errorf("fts: save snapshot file: close temp file: %w", err)
	}

	if err = os.Rename(tmpPath, path); err != nil {
		cleanupTmp()
		return fmt.Errorf("fts: save snapshot file: rename temp file: %w", err)
	}

	return nil
}

func NewFromSnapshotFile(path string, keyGen KeyGenerator, opts ...Option) (*Service, error) {
	if path == "" {
		return nil, fmt.Errorf("fts: new from snapshot file: empty path")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fts: new from snapshot file: open: %w", err)
	}
	defer f.Close()

	return NewFromSnapshot(f, keyGen, opts...)
}

type snapshotThresholdWriter struct {
	bw             *bufio.Writer
	flushThreshold int
	pending        int
}

func newSnapshotThresholdWriter(w io.Writer, bufferSize int, flushThreshold int) *snapshotThresholdWriter {
	return &snapshotThresholdWriter{
		bw:             bufio.NewWriterSize(w, bufferSize),
		flushThreshold: flushThreshold,
	}
}

func (w *snapshotThresholdWriter) Write(p []byte) (int, error) {
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

func (w *snapshotThresholdWriter) Flush() error {
	w.pending = 0
	return w.bw.Flush()
}
