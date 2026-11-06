package ftspersist

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
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

	tmpPath, err := persistfs.WriteTempFile(
		dir,
		filepath.Base(path)+".tmp-*",
		persistfs.TempFileOptions{
			Sync:           opts.SyncFile,
			BufferSize:     opts.BufferSize,
			FlushThreshold: opts.FlushThreshold,
		},
		write,
	)
	if err != nil {
		return fmt.Errorf("ftspersist: save atomic: %w", err)
	}
	defer os.Remove(tmpPath)

	if err = persistfs.Replace(tmpPath, path); err != nil {
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
	return persistfs.SyncDirectory(path)
}

func readFileLimited(path string, configuredMax, defaultMax int64) ([]byte, error) {
	maxBytes := configuredMax
	if maxBytes <= 0 {
		maxBytes = defaultMax
	}
	payload, err := persistfs.ReadFileLimited(path, maxBytes)
	if errors.Is(err, persistfs.ErrLimitExceeded) {
		return nil, fmt.Errorf("file exceeds limit %d: %w", maxBytes, err)
	}
	return payload, err
}
