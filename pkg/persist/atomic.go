package persist

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// WriteAtomic writes one file through a temporary sibling and renames it into
// place only after the callback and optional file sync succeed.
func WriteAtomic(path string, syncFile bool, write func(io.Writer) error) error {
	if path == "" {
		return fmt.Errorf("persist: empty path")
	}
	if write == nil {
		return fmt.Errorf("persist: nil writer callback")
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

	if err := write(tmpFile); err != nil {
		_ = tmpFile.Close()
		cleanup()
		return err
	}
	if syncFile {
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
