package persist

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestSaveAtomicSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-1.fidx")

	err := SaveAtomic(path, func(w io.Writer) error {
		_, writeErr := w.Write([]byte("payload"))
		return writeErr
	})
	if err != nil {
		t.Fatalf("SaveAtomic() error = %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(b) != "payload" {
		t.Fatalf("saved payload = %q, want %q", string(b), "payload")
	}
}

func TestSaveAtomicKeepsOldFileOnWriteError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-1.fidx")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	expectedErr := errors.New("boom")
	err := SaveAtomic(path, func(w io.Writer) error {
		_, _ = w.Write([]byte("new"))
		return expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("SaveAtomic() error = %v, want %v", err, expectedErr)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !bytes.Equal(b, []byte("old")) {
		t.Fatalf("saved payload = %q, want %q", string(b), "old")
	}
}
