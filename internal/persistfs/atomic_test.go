package persistfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteTempFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path, err := WriteTempFile(dir, ".tmp-*", TempFileOptions{
		BufferSize: 4, FlushThreshold: 2,
	}, func(writer io.Writer) error {
		_, err := writer.Write([]byte("payload"))
		return err
	})
	if err != nil {
		t.Fatalf("WriteTempFile() error = %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "payload" {
		t.Fatalf("temporary data = %q, want payload", data)
	}
}

func TestWriteTempFileRemovesFailedWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	wantErr := errors.New("write failed")
	if _, err := WriteTempFile(dir, ".tmp-*", TempFileOptions{}, func(io.Writer) error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("WriteTempFile() error = %v, want %v", err, wantErr)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed write left %d temporary entries", len(entries))
	}
}

func TestReadRegularFileLimit(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(path, []byte("payload"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := ReadRegularFile(path, 3); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("ReadRegularFile() error = %v, want %v", err, ErrLimitExceeded)
	}
}
