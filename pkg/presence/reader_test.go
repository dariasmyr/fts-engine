package presence

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileReaderClose(t *testing.T) {
	var nilReader *FileReader
	if err := nilReader.Close(); err != nil {
		t.Fatalf("nil FileReader.Close() error = %v", err)
	}
	if err := new(FileReader).Close(); err != nil {
		t.Fatalf("zero FileReader.Close() error = %v", err)
	}

	blob, err := BuildFromSource(source{{Term: "a", Postings: postings(0, 1)}})
	if err != nil {
		t.Fatalf("BuildFromSource() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "index.fpr")
	if err := os.WriteFile(path, blob, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("cleanup Close() error = %v", err)
		}
	})
	file := r.file
	if err := r.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed file Stat() error = %v, want %v", err, os.ErrClosed)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("repeated Close() error = %v", err)
	}
}

func TestOpenFilePreservesError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.fpr")
	if _, err := OpenFile(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("OpenFile() error = %v, want %v", err, os.ErrNotExist)
	}
}
