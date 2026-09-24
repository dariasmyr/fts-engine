package persist

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestReferenceHash(t *testing.T) {
	data := []byte("payload")
	if Hash(data) == [32]byte{} {
		t.Fatal("Hash returned zero identity")
	}
}

func TestReadBounded(t *testing.T) {
	if got, err := ReadBounded(bytes.NewReader([]byte("abc")), 3); err != nil || string(got) != "abc" {
		t.Fatalf("ReadBounded exact read = %q, %v", got, err)
	}
	if _, err := ReadBounded(bytes.NewReader([]byte("abcd")), 3); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("oversized read error = %v", err)
	}
}

func TestWriteAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "payload")
	if err := WriteAtomic(path, AtomicWriteOptions{}, func(writer io.Writer) error {
		_, err := io.WriteString(writer, "payload")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "payload" {
		t.Fatalf("atomic payload = %q, %v", data, err)
	}
}

func TestWriteAtomicKeepsOldFileOnWriteError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "payload")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	expected := errors.New("boom")
	if err := WriteAtomic(path, AtomicWriteOptions{}, func(writer io.Writer) error {
		_, _ = io.WriteString(writer, "new")
		return expected
	}); !errors.Is(err, expected) {
		t.Fatalf("WriteAtomic error = %v, want %v", err, expected)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "old" {
		t.Fatalf("old payload = %q, %v", data, err)
	}
}
