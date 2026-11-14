package format

import (
	"crypto/sha256"
	"testing"
)

func TestFileRefValid(t *testing.T) {
	t.Parallel()

	valid := FileRef{Size: 1, SHA256: sha256.Sum256([]byte("file"))}
	if !valid.Valid() {
		t.Fatal("FileRef.Valid() = false, want true")
	}
	if (FileRef{SHA256: valid.SHA256}).Valid() {
		t.Fatal("FileRef.Valid() with zero size = true, want false")
	}
	if (FileRef{Size: 1}).Valid() {
		t.Fatal("FileRef.Valid() with zero digest = true, want false")
	}
}
