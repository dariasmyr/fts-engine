package generationformat

import (
	"crypto/sha256"
	"errors"
	"testing"
)

func TestHeadRoundTrip(t *testing.T) {
	t.Parallel()

	limits := FileLimits{MaxFileBytes: 1024}
	want := Head{GenerationID: 7, ManifestHash: sha256.Sum256([]byte("manifest"))}
	data, err := EncodeHead(want, limits)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeHead(data, limits)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decoded head = %+v, want %+v", got, want)
	}
}

func TestHeadRejectsZeroGeneration(t *testing.T) {
	t.Parallel()

	if _, err := EncodeHead(Head{}, FileLimits{MaxFileBytes: 1024}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("EncodeHead() error = %v, want ErrCorrupt", err)
	}
}
