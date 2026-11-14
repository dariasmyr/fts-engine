package generationformat

import (
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
)

func TestManifestRoundTrip(t *testing.T) {
	t.Parallel()

	limits := Config{
		MaxFileBytes:   4096,
		MaxStringBytes: 128,
		MaxSegments:    2,
	}
	want := GenerationManifest{
		GenerationID: 3,
		Segments: []SegmentRef{{
			ObjectID: "seg-test",
			Vectors:  FileRef{Size: 10, SHA256: sha256.Sum256([]byte("vectors"))},
			Graph:    FileRef{Size: 20, SHA256: sha256.Sum256([]byte("graph"))},
		}},
		State: FileRef{Size: 30, SHA256: sha256.Sum256([]byte("state"))},
	}
	data, _, err := EncodeManifest(want, limits)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeManifest(data, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded manifest = %+v, want %+v", got, want)
	}
}

func TestManifestRejectsSegmentLimit(t *testing.T) {
	t.Parallel()

	ref := FileRef{Size: 1, SHA256: sha256.Sum256([]byte("file"))}
	value := GenerationManifest{
		GenerationID: 1,
		Segments:     []SegmentRef{{ObjectID: "one", Vectors: ref, Graph: ref}, {ObjectID: "two", Vectors: ref, Graph: ref}},
		State:        ref,
	}
	limits := Config{
		MaxFileBytes:   4096,
		MaxStringBytes: 128,
		MaxSegments:    1,
	}
	if _, _, err := EncodeManifest(value, limits); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("EncodeManifest() error = %v, want ErrLimitExceeded", err)
	}
}
