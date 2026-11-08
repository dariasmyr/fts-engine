package generationformat

import (
	"crypto/sha256"
	"reflect"
	"testing"
)

func FuzzDecodeHead(f *testing.F) {
	limits := FileLimits{MaxFileBytes: 1 << 20}
	hash := sha256.Sum256([]byte("manifest"))
	seed, err := EncodeHead(Head{GenerationID: 1, ManifestHash: hash}, limits)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("SCUR"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodeHead(data, limits)
		if err != nil {
			return
		}
		encoded, err := EncodeHead(decoded, limits)
		if err != nil {
			t.Fatalf("EncodeHead(decoded): %v", err)
		}
		roundTrip, err := DecodeHead(encoded, limits)
		if err != nil || roundTrip != decoded {
			t.Fatalf("head round trip = %+v, %v; want %+v", roundTrip, err, decoded)
		}
	})
}

func FuzzDecodeManifest(f *testing.F) {
	limits := ManifestLimits{
		FileLimits:  FileLimits{MaxFileBytes: 1 << 20, MaxStringBytes: 1024},
		MaxSegments: 16,
	}
	vectorHash := sha256.Sum256([]byte("vectors"))
	graphHash := sha256.Sum256([]byte("graph"))
	stateHash := sha256.Sum256([]byte("state"))
	seed, _, err := EncodeManifest(GenerationManifest{
		GenerationID: 1,
		Segments: []SegmentRef{{
			ObjectID: "segment", Vectors: FileRef{Size: 10, SHA256: vectorHash},
			Graph: FileRef{Size: 20, SHA256: graphHash},
		}},
		State: FileRef{Size: 30, SHA256: stateHash},
	}, limits)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("SMAN"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodeManifest(data, limits)
		if err != nil {
			return
		}
		encoded, _, err := EncodeManifest(decoded, limits)
		if err != nil {
			t.Fatalf("EncodeManifest(decoded): %v", err)
		}
		roundTrip, err := DecodeManifest(encoded, limits)
		if err != nil || !reflect.DeepEqual(roundTrip, decoded) {
			t.Fatalf("manifest round trip = %+v, %v; want %+v", roundTrip, err, decoded)
		}
	})
}

func FuzzDecodeState(f *testing.F) {
	limits := testStateLimits()
	seed, _, err := EncodeState(testIndexState(f), limits)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("SSTA"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodeState(data, limits)
		if err != nil {
			return
		}
		encoded, _, err := EncodeState(decoded, limits)
		if err != nil {
			t.Fatalf("EncodeState(decoded): %v", err)
		}
		roundTrip, err := DecodeState(encoded, limits)
		if err != nil || !reflect.DeepEqual(roundTrip, decoded) {
			t.Fatalf("state round trip failed: %v", err)
		}
	})
}
