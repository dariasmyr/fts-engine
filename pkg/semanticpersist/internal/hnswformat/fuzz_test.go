package hnswformat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"
	"testing"
)

func FuzzDecodeVectorFile(f *testing.F) {
	seed, limits := testVectorFile(f, 4, 2)
	f.Add(seed)
	f.Add([]byte("SVEC"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, _, err := DecodeVectorFile(context.Background(), data, limits)
		if err != nil {
			return
		}
		dimensions := decoded.Space.Dimensions()
		if dimensions <= 0 || dimensions > limits.MaxDimensions || len(decoded.Values)%dimensions != 0 {
			t.Fatalf("decoded vector shape: dimensions=%d components=%d", dimensions, len(decoded.Values))
		}
		if len(decoded.Values)/dimensions > limits.MaxVectors {
			t.Fatalf("decoded vector count exceeds limit")
		}
	})
}

func FuzzDecodeGraph(f *testing.F) {
	limits := graphLimits{
		MaxGraphBytes: 1 << 20,
		MaxVectors:    128,
		MaxLinks:      4096,
		MaxDimensions: 128,
		MaxLevel:      63,
	}
	vectorHash := sha256.Sum256([]byte("vectors"))
	seedGraph := graph{
		Dimensions: 2,
		Build: graphBuildInfo{
			BuildVersion: 1, LevelGeneratorVersion: 1,
			MaxNeighbors: 2, LevelZeroMaxNeighbors: 4, EfConstruction: 2,
		},
		Vectors:          FileRef{Size: 1, SHA256: vectorHash},
		Level0Offsets:    []uint32{0},
		UpperNodeOffsets: []uint32{0},
		UpperLinkOffsets: []uint32{0},
	}
	var output bytes.Buffer
	if _, err := encodeGraph(&output, seedGraph); err != nil {
		f.Fatal(err)
	}
	f.Add(output.Bytes())
	f.Add([]byte(graphMagic))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, _, err := decodeGraph(context.Background(), data, limits)
		if err != nil {
			return
		}
		var encoded bytes.Buffer
		if _, err := encodeGraph(&encoded, decoded); err != nil {
			t.Fatalf("encodeGraph(decoded graph): %v", err)
		}
		roundTrip, _, err := decodeGraph(context.Background(), encoded.Bytes(), limits)
		if err != nil || !reflect.DeepEqual(roundTrip, decoded) {
			t.Fatalf("graph round trip failed: %v", err)
		}
	})
}
