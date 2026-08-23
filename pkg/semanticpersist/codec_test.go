package semanticpersist

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	vectorflat "github.com/dariasmyr/fts-engine/pkg/vector/flat"
)

func TestSemanticCodecsRejectTruncationAndTrailingData(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, false)
	limits := DefaultLimits()
	state, _, err := encodeState(checkpoint, limits)
	if err != nil {
		t.Fatal(err)
	}
	vectorsData, vectorsMeta, err := vectorBytes(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	_ = vectorsData
	meta, metaRef, err := encodeSegmentMeta(checkpoint, vectorsMeta, limits)
	if err != nil {
		t.Fatal(err)
	}
	manifestData, _, err := encodeManifest(manifest{GenerationID: 1, ObjectID: objectID(vectorsMeta.SHA256, metaRef.SHA256), Vectors: vectorsMeta, SegmentMeta: metaRef, State: fileRef(state)}, limits)
	if err != nil {
		t.Fatal(err)
	}
	currentData, _, err := encodeCurrent(currentRecord{GenerationID: 1, ManifestHash: fileRef(manifestData).SHA256}, limits)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		data   []byte
		decode func([]byte) error
	}{
		{name: "state", data: state, decode: func(data []byte) error { _, err := decodeState(data, limits); return err }},
		{name: "meta", data: meta, decode: func(data []byte) error { _, err := decodeSegmentMeta(data, limits); return err }},
		{name: "manifest", data: manifestData, decode: func(data []byte) error { _, err := decodeManifest(data, limits); return err }},
		{name: "current", data: currentData, decode: func(data []byte) error { _, err := decodeCurrent(data, limits); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.decode(test.data[:len(test.data)-1]); err == nil {
				t.Fatal("truncation accepted")
			}
			if err := test.decode(append(append([]byte(nil), test.data...), 0)); err == nil {
				t.Fatal("trailing data accepted")
			}
			corrupt := append([]byte(nil), test.data...)
			corrupt[len(corrupt)/2] ^= 0xff
			if err := test.decode(corrupt); err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
}

func TestStateEncodingCanonicalizesRecordOrder(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, true)
	limits := DefaultLimits()
	canonical, _, err := encodeState(checkpoint, limits)
	if err != nil {
		t.Fatal(err)
	}
	permuted := checkpoint
	permuted.Documents = append([]semantic.DocumentRecord(nil), checkpoint.Documents...)
	permuted.Refs = append([]semantic.RefRecord(nil), checkpoint.Refs...)
	slices.Reverse(permuted.Documents)
	slices.Reverse(permuted.Refs)
	for i := range permuted.Documents {
		slices.Reverse(permuted.Documents[i].VectorIDs)
	}
	encoded, _, err := encodeState(permuted, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, encoded) {
		t.Fatal("logically equivalent mappings produced different state bytes")
	}
}

func FuzzDecodeState(f *testing.F) {
	checkpoint, _, _ := persistenceFixture(f, false)
	data, _, _ := encodeState(checkpoint, DefaultLimits())
	f.Add(data)
	f.Fuzz(func(t *testing.T, data []byte) {
		limits := DefaultLimits()
		limits.MaxFileBytes = 64 << 10
		limits.MaxVectors = 100
		limits.MaxDocuments = 100
		_, _ = decodeState(data, limits)
	})
}

func FuzzDecodeSegmentMeta(f *testing.F) {
	checkpoint, _, _ := persistenceFixture(f, false)
	_, vectors, _ := vectorBytes(checkpoint)
	data, _, _ := encodeSegmentMeta(checkpoint, vectors, DefaultLimits())
	f.Add(data)
	f.Fuzz(func(t *testing.T, data []byte) {
		limits := DefaultLimits()
		limits.MaxFileBytes = 64 << 10
		limits.MaxVectors = 100
		_, _ = decodeSegmentMeta(data, limits)
	})
}

func FuzzDecodeManifestAndCurrent(f *testing.F) {
	limits := DefaultLimits()
	manifestData, manifestRef, _ := encodeManifest(manifest{
		GenerationID: 1,
		ObjectID:     "seg-" + string(bytes.Repeat([]byte{'a'}, 64)),
		Vectors:      fileReference{Size: 44},
		SegmentMeta:  fileReference{Size: 96},
		State:        fileReference{Size: 64},
	}, limits)
	currentData, _, _ := encodeCurrent(currentRecord{GenerationID: 1, ManifestHash: manifestRef.SHA256}, limits)
	f.Add(uint8(0), manifestData)
	f.Add(uint8(1), currentData)
	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		limits := DefaultLimits()
		limits.MaxFileBytes = 64 << 10
		if kind&1 == 0 {
			_, _ = decodeManifest(data, limits)
			return
		}
		_, _ = decodeCurrent(data, limits)
	})
}

func vectorBytes(checkpoint semantic.Checkpoint) ([]byte, fileReference, error) {
	data, metadata, err := vectorflat.Marshal(checkpoint.Segment)
	return data, fileReference{Size: metadata.Size, SHA256: metadata.SHA256}, err
}

func fileRef(data []byte) fileReference {
	return fileReference{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}
}
