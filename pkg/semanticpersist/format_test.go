package semanticpersist

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	vectorflat "github.com/dariasmyr/fts-engine/pkg/vector/flat"
)

func TestSemanticFormatsRejectTruncationAndTrailingData(t *testing.T) {
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
	stateRef := fileRef(state)
	graphRef := fileRef([]byte("graph"))
	manifestData, _, err := encodeManifest(manifest{
		GenerationID: 1, ObjectID: segmentObjectID(semantic.SegmentKindChunkHNSW, vectorsMeta, graphRef),
		SegmentKind: semantic.SegmentKindChunkHNSW, Vectors: vectorsMeta, Graph: graphRef, State: stateRef,
	}, limits)
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

func TestStateRoundTripRows(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, true)
	limits := DefaultLimits()
	encoded, _, err := encodeState(checkpoint, limits)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeState(encoded, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(decoded.Rows, checkpoint.Segment.Rows()) {
		t.Fatalf("decoded v2 state = %+v", decoded)
	}
}

func TestSemanticFormatsRejectPreviousVersions(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, false)
	state, _, err := encodeState(checkpoint, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, vectors, err := vectorBytes(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	stateRef := fileRef(state)
	graphRef := fileRef([]byte("graph"))
	manifestData, _, err := encodeManifest(manifest{
		GenerationID: 1, ObjectID: segmentObjectID(semantic.SegmentKindChunkHNSW, vectors, graphRef),
		SegmentKind: semantic.SegmentKindChunkHNSW, Vectors: vectors, Graph: graphRef, State: stateRef,
	}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		data    []byte
		version uint16
		decode  func([]byte) error
	}{
		{name: "state v1", data: state, version: 1, decode: func(data []byte) error { _, err := decodeState(data, DefaultLimits()); return err }},
		{name: "manifest v1", data: manifestData, version: 1, decode: func(data []byte) error { _, err := decodeManifest(data, DefaultLimits()); return err }},
		{name: "manifest v2", data: manifestData, version: 2, decode: func(data []byte) error { _, err := decodeManifest(data, DefaultLimits()); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := append([]byte(nil), test.data...)
			binary.LittleEndian.PutUint16(data[4:6], test.version)
			if err := test.decode(data); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("previous-version error = %v", err)
			}
		})
	}
}

func TestSemanticFormatGoldenHashes(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, false)
	state, _, err := encodeState(checkpoint, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stateHash := sha256.Sum256(state)
	if got, want := hex.EncodeToString(stateHash[:]), "75488c20ad39bd4eed1cc7bece00073fab81ec0f8a855946ed931543b37fa972"; got != want {
		t.Fatalf("SSTA v3 SHA-256 = %s, want %s", got, want)
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

func FuzzDecodeManifestAndCurrent(f *testing.F) {
	limits := DefaultLimits()
	manifestData, manifestRef, _ := encodeManifest(manifest{
		GenerationID: 1,
		ObjectID:     "seg-" + string(bytes.Repeat([]byte{'a'}, 64)), SegmentKind: semantic.SegmentKindChunkHNSW,
		Vectors: fileReference{Size: 44, SHA256: sha256.Sum256([]byte("vectors"))},
		Graph:   fileReference{Size: 32, SHA256: sha256.Sum256([]byte("graph"))},
		State:   fileReference{Size: 64, SHA256: sha256.Sum256([]byte("state"))},
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

func vectorBytes(snapshot semantic.Snapshot) ([]byte, fileReference, error) {
	data, metadata, err := vectorflat.MarshalSource(snapshot.Segment.Vectors(), snapshot.Segment.MaxK())
	return data, fileReference{Size: metadata.Size, SHA256: metadata.SHA256}, err
}

func fileRef(data []byte) fileReference {
	return fileReference{Size: uint64(len(data)), SHA256: sha256.Sum256(data)}
}
