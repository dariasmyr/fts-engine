package semanticpersist

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
)

func TestSemanticFormatsRoundTripAndRejectMalformedData(t *testing.T) {
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	addAndFlush(t, service, encoder, "doc-b")
	snapshot, err := service.CommittedSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	stateData, stateRef, err := encodeState(snapshot, limits)
	if err != nil {
		t.Fatal(err)
	}
	state, err := decodeState(stateData, limits)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Segments) != 2 || state.Config != snapshot.Config() || state.Revision != snapshot.Revision() {
		t.Fatalf("decoded state = %+v", state)
	}
	ref := fileReference{Size: 32, SHA256: sha256.Sum256([]byte("object"))}
	manifestData, manifestRef, err := encodeManifest(manifest{GenerationID: 1, State: stateRef, Segments: []manifestSegment{
		{ObjectID: segmentObjectID(ref, ref), Vectors: ref, Graph: ref},
	}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := decodeManifest(manifestData, limits); err != nil || len(decoded.Segments) != 1 {
		t.Fatalf("decode manifest = %+v, %v", decoded, err)
	}
	if _, _, err := encodeManifest(manifest{GenerationID: 1, State: stateRef, Segments: []manifestSegment{
		{ObjectID: segmentObjectID(ref, fileReference{}), Vectors: ref},
	}}, limits); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("manifest without graph error = %v", err)
	}
	currentData, _, err := encodeCurrent(currentRecord{GenerationID: 1, ManifestHash: manifestRef.SHA256}, limits)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		data   []byte
		decode func([]byte) error
	}{
		{name: "state", data: stateData, decode: func(data []byte) error { _, err := decodeState(data, limits); return err }},
		{name: "manifest", data: manifestData, decode: func(data []byte) error { _, err := decodeManifest(data, limits); return err }},
		{name: "current", data: currentData, decode: func(data []byte) error { _, err := decodeCurrent(data, limits); return err }},
	} {
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

func TestSemanticFormatsRejectPreviousVersions(t *testing.T) {
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	snapshot, err := service.CommittedSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stateData, stateRef, err := encodeState(snapshot, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	manifestData, _, err := encodeManifest(manifest{GenerationID: 1, State: stateRef}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		data    []byte
		version uint16
		decode  func([]byte) error
	}{
		{name: "state v7", data: stateData, version: 7, decode: func(data []byte) error { _, err := decodeState(data, DefaultLimits()); return err }},
		{name: "manifest v5", data: manifestData, version: 5, decode: func(data []byte) error { _, err := decodeManifest(data, DefaultLimits()); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := append([]byte(nil), test.data...)
			binary.LittleEndian.PutUint16(data[4:6], test.version)
			if err := test.decode(data); !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("decode error = %v", err)
			}
		})
	}
}

func FuzzDecodeState(f *testing.F) {
	service, encoder := persistenceService(f)
	addAndFlush(f, service, encoder, "doc-a")
	snapshot, _ := service.CommittedSnapshot(context.Background())
	data, _, _ := encodeState(snapshot, DefaultLimits())
	f.Add(data)
	f.Fuzz(func(t *testing.T, data []byte) {
		limits := DefaultLimits()
		limits.MaxFileBytes = 64 << 10
		limits.MaxVectors = 100
		_, _ = decodeState(data, limits)
	})
}

func FuzzDecodeManifestAndCurrent(f *testing.F) {
	limits := DefaultLimits()
	ref := fileReference{Size: 32, SHA256: sha256.Sum256([]byte("object"))}
	manifestData, manifestRef, _ := encodeManifest(manifest{GenerationID: 1, State: ref}, limits)
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
