package semanticformat

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestStateVersionAndLifecycleLimitsRoundTrip(t *testing.T) {
	value := testServiceSnapshot(t)
	limits := testStateLimits()
	data, _, err := EncodeState(value, limits)
	if err != nil {
		t.Fatal(err)
	}
	if version := binary.LittleEndian.Uint16(data[4:6]); version != 9 {
		t.Fatalf("state version = %d, want 9", version)
	}
	decoded, err := DecodeState(data, limits)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Config.Limits.MaxStaleVectors != 7 || decoded.Config.Limits.MaxSegments != 3 {
		t.Fatalf("decoded lifecycle limits = %+v", decoded.Config.Limits)
	}

	binary.LittleEndian.PutUint16(data[4:6], 8)
	if _, err := DecodeState(data, limits); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("DecodeState old version error = %v, want ErrUnsupportedVersion", err)
	}
}

func TestStateLifecycleLimitsValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ServiceSnapshot)
	}{
		{
			name: "physical vectors",
			mutate: func(value *ServiceSnapshot) {
				value.Config.Limits.MaxLiveVectors = 100
				value.Config.Limits.MaxStaleVectors = 100
			},
		},
		{
			name: "segments",
			mutate: func(value *ServiceSnapshot) {
				value.Config.Limits.MaxSegments = 17
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := testServiceSnapshot(t)
			tt.mutate(&value)
			if _, _, err := EncodeState(value, testStateLimits()); !errors.Is(err, ErrLimitExceeded) {
				t.Fatalf("EncodeState error = %v, want ErrLimitExceeded", err)
			}
		})
	}
}

func testServiceSnapshot(t *testing.T) ServiceSnapshot {
	t.Helper()
	embedding, err := semantic.NewEmbeddingDescriptor("test", "model", "v1", "pipeline-v1", 2, vector.MetricL2Squared, 1)
	if err != nil {
		t.Fatal(err)
	}
	config := semantic.Config{
		Schema: semantic.Schema{
			Embedding: embedding,
			Chunking:  semantic.ChunkingDescriptor{ID: "chunks", Version: 1, Fingerprint: "chunks-v1"},
		},
		Limits: semantic.Limits{
			MaxLiveVectors:          8,
			MaxStaleVectors:         7,
			MaxSegments:             3,
			MaxChunksPerDocument:    2,
			MaxDocumentsPerSearch:   2,
			MaxChunkCandidates:      8,
			MaxChunksPerDocumentHit: 1,
		},
	}
	index, err := semantic.NewIndex(config)
	if err != nil {
		t.Fatal(err)
	}
	state, err := index.State(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return ServiceSnapshot{
		Config:               state.Config,
		Revision:             state.Revision,
		MaxAllocatedVectorID: state.MaxAllocatedVectorID,
		NextSegmentID:        state.NextSegmentID,
	}
}

func testStateLimits() StateLimits {
	return StateLimits{
		FileLimits:           FileLimits{MaxFileBytes: 1 << 20, MaxStringBytes: 1024},
		MaxVectorBytes:       1 << 20,
		MaxDimensions:        128,
		MaxVectors:           128,
		MaxSegments:          16,
		MaxDocuments:         128,
		MaxChunksPerDocument: 16,
		MaxK:                 128,
		MaxEfSearch:          128,
		MaxVisitLimit:        128,
	}
}
