package generationformat

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/internal/format"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestStateVersionAndLifecycleLimitsRoundTrip(t *testing.T) {
	value := testIndexState(t)
	limits := testStateLimits()
	data, _, err := EncodeState(value, limits)
	if err != nil {
		t.Fatal(err)
	}
	if version := binary.LittleEndian.Uint16(data[4:6]); version != 11 {
		t.Fatalf("state version = %d, want 11", version)
	}
	decoded, err := DecodeState(data, limits)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Config.Limits.MaxStaleVectors != 7 || decoded.Config.Limits.MaxSegments != 3 {
		t.Fatalf("decoded lifecycle limits = %+v", decoded.Config.Limits)
	}
	if decoded.ID != value.ID {
		t.Fatalf("decoded index ID = %x, want %x", decoded.ID, value.ID)
	}

	binary.LittleEndian.PutUint16(data[4:6], 10)
	if _, err := DecodeState(data, limits); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("DecodeState old version error = %v, want ErrUnsupportedVersion", err)
	}
}

func TestStateRejectsUnknownCompatibilityVersion(t *testing.T) {
	limits := testStateLimits()
	data, _, err := EncodeState(testIndexState(t), limits)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(data[wireHeaderSize+stateIndexIDSize:], semantic.CompatibilityFormatVersion+1)
	body := data[:len(data)-wireChecksumSize]
	binary.LittleEndian.PutUint32(data[len(body):], format.CRC32(body))
	if _, err := DecodeState(data, limits); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("DecodeState() error = %v, want ErrUnsupportedVersion", err)
	}
}

func TestStateLifecycleLimitsValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*IndexState)
	}{
		{
			name: "physical vectors",
			mutate: func(value *IndexState) {
				value.Config.Limits.MaxLiveVectors = 100
				value.Config.Limits.MaxStaleVectors = 100
			},
		},
		{
			name: "segments",
			mutate: func(value *IndexState) {
				value.Config.Limits.MaxSegments = 17
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := testIndexState(t)
			tt.mutate(&value)
			if _, _, err := EncodeState(value, testStateLimits()); !errors.Is(err, ErrLimitExceeded) {
				t.Fatalf("EncodeState error = %v, want ErrLimitExceeded", err)
			}
		})
	}
}

func TestDecodeStateRejectsSegmentLimit(t *testing.T) {
	t.Parallel()

	value := testIndexState(t)
	value.Segments = []SegmentState{{ID: 1}, {ID: 2}}
	data, _, err := EncodeState(value, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	limits := testStateLimits()
	limits.MaxSegments = 1
	if _, err := DecodeState(data, limits); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("DecodeState() error = %v, want ErrLimitExceeded", err)
	}
}

func testIndexState(t testing.TB) IndexState {
	t.Helper()
	embedding, err := semantic.NewEmbeddingSchema("model", "v1", "pipeline-v1", 2, vector.MetricL2Squared, 1)
	if err != nil {
		t.Fatal(err)
	}
	config := semantic.IndexConfig{
		Schema: semantic.Schema{
			Embedding: embedding,
			Chunking:  semantic.ChunkingSchema{ID: "chunks", Version: 1, Fingerprint: "chunks-v1"},
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
	state, err := index.ExportState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return IndexState{
		ID:                   state.ID,
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
