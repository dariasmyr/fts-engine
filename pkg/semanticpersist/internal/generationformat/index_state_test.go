package generationformat

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
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
	if version := binary.LittleEndian.Uint16(data[4:6]); version != 18 {
		t.Fatalf("state version = %d, want 18", version)
	}
	decoded, err := DecodeState(data, limits)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Config.Operational.Capacity.MaxStaleVectors != 7 || decoded.Config.Operational.Capacity.MaxSegments != 3 {
		t.Fatalf("decoded lifecycle limits = %+v", decoded.Config.Operational.Capacity)
	}
	if decoded.ID != value.ID {
		t.Fatalf("decoded index ID = %x, want %x", decoded.ID, value.ID)
	}

	binary.LittleEndian.PutUint16(data[4:6], 17)
	if _, err := DecodeState(data, limits); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("DecodeState old version error = %v, want ErrUnsupportedVersion", err)
	}
}

func TestStateIndexConfigRoundTrip(t *testing.T) {
	value := testIndexState(t)

	data, _, err := EncodeState(value, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeState(data, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Config != value.Config {
		t.Fatalf("semantic config = %+v, want %+v", decoded.Config, value.Config)
	}
}

func TestStateConfigRoundTrip(t *testing.T) {
	value := testIndexState(t)
	value.Pipeline = &semantic.PipelineConfig{
		Model: embedding.Model{
			ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 16,
			DocumentPrefix: "document: ", QueryPrefix: "query: ",
		},
		Chunking: chunk.Config{TargetBytes: 8, MaxBytes: 16, OverlapBytes: 2},
		Operational: semantic.PipelineOperationalLimits{Encoding: semantic.EncodingLimits{
			MaxFields: 5, MaxSourceBytes: 2048, MaxEmbeddingBytes: 24,
		}},
		EncodingBehaviorVersion: semantic.CurrentEncodingBehaviorVersion,
	}

	data, _, err := EncodeState(value, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeState(data, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Pipeline == nil || *decoded.Pipeline != *value.Pipeline {
		t.Fatalf("pipeline config = %+v, want %+v", decoded.Pipeline, value.Pipeline)
	}
}

func TestStateRejectsUnsupportedEncodingBehavior(t *testing.T) {
	t.Parallel()

	for _, version := range []uint32{0, semantic.CurrentEncodingBehaviorVersion + 1} {
		value := testIndexState(t)
		value.Pipeline = &semantic.PipelineConfig{
			Model: embedding.Model{
				ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 16,
			},
			Chunking:                chunk.Config{TargetBytes: 8, MaxBytes: 16, OverlapBytes: 2},
			Operational:             semantic.PipelineOperationalLimits{Encoding: semantic.EncodingLimits{MaxFields: 5, MaxSourceBytes: 2048, MaxEmbeddingBytes: 24}},
			EncodingBehaviorVersion: version,
		}

		if _, _, err := EncodeState(value, testStateLimits()); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("EncodeState(version %d) error = %v, want ErrUnsupportedVersion", version, err)
		}
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
				value.Config.Operational.Capacity.MaxLiveVectors = 100
				value.Config.Operational.Capacity.MaxStaleVectors = 100
			},
		},
		{
			name: "segments",
			mutate: func(value *IndexState) {
				value.Config.Operational.Capacity.MaxSegments = 17
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
	config := semantic.IndexConfig{
		Dimensions: 2,
		Metric:     vector.MetricL2Squared,
		Operational: semantic.OperationalLimits{
			Capacity:  semantic.CapacityLimits{MaxLiveVectors: 8, MaxStaleVectors: 7, MaxSegments: 3},
			Ingestion: semantic.IngestionLimits{MaxChunksPerDocument: 2},
			Search:    semantic.SearchLimits{MaxResults: 2, MaxCandidateChunks: 8, MaxChunksPerResult: 1, MaxQueryChunks: 3},
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

func testStateLimits() Config {
	return Config{
		MaxFileBytes:         1 << 20,
		MaxStringBytes:       1024,
		MaxVectorBytes:       1 << 20,
		MaxDimensions:        128,
		MaxVectors:           128,
		MaxSegments:          16,
		MaxDocuments:         128,
		MaxChunksPerDocument: 16,
		MaxQueryChunks:       16,
		MaxK:                 128,
		MaxEfSearch:          128,
		MaxVisitLimit:        128,
	}
}
