package generationformat

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
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

func TestStateRoundTrip(t *testing.T) {
	value := testIndexState(t)
	data, _, err := EncodeState(value, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeState(data, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, value) {
		t.Fatalf("decoded state = %+v, want %+v", decoded, value)
	}
}

func TestStateWireLayoutIsReproducible(t *testing.T) {
	value := testIndexState(t)
	first, _, err := EncodeState(value, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := EncodeState(value, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("EncodeState produced different bytes for the same value")
	}
	const wantSize = 353
	const wantHash = "e5676cfb9b0073eefc39315f60f38a2efc3bea242e6e44c6064d26716c52b76f"
	if got := len(first); got != wantSize {
		t.Fatalf("encoded size = %d, want %d", got, wantSize)
	}
	if got := hashString(first); got != wantHash {
		t.Fatalf("encoded SHA-256 = %s, want %s", got, wantHash)
	}
}

func TestStatePreservesEncodingBehaviorVersion(t *testing.T) {
	value := testIndexState(t)
	value.Pipeline.EncodingBehaviorVersion = 42
	data, _, err := EncodeState(value, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeState(data, testStateLimits())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Pipeline == nil || decoded.Pipeline.EncodingBehaviorVersion != 42 {
		t.Fatalf("encoding behavior version = %+v, want 42", decoded.Pipeline)
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

func testIndexState(tb testing.TB) IndexState {
	tb.Helper()
	return IndexState{
		ID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Config: IndexConfig{
			Dimensions: 2,
			Metric:     2,
			Build:      BuildConfig{MaxNeighbors: 4, EfConstruction: 8, Seed: 99},
			Operational: OperationalLimits{
				Capacity:       CapacityLimits{MaxLiveVectors: 8, MaxStaleVectors: 7, MaxSegments: 3},
				Ingestion:      IngestionLimits{MaxChunksPerDocument: 2},
				Search:         SearchLimits{MaxResults: 2, MaxCandidateChunks: 8, MaxChunksPerResult: 1, MaxQueryChunks: 3, MaxEfSearch: 8, MaxVisitLimit: 8},
				SearchDefaults: SearchConfig{EfSearch: 4, VisitLimit: 8},
			},
		},
		Pipeline: &PipelineConfig{
			Model: Model{
				ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 16,
				DocumentPrefix: "document: ", QueryPrefix: "query: ",
			},
			Chunking:                ChunkingConfig{TargetBytes: 8, MaxBytes: 16, OverlapBytes: 2},
			Operational:             PipelineOperationalLimits{Encoding: EncodingLimits{MaxFields: 5, MaxSourceBytes: 2048, MaxEmbeddingBytes: 24}},
			EncodingBehaviorVersion: 1,
		},
		Revision:             11,
		MaxAllocatedVectorID: 2,
		NextSegmentID:        2,
		Segments: []SegmentState{{
			ID: 1,
			Rows: []VectorRow{
				{VectorID: 1, Chunk: Ref{ID: "chunk-1", DocID: "doc-1", Field: "title", Ordinal: 0, StartByte: 0, EndByte: 5}},
				{VectorID: 2, Chunk: Ref{ID: "chunk-2", DocID: "doc-1", Field: "body", Ordinal: 1, StartByte: 6, EndByte: 12}},
			},
			LivenessWords: []uint64{3},
		}},
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

func hashString(data []byte) string {
	const hex = "0123456789abcdef"
	sum := sha256.Sum256(data)
	result := make([]byte, len(sum)*2)
	for i, value := range sum {
		result[2*i] = hex[value>>4]
		result[2*i+1] = hex[value&0x0f]
	}
	return string(result)
}
