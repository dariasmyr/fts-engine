package semantic

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestIndex_CheckpointRestoreRoundTripPreservesCommittedIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0), testChunk("doc-a", "a-2", 1, 0))
	addTestDocument(t, index, "doc-b", testChunk("doc-b", "b-1", 10, 0))
	mustFlush(t, index)

	state, err := index.CreateCheckpoint(ctx)
	if err != nil {
		t.Fatalf("CreateCheckpoint() error = %v", err)
	}
	if state.Revision != 2 || state.MaxAllocatedVectorID != 3 || state.NextSegmentID != 2 {
		t.Fatalf("CreateCheckpoint() metadata = revision %d, max vector %d, next segment %d; want 2, 3, 2",
			state.Revision, state.MaxAllocatedVectorID, state.NextSegmentID)
	}
	if len(state.Segments) != 1 || len(state.Segments[0].Rows) != 3 {
		t.Fatalf("CreateCheckpoint() segments = %d with rows %d, want 1 with 3 rows",
			len(state.Segments), len(state.Segments[0].Rows))
	}

	restored, err := Restore(ctx, *state)
	if err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	assertStatistics(t, restored.Statistics(), Statistics{
		Documents:            2,
		PhysicalVectors:      3,
		LiveVectors:          3,
		MaxAllocatedVectorID: 3,
	})
	assertNearestDocument(t, restored, []float32{0, 0}, "doc-a")

	addTestDocument(t, restored, "doc-c", testChunk("doc-c", "c-1", 20, 0))
	mustFlush(t, restored)

	continued, err := restored.CreateCheckpoint(ctx)
	if err != nil {
		t.Fatalf("CreateCheckpoint() after restore error = %v", err)
	}
	if continued.Revision != 3 || continued.MaxAllocatedVectorID != 4 || continued.NextSegmentID != 3 {
		t.Fatalf("continued metadata = revision %d, max vector %d, next segment %d; want 3, 4, 3",
			continued.Revision, continued.MaxAllocatedVectorID, continued.NextSegmentID)
	}
	if len(continued.Segments) != 2 {
		t.Fatalf("continued segment count = %d, want 2", len(continued.Segments))
	}
	if got := continued.Segments[1].Rows[0].VectorID; got != 4 {
		t.Fatalf("continued vector ID = %d, want 4", got)
	}
}

func TestIndex_CheckpointRejectsPendingMutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))

	if _, err := index.CreateCheckpoint(ctx); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("CreateCheckpoint() error = %v, want %v", err, ErrPendingMutations)
	}
	if err := index.Compact(ctx); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("Compact() error = %v, want %v", err, ErrPendingMutations)
	}

	result, err := index.Search(ctx, []EncodedChunk{testChunk("query", "q-1", 0, 0)}, 1, SearchOptions{})
	if err != nil {
		t.Fatalf("Search() before Flush error = %v", err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("Search() before Flush returned %d hits, want 0", len(result.Hits))
	}

	mustFlush(t, index)
	state, err := index.CreateCheckpoint(ctx)
	if err != nil {
		t.Fatalf("CreateCheckpoint() after Flush error = %v", err)
	}
	if state.Revision != 1 || len(state.Segments) != 1 {
		t.Fatalf("CreateCheckpoint() after Flush = revision %d, segments %d; want 1, 1",
			state.Revision, len(state.Segments))
	}
}

func TestRestoreRejectsInvalidCheckpoint(t *testing.T) {
	t.Parallel()

	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))
	mustFlush(t, index)
	valid, err := index.CreateCheckpoint(context.Background())
	if err != nil {
		t.Fatalf("CreateCheckpoint() error = %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*IndexCheckpoint)
		wantErr error
	}{
		{
			name: "zero next segment ID",
			mutate: func(state *IndexCheckpoint) {
				state.NextSegmentID = 0
			},
			wantErr: ErrInternalState,
		},
		{
			name: "allocated watermark below stored vector",
			mutate: func(state *IndexCheckpoint) {
				state.MaxAllocatedVectorID = 0
			},
			wantErr: ErrInternalState,
		},
		{
			name: "segment embedding mismatch",
			mutate: func(state *IndexCheckpoint) {
				state.Segments[0].Schema.Embedding.ModelVersion = "other"
			},
			wantErr: ErrEmbeddingMismatch,
		},
		{
			name: "invalid liveness word count",
			mutate: func(state *IndexCheckpoint) {
				state.Segments[0].LivenessWords = nil
			},
			wantErr: ErrInvalidSegment,
		},
		{
			name: "unsupported checkpoint index",
			mutate: func(state *IndexCheckpoint) {
				state.Segments[0].Index = checkpointIndexStub{}
			},
			wantErr: ErrInvalidSegment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := cloneTestCheckpoint(*valid)
			tt.mutate(&state)
			if _, err := Restore(context.Background(), state); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Restore() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

type checkpointIndexStub struct{}

func (checkpointIndexStub) Len() int                            { return 1 }
func (checkpointIndexStub) Dimensions() int                     { return 2 }
func (checkpointIndexStub) Metric() vector.Metric               { return vector.MetricL2Squared }
func (checkpointIndexStub) Normalization() vector.Normalization { return vector.NormalizationNone }

func cloneTestCheckpoint(state IndexCheckpoint) IndexCheckpoint {
	state.Segments = append([]CheckpointSegment(nil), state.Segments...)
	for i := range state.Segments {
		state.Segments[i].Rows = append([]VectorRow(nil), state.Segments[i].Rows...)
		state.Segments[i].LivenessWords = append([]uint64(nil), state.Segments[i].LivenessWords...)
	}
	return state
}

func newTestIndex(t *testing.T) *Index {
	t.Helper()

	embedding, err := NewEmbeddingDescriptor(
		"test-provider",
		"test-model",
		"v1",
		"test-pipeline-v1",
		2,
		vector.MetricL2Squared,
		1,
	)
	if err != nil {
		t.Fatalf("NewEmbeddingDescriptor() error = %v", err)
	}
	index, err := NewIndex(Config{
		Schema: Schema{
			Embedding: embedding,
			Chunking: ChunkingDescriptor{
				ID:          "test-chunks",
				Version:     1,
				Fingerprint: "test-chunks-v1",
			},
		},
		Limits: Limits{
			MaxLiveVectors:          32,
			MaxStaleVectors:         32,
			MaxSegments:             8,
			MaxChunksPerDocument:    4,
			MaxDocumentsPerSearch:   4,
			MaxChunkCandidates:      16,
			MaxChunksPerDocumentHit: 2,
		},
		HNSW: HNSWTuning{
			MaxNeighbors:      2,
			EfConstruction:    4,
			Seed:              1,
			DefaultEfSearch:   8,
			MaxEfSearch:       16,
			DefaultVisitLimit: 32,
			MaxVisitLimit:     32,
		},
	})
	if err != nil {
		t.Fatalf("NewIndex() error = %v", err)
	}
	return index
}

func testChunk(docID fts.DocID, chunkID ChunkID, values ...float32) EncodedChunk {
	return EncodedChunk{
		Ref: Ref{
			ID:        chunkID,
			DocID:     docID,
			Field:     "body",
			StartByte: 0,
			EndByte:   1,
		},
		Vector: values,
	}
}

func addTestDocument(t *testing.T, index *Index, docID fts.DocID, chunks ...EncodedChunk) {
	t.Helper()
	if err := index.Add(context.Background(), docID, chunks); err != nil {
		t.Fatalf("Add(%q) error = %v", docID, err)
	}
}

func replaceTestDocument(t *testing.T, index *Index, docID fts.DocID, chunks ...EncodedChunk) {
	t.Helper()
	if err := index.Replace(context.Background(), docID, chunks); err != nil {
		t.Fatalf("Replace(%q) error = %v", docID, err)
	}
}

func mustFlush(t *testing.T, index *Index) {
	t.Helper()
	if err := index.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func assertStatistics(t *testing.T, got, expected Statistics) {
	t.Helper()
	if got != expected {
		t.Fatalf("Statistics() = %+v, want %+v", got, expected)
	}
}

func assertNearestDocument(t *testing.T, index *Index, query []float32, expected fts.DocID) {
	t.Helper()
	result, err := index.Search(
		context.Background(),
		[]EncodedChunk{testChunk("query", "query-1", query...)},
		1,
		SearchOptions{},
	)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != expected {
		t.Fatalf("Search() hits = %+v, want nearest document %q", result.Hits, expected)
	}
}
