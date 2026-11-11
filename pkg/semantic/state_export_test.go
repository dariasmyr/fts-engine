package semantic

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

func TestCommittedIndexStateRestoreRoundTripPreservesCommittedIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0), testChunk("doc-a", "a-2", 1, 0))
	addTestDocument(t, index, "doc-b", testChunk("doc-b", "b-1", 10, 0))
	mustCommit(t, index)

	state, err := index.ExportState(ctx)
	if err != nil {
		t.Fatalf("ExportState() error = %v", err)
	}
	if state.Revision != 2 || state.MaxAllocatedVectorID != 3 || state.NextSegmentID != 2 {
		t.Fatalf("ExportState() metadata = revision %d, max vector %d, next segment %d; want 2, 3, 2",
			state.Revision, state.MaxAllocatedVectorID, state.NextSegmentID)
	}
	if state.ID == (IndexID{}) {
		t.Fatal("ExportState() index ID is zero")
	}
	if len(state.Segments) != 1 || len(state.Segments[0].Rows) != 3 {
		t.Fatalf("ExportState() segments = %d with rows %d, want 1 with 3 rows",
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
	mustCommit(t, restored)

	continued, err := restored.ExportState(ctx)
	if err != nil {
		t.Fatalf("ExportState() after restore error = %v", err)
	}
	if continued.Revision != 3 || continued.MaxAllocatedVectorID != 4 || continued.NextSegmentID != 3 {
		t.Fatalf("continued metadata = revision %d, max vector %d, next segment %d; want 3, 4, 3",
			continued.Revision, continued.MaxAllocatedVectorID, continued.NextSegmentID)
	}
	if continued.ID != state.ID {
		t.Fatalf("continued index ID = %x, want %x", continued.ID, state.ID)
	}
	if len(continued.Segments) != 2 {
		t.Fatalf("continued segment count = %d, want 2", len(continued.Segments))
	}
	if got := continued.Segments[1].Rows[0].VectorID; got != 4 {
		t.Fatalf("continued vector ID = %d, want 4", got)
	}
}

func TestIndexExportStateRejectsPendingMutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))

	if _, err := index.ExportState(ctx); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("ExportState() error = %v, want %v", err, ErrPendingMutations)
	}
	if err := index.Compact(ctx); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("Compact() error = %v, want %v", err, ErrPendingMutations)
	}

	result, err := index.Search(ctx, []ChunkEmbedding{testChunk("query", "q-1", 0, 0)}, 1, QueryConfig{})
	if err != nil {
		t.Fatalf("Search() before Commit error = %v", err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("Search() before Commit returned %d hits, want 0", len(result.Hits))
	}

	mustCommit(t, index)
	state, err := index.ExportState(ctx)
	if err != nil {
		t.Fatalf("ExportState() after Commit error = %v", err)
	}
	if state.Revision != 1 || len(state.Segments) != 1 {
		t.Fatalf("ExportState() after Commit = revision %d, segments %d; want 1, 1",
			state.Revision, len(state.Segments))
	}
}

func TestIndexCommitAndExportStateCommitsPendingRevision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))

	state, err := index.CommitAndExportState(ctx)
	if err != nil {
		t.Fatalf("CommitAndExportState() error = %v", err)
	}
	if state.Revision != 1 || len(state.Segments) != 1 {
		t.Fatalf(
			"CommitAndExportState() = revision %d, segments %d; want 1, 1",
			state.Revision,
			len(state.Segments),
		)
	}
	if got := state.Segments[0].Rows[0].Chunk.DocID; got != "doc-a" {
		t.Fatalf("committed document = %q, want doc-a", got)
	}
	if _, err := index.ExportState(ctx); err != nil {
		t.Fatalf("ExportState() after CommitAndExportState error = %v", err)
	}
}

func TestIndexCommitWithStateRejectsCandidateWithoutPublishing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))
	rejected := errors.New("reject candidate")

	err := index.CommitWithState(ctx, func(state *CommittedIndexState) error {
		if state.Revision != 1 || state.MaxAllocatedVectorID != 1 ||
			state.NextSegmentID != 2 || len(state.Segments) != 1 {
			t.Fatalf(
				"candidate state = revision %d, max vector %d, next segment %d, segments %d; want 1, 1, 2, 1",
				state.Revision,
				state.MaxAllocatedVectorID,
				state.NextSegmentID,
				len(state.Segments),
			)
		}
		result, err := index.Search(
			ctx,
			[]ChunkEmbedding{testChunk("query", "q-1", 0, 0)},
			1,
			QueryConfig{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Hits) != 0 {
			t.Fatalf("Search() during accept returned hits %+v, want none", result.Hits)
		}
		return rejected
	})
	if !errors.Is(err, rejected) {
		t.Fatalf("CommitWithState() error = %v, want rejected candidate", err)
	}
	if _, err := index.ExportState(ctx); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("ExportState() after rejection error = %v, want ErrPendingMutations", err)
	}

	if err := index.CommitWithState(ctx, func(*CommittedIndexState) error { return nil }); err != nil {
		t.Fatal(err)
	}
	assertNearestDocument(t, index, []float32{0, 0}, "doc-a")
}

func TestIndexCommitWithStateRejectsNilCallback(t *testing.T) {
	t.Parallel()

	index := newTestIndex(t)
	if err := index.CommitWithState(context.Background(), nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("CommitWithState() error = %v, want ErrInvalidConfig", err)
	}
	if err := index.CommitWithState(nil, nil); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("CommitWithState(nil) error = %v, want vector.ErrNilContext", err)
	}
}

func TestIndexCommitWithStatePublishesAfterAcceptedContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))

	err := index.CommitWithState(ctx, func(*CommittedIndexState) error {
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("CommitWithState() error = %v, want nil after acceptance", err)
	}
	assertNearestDocument(t, index, []float32{0, 0}, "doc-a")
}

func TestRestoreRejectsInvalidCommittedIndexState(t *testing.T) {
	t.Parallel()

	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))
	mustCommit(t, index)
	valid, err := index.ExportState(context.Background())
	if err != nil {
		t.Fatalf("ExportState() error = %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*CommittedIndexState)
		wantErr error
	}{
		{
			name: "zero index ID",
			mutate: func(state *CommittedIndexState) {
				state.ID = IndexID{}
			},
			wantErr: ErrInternalState,
		},
		{
			name: "zero next segment ID",
			mutate: func(state *CommittedIndexState) {
				state.NextSegmentID = 0
			},
			wantErr: ErrInternalState,
		},
		{
			name: "allocated watermark below stored vector",
			mutate: func(state *CommittedIndexState) {
				state.MaxAllocatedVectorID = 0
			},
			wantErr: ErrInternalState,
		},
		{
			name: "invalid liveness word count",
			mutate: func(state *CommittedIndexState) {
				state.Segments[0].LivenessWords = nil
			},
			wantErr: ErrInvalidSegment,
		},
		{
			name: "nil state index",
			mutate: func(state *CommittedIndexState) {
				state.Segments[0].Index = nil
			},
			wantErr: ErrInvalidSegment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := cloneTestState(*valid)
			tt.mutate(&state)
			if _, err := Restore(context.Background(), state); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Restore() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func cloneTestState(state CommittedIndexState) CommittedIndexState {
	state.Segments = append([]SegmentState(nil), state.Segments...)
	for i := range state.Segments {
		state.Segments[i].Rows = append([]VectorRow(nil), state.Segments[i].Rows...)
		state.Segments[i].LivenessWords = append([]uint64(nil), state.Segments[i].LivenessWords...)
	}
	return state
}

func newTestIndex(t *testing.T) *Index {
	t.Helper()

	index, err := NewIndex(IndexConfig{
		Dimensions: 2,
		Metric:     vector.MetricL2Squared,
		Limits: LimitsConfig{
			MaxLiveVectors: 32, MaxStaleVectors: 32, MaxSegments: 8, MaxChunksPerDocument: 4,
		},
		Search: SearchConfig{MaxResults: 4, MaxCandidateChunks: 16, MaxChunksPerResult: 2},
		HNSW: hnsw.Config{
			Build:  hnsw.BuildConfig{MaxNeighbors: 2, EfConstruction: 4, Seed: 1},
			Search: hnsw.SearchConfig{EfSearch: 8, VisitLimit: 32},
		},
	})
	if err != nil {
		t.Fatalf("NewIndex() error = %v", err)
	}
	return index
}

func testChunk(docID fts.DocID, chunkID ChunkID, values ...float32) ChunkEmbedding {
	return ChunkEmbedding{
		Ref: Ref{
			ID:        chunkID,
			DocID:     docID,
			Field:     "body",
			StartByte: 0,
			EndByte:   1,
		},
		Embedding: values,
	}
}

func addTestDocument(t *testing.T, index *Index, docID fts.DocID, chunks ...ChunkEmbedding) {
	t.Helper()
	if err := index.AddDocument(context.Background(), docID, chunks); err != nil {
		t.Fatalf("Add(%q) error = %v", docID, err)
	}
}

func replaceTestDocument(t *testing.T, index *Index, docID fts.DocID, chunks ...ChunkEmbedding) {
	t.Helper()
	if err := index.ReplaceDocument(context.Background(), docID, chunks); err != nil {
		t.Fatalf("Replace(%q) error = %v", docID, err)
	}
}

func mustCommit(t *testing.T, index *Index) {
	t.Helper()
	if err := index.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
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
		[]ChunkEmbedding{testChunk("query", "query-1", query...)},
		1,
		QueryConfig{},
	)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != expected {
		t.Fatalf("Search() hits = %+v, want nearest document %q", result.Hits, expected)
	}
}
