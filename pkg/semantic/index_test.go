package semantic

import (
	"context"
	"errors"
	"testing"
)

func TestIndexDocumentLifecycleAndCompaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))
	addTestDocument(t, index, "doc-b", testChunk("doc-b", "b-1", 10, 0))
	mustFlush(t, index)
	assertStatistics(t, index.Statistics(), Statistics{
		Documents:            2,
		PhysicalVectors:      2,
		LiveVectors:          2,
		MaxAllocatedVectorID: 2,
	})

	replaceTestDocument(t, index, "doc-a", testChunk("doc-a", "a-2", 20, 0))
	assertNearestDocument(t, index, []float32{0, 0}, "doc-a")
	if err := index.Compact(ctx); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("Compact() with pending replacement error = %v, want %v", err, ErrPendingMutations)
	}
	mustFlush(t, index)
	assertNearestDocument(t, index, []float32{20, 0}, "doc-a")
	assertStatistics(t, index.Statistics(), Statistics{
		Documents:            2,
		PhysicalVectors:      3,
		LiveVectors:          2,
		StaleVectors:         1,
		MaxAllocatedVectorID: 3,
	})

	if err := index.Delete(ctx, "doc-b"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	assertNearestDocument(t, index, []float32{10, 0}, "doc-b")
	mustFlush(t, index)
	assertStatistics(t, index.Statistics(), Statistics{
		Documents:            1,
		PhysicalVectors:      3,
		LiveVectors:          1,
		StaleVectors:         2,
		MaxAllocatedVectorID: 3,
	})

	if err := index.Compact(ctx); err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	assertStatistics(t, index.Statistics(), Statistics{
		Documents:            1,
		PhysicalVectors:      1,
		LiveVectors:          1,
		MaxAllocatedVectorID: 3,
	})
	assertNearestDocument(t, index, []float32{20, 0}, "doc-a")

	state, err := index.CreateCheckpoint(ctx)
	if err != nil {
		t.Fatalf("CreateCheckpoint() error = %v", err)
	}
	if len(state.Segments) != 1 || len(state.Segments[0].Rows) != 1 {
		t.Fatalf("compacted state has %d segments and %d rows, want 1 and 1",
			len(state.Segments), len(state.Segments[0].Rows))
	}
	if got := state.Segments[0].Rows[0].VectorID; got != 3 {
		t.Fatalf("compaction changed vector ID to %d, want 3", got)
	}
	if state.NextSegmentID != 4 {
		t.Fatalf("next segment ID after compaction = %d, want 4", state.NextSegmentID)
	}
}
