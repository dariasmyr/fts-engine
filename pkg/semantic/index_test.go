package semantic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

func TestIndexOwnsMetricPreparation(t *testing.T) {
	schema := serviceTestSchema(t)
	index, err := NewIndex(serviceTestConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	raw := []float32{3, 4}
	if err := index.AddDocument(t.Context(), "doc", []ChunkEmbedding{{
		Ref:       Ref{ID: "chunk", DocID: "doc", Field: "body", EndByte: 1},
		Embedding: raw,
	}}); err != nil {
		t.Fatal(err)
	}
	raw[0] = 99
	stored := make([]float32, 2)
	if err := index.state.pending.additions[0].vector.CopyTo(stored); err != nil {
		t.Fatal(err)
	}
	if len(stored) != 2 || math.Abs(float64(stored[0])-0.6) > 1e-6 || math.Abs(float64(stored[1])-0.8) > 1e-6 {
		t.Fatalf("prepared embedding = %v, want [0.6 0.8]", stored)
	}
	if err := index.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	result, err := index.Search(t.Context(), []ChunkEmbedding{{Embedding: []float32{6, 8}}}, 1, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != "doc" {
		t.Fatalf("Search() hits = %+v, want doc", result.Hits)
	}
}

func TestIndexDocumentLifecycleAndCompaction(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	index := newTestIndex(t)
	addTestDocument(t, index, "doc-a", testChunk("doc-a", "a-1", 0, 0))
	addTestDocument(t, index, "doc-b", testChunk("doc-b", "b-1", 10, 0))
	mustCommit(t, index)
	assertStatistics(t, index.Statistics(), Statistics{
		Documents:            2,
		PhysicalVectors:      2,
		LiveVectors:          2,
		MaxAllocatedVectorID: 2,
	})

	replaceTestDocument(t, index, "doc-a", testChunk("doc-a", "a-2", 20, 0))
	assertNearestDocument(t, index, []float32{0, 0}, "doc-a")
	assertNearestDocument(t, index, []float32{20, 0}, "doc-b")
	if err := index.Compact(ctx); !errors.Is(err, ErrPendingMutations) {
		t.Fatalf("Compact() with pending replacement error = %v, want %v", err, ErrPendingMutations)
	}
	mustCommit(t, index)
	assertNearestDocument(t, index, []float32{20, 0}, "doc-a")
	assertStatistics(t, index.Statistics(), Statistics{
		Documents:            2,
		PhysicalVectors:      3,
		LiveVectors:          2,
		StaleVectors:         1,
		MaxAllocatedVectorID: 3,
	})

	if err := index.DeleteDocument(ctx, "doc-b"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	assertNearestDocument(t, index, []float32{10, 0}, "doc-b")
	mustCommit(t, index)
	assertStatistics(t, index.Statistics(), Statistics{
		Documents:            1,
		PhysicalVectors:      3,
		LiveVectors:          1,
		StaleVectors:         2,
		MaxAllocatedVectorID: 3,
	})
	assertNearestDocument(t, index, []float32{10, 0}, "doc-a")

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

	state, err := index.ExportState(ctx)
	if err != nil {
		t.Fatalf("ExportState() error = %v", err)
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

func TestIndexSearchDuringCommit(t *testing.T) {
	t.Parallel()

	index := newTestIndex(t)
	addTestDocument(t, index, "published", testChunk("published", "published-1", 0, 0))
	mustCommit(t, index)

	start := make(chan struct{})
	searchDone := make(chan error, 1)
	go func() {
		<-start
		for range 500 {
			result, err := index.Search(
				context.Background(),
				[]ChunkEmbedding{testChunk("query", "query-1", 0, 0)},
				1,
				SearchOptions{},
			)
			if err != nil {
				searchDone <- err
				return
			}
			if len(result.Hits) != 1 || result.Hits[0].DocID != "published" {
				searchDone <- fmt.Errorf("search hits = %+v, want published", result.Hits)
				return
			}
		}
		searchDone <- nil
	}()

	close(start)
	for n := 0; n < 6; n++ {
		name := fmt.Sprintf("pending-%d", n)
		docID := fts.DocID(name)
		addTestDocument(
			t,
			index,
			docID,
			testChunk(docID, ChunkID(name+"-1"), float32(100+n), 0),
		)
		mustCommit(t, index)
	}
	if err := <-searchDone; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledCommitKeepsPreviousIndexViewVisible(t *testing.T) {
	t.Parallel()

	index := newTestIndex(t)
	addTestDocument(t, index, "published", testChunk("published", "published-1", 0, 0))
	mustCommit(t, index)
	addTestDocument(t, index, "pending", testChunk("pending", "pending-1", 10, 0))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := index.Commit(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Commit() error = %v, want context.Canceled", err)
	}
	assertNearestDocument(t, index, []float32{10, 0}, "published")
	if got := index.Statistics(); got.Documents != 2 || got.LiveVectors != 2 || got.PhysicalVectors != 2 {
		t.Fatalf("Statistics() after canceled commit = %+v", got)
	}

	mustCommit(t, index)
	assertNearestDocument(t, index, []float32{10, 0}, "pending")
}
