package semantic

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func TestChunkHNSWSegmentAccessors(t *testing.T) {
	checkpoint := hnswSnapshotFixture(t)
	if checkpoint.Segment.Kind() != SegmentKindChunkHNSW || checkpoint.Segment.Vectors() == nil || checkpoint.Segment.HNSW() == nil {
		t.Fatalf("HNSW accessors = kind %d, vectors %p, graph %p", checkpoint.Segment.Kind(), checkpoint.Segment.Vectors(), checkpoint.Segment.HNSW())
	}
	result, err := checkpoint.SearchChunks(context.Background(), []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.UsedExactFallback || len(result.Hits) != 2 || result.Hits[0].Ref.DocID != "doc-00" {
		t.Fatalf("HNSW result = %+v", result)
	}
}

func TestSnapshotRebuildsHNSWAfterRemovingStaleRows(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 20 {
		if err := service.AddDocument(ctx, []ChunkVector{testChunk(fts.DocID(fmt.Sprintf("doc-%02d", i)), "chunk", 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldGraph := buildSnapshotGraph(t, snapshot, 7)
	entry, _, ok := oldGraph.EntryPoint()
	if !ok {
		t.Fatal("graph has no entry point")
	}
	entryOrdinal := int(entry)
	if entryOrdinal >= len(snapshot.Rows) {
		t.Fatalf("entry ordinal %d out of range", entryOrdinal)
	}
	deletedDocID := snapshot.Rows[entryOrdinal].Chunk.DocID
	if !service.DeleteDocument(deletedDocID) {
		t.Fatal("failed to stale graph entry document")
	}
	checkpoint, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Segment.Len() != 19 || len(checkpoint.Rows) != 19 {
		t.Fatalf("dense checkpoint rows = %d/%d", checkpoint.Segment.Len(), len(checkpoint.Rows))
	}
	liveSegment := checkpoint.Segment
	staleSegment := &SealedSegment{kind: SegmentKindChunkHNSW, vectors: oldGraph, graph: oldGraph, rows: snapshot.Rows, component: MutableHeadID}
	checkpoint.Segment = staleSegment
	if err := checkpoint.Validate(); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("stale topology error = %v", err)
	}
	checkpoint.Segment = liveSegment
	graph := buildSnapshotGraph(t, checkpoint, 7)
	checkpoint.Segment, err = NewHNSWSegment(MutableHeadID, graph, graph, checkpoint.Rows)
	if err != nil {
		t.Fatal(err)
	}
	result, err := checkpoint.SearchChunks(ctx, []float32{19, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Ref.DocID == deletedDocID || result.Stats.RejectedNodes != 0 {
		t.Fatalf("rebuilt search result = %+v", result)
	}
}

func hnswSnapshotFixture(t *testing.T) Snapshot {
	t.Helper()
	service := newTestService(t)
	for i := range 12 {
		if err := service.AddDocument(context.Background(), []ChunkVector{testChunk(fts.DocID(fmt.Sprintf("doc-%02d", i)), "chunk", 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint, err := service.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	graph := buildSnapshotGraph(t, checkpoint, 11)
	checkpoint.Segment, err = NewHNSWSegment(MutableHeadID, checkpoint.Segment.Vectors(), graph, checkpoint.Rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoint.Validate(); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func buildSnapshotGraph(t testing.TB, checkpoint Snapshot, seed uint64) *hnsw.Reader {
	t.Helper()
	maxK := max(checkpoint.MaxK, checkpoint.MaxChunkCandidates)
	graph, err := hnsw.Build(context.Background(), checkpoint.Segment.Vectors(), hnsw.BuildOptions{
		BuildConfig: hnsw.BuildConfig{
			Dimensions: checkpoint.Space.Dimensions, Metric: checkpoint.Space.Metric,
			MaxVectors: checkpoint.Segment.Len(), MaxVectorBytes: uint64(checkpoint.Segment.Len() * checkpoint.Space.Dimensions * 4),
			MaxNeighbors: 4, EfConstruction: 16, Seed: seed,
		},
		SearchConfig: hnsw.SearchConfig{
			DefaultEfSearch: maxK, MaxEfSearch: maxK, DefaultVisitLimit: checkpoint.Segment.Len(),
			MaxVisitLimit: checkpoint.Segment.Len(), MaxK: maxK,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return graph
}

var _ vector.Searcher = (*SealedSegment)(nil)
