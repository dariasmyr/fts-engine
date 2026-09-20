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
	if checkpoint.Segment.Kind() != SegmentKindChunkHNSW || checkpoint.Segment.Vectors() == nil || checkpoint.Segment.Searcher() == nil {
		t.Fatalf("search accessors = kind %d, vectors %p, searcher %p", checkpoint.Segment.Kind(), checkpoint.Segment.Vectors(), checkpoint.Segment.Searcher())
	}
	result, err := checkpoint.searchChunks(context.Background(), []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.Hits[0].Ref.DocID != "doc-00" {
		t.Fatalf("HNSW result = %+v", result)
	}
}

func TestBuildSegmentSearchesWithoutSnapshot(t *testing.T) {
	checkpoint := hnswSnapshotFixture(t)
	segment, err := BuildSegment(context.Background(), MutableHeadID, SegmentMetadata{
		Space: checkpoint.Space, Chunking: checkpoint.Chunking,
	}, checkpoint.Segment.Vectors(), checkpoint.Segment.Rows(), hnsw.BuildOptions{
		BuildConfig: hnsw.BuildConfig{
			Dimensions: checkpoint.Space.Dimensions, Metric: checkpoint.Space.Metric,
			MaxVectors: checkpoint.Segment.Len(), MaxVectorBytes: uint64(checkpoint.Segment.Len() * checkpoint.Space.Dimensions * 4),
			MaxNeighbors: 4, EfConstruction: 16, Seed: 23,
		},
		SearchConfig: hnsw.SearchConfig{
			DefaultEfSearch: checkpoint.MaxK, MaxEfSearch: checkpoint.MaxK,
			DefaultVisitLimit: checkpoint.Segment.Len(), MaxVisitLimit: checkpoint.Segment.Len(), MaxK: checkpoint.MaxK,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = segment.Search(context.Background(), []float32{0, 0}, 2, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotRebuildsHNSWAfterRemovingStaleRows(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 20 {
		if err := addDocument(t, service, ctx, []ChunkVector{testChunk(fts.DocID(fmt.Sprintf("doc-%02d", i)), "chunk", 0, []float32{float32(i), 0})}); err != nil {
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
		t.Fatal("searcher has no entry point")
	}
	entryOrdinal := int(entry)
	if entryOrdinal >= len(snapshot.Segment.Rows()) {
		t.Fatalf("entry ordinal %d out of range", entryOrdinal)
	}
	deletedDocID := snapshot.Segment.Rows()[entryOrdinal].Chunk.DocID
	if !deleteDocument(t, service, deletedDocID) {
		t.Fatal("failed to stale searcher entry document")
	}
	checkpoint, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Segment.Len() != 19 || len(checkpoint.Segment.Rows()) != 19 {
		t.Fatalf("dense checkpoint rows = %d/%d", checkpoint.Segment.Len(), len(checkpoint.Segment.Rows()))
	}
	liveSegment := checkpoint.Segment
	staleSegment := &Segment{metadata: SegmentMetadata{Space: checkpoint.Space, Chunking: checkpoint.Chunking}, searcher: oldGraph, rows: checkpoint.Segment.Rows(), component: MutableHeadID}
	checkpoint.Segment = staleSegment
	if err := checkpoint.Validate(); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("stale topology error = %v", err)
	}
	checkpoint.Segment = liveSegment
	searcher := buildSnapshotGraph(t, checkpoint, 7)
	checkpoint.Segment, err = NewSegment(MutableHeadID, SegmentMetadata{Space: checkpoint.Space, Chunking: checkpoint.Chunking}, searcher, checkpoint.Segment.Rows())
	if err != nil {
		t.Fatal(err)
	}
	result, err := checkpoint.searchChunks(ctx, []float32{19, 0}, 1)
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
		if err := addDocument(t, service, context.Background(), []ChunkVector{testChunk(fts.DocID(fmt.Sprintf("doc-%02d", i)), "chunk", 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint, err := service.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	searcher := buildSnapshotGraph(t, checkpoint, 11)
	checkpoint.Segment, err = NewSegment(MutableHeadID, SegmentMetadata{Space: checkpoint.Space, Chunking: checkpoint.Chunking}, searcher, checkpoint.Segment.Rows())
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoint.Validate(); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func buildSnapshotGraph(t testing.TB, checkpoint Snapshot, seed uint64) *hnsw.Searcher {
	t.Helper()
	maxK := max(checkpoint.MaxK, checkpoint.MaxChunkCandidates)
	searcher, err := hnsw.BuildSearcher(context.Background(), checkpoint.Segment.Vectors(), hnsw.BuildOptions{
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
	return searcher
}

var _ vector.Searcher = (*Segment)(nil)
