package semantic

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestSnapshotRoundTripPreservesSearchAndMappings(t *testing.T) {
	service := newSnapshotTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{
		testChunk("doc-b", "b-1", 0, []float32{1, 0}),
		testChunk("doc-b", "b-2", 1, []float32{1, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "a-1", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := replaceDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "a-new", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}

	wantChunks, err := service.searchChunks(ctx, []float32{0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	wantDocuments, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoint.Segment.Rows()) != 3 || checkpoint.Segment.Len() != 3 {
		t.Fatalf("checkpoint statistics/state = %+v", checkpoint)
	}
	gotChunks, err := checkpoint.searchChunks(ctx, []float32{0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	gotDocuments, err := checkpoint.searchEncodedDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(gotChunks.Hits, wantChunks.Hits) || !slices.EqualFunc(gotDocuments.Hits, wantDocuments.Hits, func(a, b DocumentHit) bool {
		return a.DocID == b.DocID && a.Distance == b.Distance && slices.Equal(a.Chunks, b.Chunks)
	}) {
		t.Fatalf("reader search mismatch\nchunks=%+v\ndocuments=%+v", gotChunks, gotDocuments)
	}
}

func TestSnapshotRejectsDanglingAndDuplicateState(t *testing.T) {
	service := newSnapshotTestService(t)
	if err := addDocument(t, service, context.Background(), []ChunkVector{testChunk("doc", "chunk", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := service.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	bad := checkpoint
	rows := checkpoint.Segment.Rows()
	rows[0].VectorID = 0
	bad.Segment = testSegmentWithRows(checkpoint, rows)
	if err := bad.Validate(); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("zero VectorID error = %v", err)
	}
	if err := addDocument(t, service, context.Background(), []ChunkVector{testChunk("doc-2", "chunk-2", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err = service.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	bad = checkpoint
	rows = checkpoint.Segment.Rows()
	rows[0], rows[1] = rows[1], rows[0]
	bad.Segment = testSegmentWithRows(checkpoint, rows)
	if err := bad.Validate(); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("non-monotonic row IDs error = %v", err)
	}
	bad = checkpoint
	rows = checkpoint.Segment.Rows()
	rows[1].Chunk.DocID = rows[0].Chunk.DocID
	rows[1].Chunk.ID = rows[0].Chunk.ID
	bad.Segment = testSegmentWithRows(checkpoint, rows)
	if err := bad.Validate(); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("duplicate document chunk error = %v", err)
	}
	bad = checkpoint
	bad.Segment = testSegmentWithRows(checkpoint, nil)
	if err := bad.Validate(); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("row-size error = %v", err)
	}
	bad = checkpoint
	bad.MaxChunkCandidates = bad.Segment.MaxK() + 1
	if err := bad.Validate(); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("segment search-bound error = %v", err)
	}
}

func TestSnapshotHonorsContext(t *testing.T) {
	service := newTestService(t)
	if _, err := service.Snapshot(nil); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Snapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
}

func TestSnapshotContainsOnlyLiveRowsAndPreservesSearch(t *testing.T) {
	service := newSnapshotTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := replaceDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "new", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	if !deleteDocument(t, service, "doc-b") {
		t.Fatal("DeleteDocument returned false")
	}
	checkpoint, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want, err := service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Segment.Len() != 1 || len(checkpoint.Segment.Rows()) != 1 {
		t.Fatalf("dense checkpoint rows = %d/%d", checkpoint.Segment.Len(), len(checkpoint.Segment.Rows()))
	}
	rows := checkpoint.Segment.Rows()
	if checkpoint.MaxAllocatedVectorID != 3 || rows[0].VectorID != 3 || rows[0].Chunk.ID != "new" {
		t.Fatalf("dense mappings = %+v", checkpoint)
	}
	got, err := checkpoint.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Hits, want.Hits) || got.Stats.RejectedNodes != 0 {
		t.Fatalf("checkpoint search changed\nwant=%+v\ngot=%+v", want, got)
	}
}

func TestSnapshotSupportsNoLiveVectors(t *testing.T) {
	service := newSnapshotTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc", "chunk", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if !deleteDocument(t, service, "doc") {
		t.Fatal("DeleteDocument returned false")
	}
	checkpoint, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Segment.Len() != 0 || len(checkpoint.Segment.Rows()) != 0 || checkpoint.MaxAllocatedVectorID != 1 {
		t.Fatalf("empty checkpoint = %+v", checkpoint)
	}
	if err := checkpoint.Validate(); err != nil {
		t.Fatal(err)
	}
}

func snapshotRowIDs(checkpoint Snapshot) []VectorID {
	rows := checkpoint.Segment.Rows()
	ids := make([]VectorID, len(rows))
	for i, row := range rows {
		ids[i] = row.VectorID
	}
	return ids
}

func testSegmentWithRows(snapshot Snapshot, rows []VectorRow) *Segment {
	return &Segment{
		component: snapshot.Segment.component,
		metadata:  snapshot.Segment.metadata,
		rows:      rows,
		searcher:  snapshot.Segment.searcher,
	}
}

func newSnapshotTestService(t *testing.T) *Service {
	t.Helper()
	config := testConfig(10, 100)
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return service
}
