package semantic

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestCheckpointRoundTripPreservesSearchAndMappings(t *testing.T) {
	service := newCheckpointTestService(t)
	ctx := context.Background()
	if err := service.AddDocument(ctx, []ChunkVector{
		testChunk("doc-b", "b-1", 0, []float32{1, 0}),
		testChunk("doc-b", "b-2", 1, []float32{1, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc-a", "a-1", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := service.ReplaceDocument(ctx, []ChunkVector{testChunk("doc-a", "a-new", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}

	wantChunks, err := service.SearchChunks(ctx, []float32{0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	wantDocuments, err := service.SearchDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := service.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoint.Documents) != 2 || checkpoint.Documents[0].DocID != "doc-a" || checkpoint.Documents[1].DocID != "doc-b" {
		t.Fatalf("documents are not canonical: %+v", checkpoint.Documents)
	}
	if checkpoint.Live.AllowedOrdinalCount() != 3 || checkpoint.Segment.Len() != 4 || checkpoint.DuplicateStatistics == nil || checkpoint.DuplicateStatistics.DuplicateRows != 1 {
		t.Fatalf("checkpoint statistics/state = %+v", checkpoint)
	}
	reader, err := OpenCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	gotChunks, err := reader.SearchChunks(ctx, []float32{0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	gotDocuments, err := reader.SearchDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(gotChunks.Hits, wantChunks.Hits) || !slices.EqualFunc(gotDocuments.Hits, wantDocuments.Hits, func(a, b DocumentHit) bool {
		return a.DocID == b.DocID && a.Distance == b.Distance && slices.Equal(a.Chunks, b.Chunks)
	}) {
		t.Fatalf("reader search mismatch\nchunks=%+v\ndocuments=%+v", gotChunks, gotDocuments)
	}
}

func TestCheckpointRejectsDanglingAndDuplicateState(t *testing.T) {
	service := newCheckpointTestService(t)
	if err := service.AddDocument(context.Background(), []ChunkVector{testChunk("doc", "chunk", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := service.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}

	bad := checkpoint
	bad.VectorIDs = []VectorID{0}
	if err := bad.Validate(); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("zero VectorID error = %v", err)
	}
	bad = checkpoint
	bad.Documents = append(cloneDocumentRecords(checkpoint.Documents), checkpoint.Documents[0])
	if err := bad.Validate(); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("duplicate document error = %v", err)
	}
	bad = checkpoint
	bad.Live, err = vector.NewBitSetFromWords(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.Validate(); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("liveness-size error = %v", err)
	}
	bad = checkpoint
	stats := *checkpoint.DuplicateStatistics
	stats.DuplicateRows++
	bad.DuplicateStatistics = &stats
	if err := bad.Validate(); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("duplicate-stat error = %v", err)
	}
	bad = checkpoint
	bad.MaxChunkCandidates = bad.Segment.MaxK() + 1
	if err := bad.Validate(); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("segment search-bound error = %v", err)
	}
}

func TestCheckpointSkipsDuplicateStatisticsByDefault(t *testing.T) {
	service := newTestService(t)
	if err := service.AddDocument(context.Background(), []ChunkVector{testChunk("doc", "chunk", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := service.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.DuplicateStatistics != nil {
		t.Fatalf("duplicate statistics = %+v, want nil", checkpoint.DuplicateStatistics)
	}
}

func TestCheckpointBuildLiveOnlyRemovesStaleRowsAndPreservesSearch(t *testing.T) {
	service := newCheckpointTestService(t)
	ctx := context.Background()
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc-a", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := service.ReplaceDocument(ctx, []ChunkVector{testChunk("doc-a", "new", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	if !service.DeleteDocument("doc-b") {
		t.Fatal("DeleteDocument returned false")
	}
	checkpoint, err := service.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	wantReader, err := OpenCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	want, err := wantReader.SearchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}

	compacted, err := checkpoint.BuildLiveOnly(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Segment.Len() != 3 || compacted.Segment.Len() != 1 || compacted.Live.TotalOrdinalCount() != 1 || compacted.Live.AllowedOrdinalCount() != 1 {
		t.Fatalf("source/compacted state = %d/%d, live=%d/%d", checkpoint.Segment.Len(), compacted.Segment.Len(), compacted.Live.AllowedOrdinalCount(), compacted.Live.TotalOrdinalCount())
	}
	if compacted.MaxAllocatedVectorID != 3 || !slices.Equal(compacted.VectorIDs, []VectorID{3}) || len(compacted.Refs) != 1 || compacted.Refs[0].VectorID != 3 {
		t.Fatalf("compacted mappings = %+v", compacted)
	}
	if compacted.DuplicateStatistics == nil || compacted.DuplicateStatistics.VectorRows != 1 || compacted.DuplicateStatistics.DuplicateRows != 0 {
		t.Fatalf("compacted duplicate statistics = %+v", compacted.DuplicateStatistics)
	}
	reader, err := OpenCheckpoint(compacted)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.SearchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Hits, want.Hits) || got.Stats.RejectedNodes != 0 {
		t.Fatalf("compacted search changed\nwant=%+v\ngot=%+v", want, got)
	}
}

func TestCheckpointBuildLiveOnlySupportsNoLiveVectors(t *testing.T) {
	service := newCheckpointTestService(t)
	ctx := context.Background()
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc", "chunk", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if !service.DeleteDocument("doc") {
		t.Fatal("DeleteDocument returned false")
	}
	checkpoint, err := service.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	compacted, err := checkpoint.BuildLiveOnly(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if compacted.Segment.Len() != 0 || compacted.Live.TotalOrdinalCount() != 0 || len(compacted.VectorIDs) != 0 || len(compacted.Refs) != 0 || len(compacted.Documents) != 0 || compacted.MaxAllocatedVectorID != 1 {
		t.Fatalf("empty compacted checkpoint = %+v", compacted)
	}
	if err := compacted.Validate(); err != nil {
		t.Fatal(err)
	}
}

func newCheckpointTestService(t *testing.T) *Service {
	t.Helper()
	config := testConfig(10, 100)
	config.CollectDuplicateStatistics = true
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return service
}
