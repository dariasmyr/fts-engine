package semantic

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestCheckpointRoundTripPreservesSearchAndMappings(t *testing.T) {
	service := newTestService(t)
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
	if checkpoint.Live.AllowedOrdinalCount() != 3 || checkpoint.Segment.Len() != 4 || checkpoint.DuplicateStatistics.DuplicateRows != 1 {
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
	service := newTestService(t)
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
	bad.DuplicateStatistics.DuplicateRows++
	if err := bad.Validate(); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("duplicate-stat error = %v", err)
	}
	bad = checkpoint
	bad.MaxChunkCandidates = bad.Segment.MaxK() + 1
	if err := bad.Validate(); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("segment search-bound error = %v", err)
	}
}
