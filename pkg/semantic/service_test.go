package semantic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func testConfig(maxK, maxCandidates int) Config {
	return Config{
		Space: SpaceDescriptor{
			ID:                  "test-space-v1",
			Dimensions:          2,
			Metric:              vector.MetricL2Squared,
			Normalization:       vector.NormalizationNone,
			VectorFormatVersion: 1,
		},
		Chunking:                ChunkingDescriptor{ID: "test-chunks-v1"},
		MaxVectors:              200,
		MaxChunksPerDocument:    20,
		MaxK:                    maxK,
		MaxChunkCandidates:      maxCandidates,
		MaxChunksPerDocumentHit: 3,
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	service, err := New(testConfig(10, 100))
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func addDocument(t *testing.T, service *Service, ctx context.Context, batch []ChunkVector) error {
	t.Helper()
	if len(batch) == 0 {
		return service.addEncodedDocument(ctx, "", batch)
	}
	if err := service.addEncodedDocument(ctx, batch[0].Ref.DocID, batch); err != nil {
		return err
	}
	return service.Flush(ctx)
}

func replaceDocument(t *testing.T, service *Service, ctx context.Context, batch []ChunkVector) error {
	t.Helper()
	if len(batch) == 0 {
		return service.replaceEncodedDocument(ctx, "", batch)
	}
	if err := service.replaceEncodedDocument(ctx, batch[0].Ref.DocID, batch); err != nil {
		return err
	}
	return service.Flush(ctx)
}

func deleteDocument(t *testing.T, service *Service, docID fts.DocID) bool {
	t.Helper()
	deleted := service.DeleteDocument(docID)
	if deleted {
		if err := service.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return deleted
}

func testChunk(docID fts.DocID, id chunk.ID, ordinal uint32, value []float32) ChunkVector {
	return ChunkVector{
		Ref:    chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, Ordinal: ordinal, StartByte: uint64(ordinal * 10), EndByte: uint64(ordinal*10 + 10)},
		Vector: value,
	}
}

func TestLifecycleChunkSearchGroupingAndStatistics(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{
		testChunk("doc-a", "a-1", 0, []float32{0, 0}),
		testChunk("doc-a", "a-2", 1, []float32{10, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-b", "b-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}

	chunks, err := service.searchChunks(ctx, []float32{0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks.Hits) != 3 || chunks.Hits[0].Ref.DocID != "doc-a" || chunks.Hits[1].Ref.DocID != "doc-b" {
		t.Fatalf("chunk hits = %+v", chunks.Hits)
	}
	documents, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if documents.GroupingIncomplete || len(documents.Hits) != 2 || documents.Hits[0].DocID != "doc-a" || documents.Hits[1].DocID != "doc-b" {
		t.Fatalf("document result = %+v", documents)
	}
	if documents.Hits[0].Distance != 0 || len(documents.Hits[0].Chunks) != 2 {
		t.Fatalf("doc-a grouping = %+v", documents.Hits[0])
	}
	stats := service.Statistics()
	if stats.Documents != 2 || stats.PhysicalVectors != 3 || stats.LiveVectors != 3 || stats.StaleVectors != 0 || stats.MaxAllocatedVectorID != 3 {
		t.Fatalf("statistics = %+v", stats)
	}

	badReplacement := []ChunkVector{testChunk("doc-a", "bad", 0, []float32{1})}
	if err := service.replaceEncodedDocument(ctx, "doc-a", badReplacement); !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Fatalf("replacement error = %v", err)
	}
	nearest, err := service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || nearest.Hits[0].Ref.ID != "a-1" {
		t.Fatalf("old version not preserved: %+v, %v", nearest, err)
	}
	if err := replaceDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "a-new", 0, []float32{3, 0})}); err != nil {
		t.Fatal(err)
	}
	nearest, err = service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if nearest.Hits[0].Ref.DocID != "doc-b" || nearest.Hits[1].Ref.ID != "a-new" {
		t.Fatalf("stale chunks leaked: %+v", nearest.Hits)
	}
	if !deleteDocument(t, service, "doc-b") || service.DeleteDocument("doc-b") {
		t.Fatal("DeleteDocument result mismatch")
	}
	stats = service.Statistics()
	if stats.Documents != 1 || stats.PhysicalVectors != 4 || stats.LiveVectors != 1 || stats.StaleVectors != 3 || stats.MaxAllocatedVectorID != 4 {
		t.Fatalf("post-update statistics = %+v", stats)
	}
}

func TestServiceSearchUsesImmutableHNSWSegmentSet(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 3 {
		docID := fts.DocID(fmt.Sprintf("doc-%d", i))
		if err := service.addEncodedDocument(ctx, docID, []ChunkVector{testChunk(docID, chunk.ID(fmt.Sprintf("chunk-%d", i)), 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	service.mu.RLock()
	segments := make([]*Segment, 0, len(service.published.segments))
	for _, view := range service.published.segments {
		segments = append(segments, view.segment)
	}
	service.mu.RUnlock()
	if len(segments) != 1 {
		t.Fatalf("segments = %d, want one flushed segment", len(segments))
	}
	for _, segment := range segments {
		if segment.Searcher() == nil || segment.Vectors() == nil || segment.Kind() != SegmentKindChunkHNSW {
			t.Fatalf("segment is not HNSW-backed: %+v", segment)
		}
	}
	result, err := service.searchChunks(ctx, []float32{0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 3 {
		t.Fatalf("hits = %d, want 3", len(result.Hits))
	}
}

func TestMultiSegmentSearchMergesChunksAndDocuments(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-far", "far", 0, []float32{10, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-near", "near", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	chunks, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks.Hits) != 2 || chunks.Hits[0].Ref.DocID != "doc-near" || chunks.Hits[1].Ref.DocID != "doc-far" {
		t.Fatalf("merged chunk hits = %+v", chunks.Hits)
	}

	documents, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if documents.GroupingIncomplete || len(documents.Hits) != 2 || documents.Hits[0].DocID != "doc-near" || documents.Hits[1].DocID != "doc-far" {
		t.Fatalf("merged document hits = %+v", documents)
	}
}

func TestMultiSegmentSearchUsesDeterministicComponentTieOrdering(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-first", "first", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-second", "second", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	result, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.Hits[0].Ref.DocID != "doc-first" || result.Hits[1].Ref.DocID != "doc-second" {
		t.Fatalf("tie-ordered hits = %+v", result.Hits)
	}
}

func TestMultiSegmentSearchAppliesPublishedStaleFilters(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-old", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-live", "live", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if !service.DeleteDocument("doc-old") {
		t.Fatal("DeleteDocument returned false")
	}

	beforeFlush, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeFlush.Hits) != 2 {
		t.Fatalf("pre-publication hits = %+v", beforeFlush.Hits)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	afterFlush, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterFlush.Hits) != 1 || afterFlush.Hits[0].Ref.DocID != "doc-live" {
		t.Fatalf("post-publication hits = %+v", afterFlush.Hits)
	}
}

func TestMultiSegmentSearchPropagatesIncompleteANNState(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 3 {
		docID := fts.DocID(fmt.Sprintf("doc-%d", i))
		if err := service.addEncodedDocument(ctx, docID, []ChunkVector{testChunk(docID, chunk.ID(fmt.Sprintf("chunk-%d", i)), 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := service.searchEncodedDocumentsWithOptions(ctx, []float32{0, 0}, 1, SearchOptions{
		CandidateChunks: 3,
		VisitLimit:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.GroupingIncomplete || result.Stats.Termination != vector.TerminationVisitLimit {
		t.Fatalf("incomplete grouping result = %+v", result)
	}
}

func TestMutationsBecomeVisibleAfterFlush(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	old := []ChunkVector{testChunk("doc", "old", 0, []float32{0, 0})}
	if err := service.addEncodedDocument(ctx, "doc", old); err != nil {
		t.Fatal(err)
	}
	result, err := service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("unflushed add is visible: %+v", result.Hits)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.ID != "old" {
		t.Fatalf("flushed add result = %+v, %v", result, err)
	}

	if err := service.replaceEncodedDocument(ctx, "doc", []ChunkVector{testChunk("doc", "new", 0, []float32{10, 0})}); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.ID != "old" {
		t.Fatalf("unflushed replacement result = %+v, %v", result, err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.ID != "new" {
		t.Fatalf("flushed replacement result = %+v, %v", result, err)
	}

	if !service.DeleteDocument("doc") {
		t.Fatal("DeleteDocument returned false")
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.ID != "new" {
		t.Fatalf("unflushed deletion result = %+v, %v", result, err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 0 {
		t.Fatalf("flushed deletion result = %+v, %v", result, err)
	}
}

func TestCanceledFlushDoesNotPublishPendingBatch(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "doc", []ChunkVector{testChunk("doc", "chunk", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := service.Flush(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush error = %v", err)
	}
	result, err := service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("canceled flush published rows: %+v", result.Hits)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 {
		t.Fatalf("retry flush result = %+v, %v", result, err)
	}
}

func TestCompactRemovesStaleVectorsAndPreservesStableIDs(t *testing.T) {
	config := testConfig(3, 3)
	config.MaxVectors = 3
	config.MaxChunksPerDocument = 3
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "a-old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := replaceDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "a-new", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	before, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if stats := service.Statistics(); stats.PhysicalVectors != 2 || stats.LiveVectors != 2 || stats.StaleVectors != 0 || stats.MaxAllocatedVectorID != 3 {
		t.Fatalf("compacted statistics = %+v", stats)
	}
	after, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after.Hits, before.Hits) || after.Stats.RejectedNodes != 0 {
		t.Fatalf("search changed after compaction\nbefore=%+v\nafter=%+v", before, after)
	}
	checkpoint, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snapshotRowIDs(checkpoint), []VectorID{2, 3}) {
		t.Fatalf("compacted row IDs = %v", snapshotRowIDs(checkpoint))
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-c", "c", 0, []float32{3, 0})}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err = service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snapshotRowIDs(checkpoint), []VectorID{2, 3, 4}) || checkpoint.MaxAllocatedVectorID != 4 {
		t.Fatalf("post-compaction row IDs/max allocated ID = %v/%d", snapshotRowIDs(checkpoint), checkpoint.MaxAllocatedVectorID)
	}
}

func TestCanceledCompactLeavesServiceUnchanged(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := replaceDocument(t, service, ctx, []ChunkVector{testChunk("doc", "new", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	want := service.Statistics()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := service.Compact(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled compaction error = %v", err)
	}
	if got := service.Statistics(); got != want {
		t.Fatalf("canceled compaction changed service: got %+v, want %+v", got, want)
	}
}

func TestCompactEmptyServiceReclaimsCapacity(t *testing.T) {
	config := testConfig(1, 1)
	config.MaxVectors = 1
	config.MaxChunksPerDocument = 1
	config.MaxChunksPerDocumentHit = 1
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("old", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if !deleteDocument(t, service, "old") {
		t.Fatal("DeleteDocument returned false")
	}
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if stats := service.Statistics(); stats.PhysicalVectors != 0 || stats.LiveVectors != 0 || stats.StaleVectors != 0 || stats.MaxAllocatedVectorID != 1 {
		t.Fatalf("empty compacted statistics = %+v", stats)
	}
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("new", "new", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := service.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(snapshotRowIDs(checkpoint), []VectorID{2}) || checkpoint.MaxAllocatedVectorID != 2 {
		t.Fatalf("reclaimed row IDs/max allocated ID = %v/%d", snapshotRowIDs(checkpoint), checkpoint.MaxAllocatedVectorID)
	}
}

func TestDocumentValidationAndExplicitMutationErrors(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "", nil); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("empty batch error = %v", err)
	}
	mixed := []ChunkVector{
		testChunk("doc-a", "one", 0, []float32{0, 0}),
		testChunk("doc-b", "two", 1, []float32{1, 0}),
	}
	if err := service.addEncodedDocument(ctx, "doc-a", mixed); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("mixed batch error = %v", err)
	}
	invalidRange := testChunk("doc-a", "bad-range", 0, []float32{0, 0})
	invalidRange.Ref.StartByte, invalidRange.Ref.EndByte = 10, 2
	if err := service.addEncodedDocument(ctx, "doc-a", []ChunkVector{invalidRange}); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("range error = %v", err)
	}
	valid := []ChunkVector{testChunk("doc-a", "one", 0, []float32{0, 0})}
	if err := addDocument(t, service, ctx, valid); err != nil {
		t.Fatal(err)
	}
	if err := service.addEncodedDocument(ctx, "doc-a", valid); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("duplicate document error = %v", err)
	}
	if err := service.replaceEncodedDocument(ctx, "missing", []ChunkVector{testChunk("missing", "one", 0, []float32{0, 0})}); !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("missing replacement error = %v", err)
	}
	if stats := service.Statistics(); stats.Documents != 1 || stats.PhysicalVectors != 1 || stats.MaxAllocatedVectorID != 1 {
		t.Fatalf("failed operations changed state: %+v", stats)
	}
}

func TestVectorIDsAreMonotonicAndExhaustionIsAtomic(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-a", "a", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	deleteDocument(t, service, "doc-a")
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if stats := service.Statistics(); stats.MaxAllocatedVectorID != 2 || stats.LiveVectors != 1 || stats.StaleVectors != 1 {
		t.Fatalf("statistics = %+v", stats)
	}

	config := testConfig(2, 2)
	config.InitialMaxAllocatedVectorID = VectorID(math.MaxUint64 - 1)
	exhausted, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	err = exhausted.addEncodedDocument(ctx, "doc", []ChunkVector{
		testChunk("doc", "one", 0, []float32{0, 0}),
		testChunk("doc", "two", 1, []float32{1, 0}),
	})
	if !errors.Is(err, ErrVectorIDExhausted) {
		t.Fatalf("exhaustion error = %v", err)
	}
	if stats := exhausted.Statistics(); stats.PhysicalVectors != 0 || stats.MaxAllocatedVectorID != VectorID(math.MaxUint64-1) {
		t.Fatalf("exhausted state changed: %+v", stats)
	}
}

func TestReplacementUsesLiveCapacity(t *testing.T) {
	config := testConfig(2, 2)
	config.MaxVectors = 2
	config.MaxChunksPerDocument = 2
	config.MaxChunksPerDocumentHit = 2
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{testChunk("doc", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	err = replaceDocument(t, service, ctx, []ChunkVector{
		testChunk("doc", "new-1", 0, []float32{5, 0}),
		testChunk("doc", "new-2", 1, []float32{6, 0}),
	})
	if err != nil {
		t.Fatalf("replacement error = %v", err)
	}
	result, err := service.searchChunks(ctx, []float32{5, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.Hits[0].Ref.ID != "new-1" {
		t.Fatalf("replacement result = %+v", result.Hits)
	}
	if stats := service.Statistics(); stats.PhysicalVectors != 3 || stats.LiveVectors != 2 || stats.StaleVectors != 1 || stats.MaxAllocatedVectorID != 3 {
		t.Fatalf("replacement statistics = %+v", stats)
	}
}

func TestWholeDocumentAndGroupingBudget(t *testing.T) {
	config := testConfig(1, 1)
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	whole, err := chunk.Whole("doc-a", fts.DefaultField, "short text")
	if err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, context.Background(), []ChunkVector{{Ref: whole.Ref, Vector: []float32{0, 0}}}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, context.Background(), []ChunkVector{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	result, err := service.searchEncodedDocuments(context.Background(), []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Chunks[0].Ref.ID != chunk.WholeID || !result.GroupingIncomplete || result.CandidateChunks != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestCompleteGroupingUsesAllCandidateChunks(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []ChunkVector{
		testChunk("doc-a", "a-1", 0, []float32{0, 0}),
		testChunk("doc-a", "a-2", 1, []float32{1, 0}),
		testChunk("doc-a", "a-3", 2, []float32{2, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	for i, docID := range []fts.DocID{"doc-b", "doc-c", "doc-d"} {
		if err := addDocument(t, service, ctx, []ChunkVector{testChunk(docID, "whole", 0, []float32{float32(i + 3), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupingIncomplete || result.CandidateChunks != 6 || result.DistinctDocuments != 4 {
		t.Fatalf("grouping result = %+v", result)
	}
	if result.Stats.VisitedNodes != 6 || result.Stats.DistanceComputations != 6 {
		t.Fatalf("complete stats = %+v, want six candidate distances", result.Stats)
	}
}

func TestDocumentGroupingCandidateBudgetMayExceedPublicMaxK(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 11 {
		docID := fts.DocID(fmt.Sprintf("doc-%02d", i))
		if err := addDocument(t, service, ctx, []ChunkVector{testChunk(docID, "whole", 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupingIncomplete || result.CandidateChunks != 11 || result.DistinctDocuments != 11 {
		t.Fatalf("grouping result = %+v", result)
	}
	if _, err := service.searchChunks(ctx, []float32{0, 0}, 11); !errors.Is(err, vector.ErrInvalidK) {
		t.Fatalf("public chunk search error = %v", err)
	}
}

func TestDocumentSearchCandidateBudgetIsRequestScoped(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 4 {
		docID := fts.DocID(fmt.Sprintf("doc-%d", i))
		if err := addDocument(t, service, ctx, []ChunkVector{testChunk(docID, "whole", 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}

	result, err := service.searchEncodedDocumentsWithOptions(ctx, []float32{0, 0}, 2, SearchOptions{CandidateChunks: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks != 2 || result.DistinctDocuments != 2 || !result.GroupingIncomplete {
		t.Fatalf("limited grouping result = %+v", result)
	}
	if len(result.Hits) != 2 || result.Hits[0].DocID != "doc-0" || result.Hits[1].DocID != "doc-1" {
		t.Fatalf("limited grouping hits = %+v", result.Hits)
	}

	result, err = service.searchEncodedDocumentsWithOptions(ctx, []float32{0, 0}, 2, SearchOptions{CandidateChunks: 4})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks != 4 || result.DistinctDocuments != 4 || result.GroupingIncomplete {
		t.Fatalf("expanded grouping result = %+v", result)
	}
}

func TestDocumentSearchClampsCandidateBudgetToConfiguredMaximum(t *testing.T) {
	service := newTestService(t)
	if err := addDocument(t, service, context.Background(), []ChunkVector{testChunk("doc", "whole", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	result, err := service.searchEncodedDocumentsWithOptions(context.Background(), []float32{0, 0}, 1, SearchOptions{CandidateChunks: 101})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks != 1 || result.GroupingIncomplete {
		t.Fatalf("clamped grouping result = %+v", result)
	}
}

func TestConcurrentSameDocumentAddAndSearchReplace(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	const writers = 8
	var wg sync.WaitGroup
	errorsCh := make(chan error, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errorsCh <- service.addEncodedDocument(ctx, "shared", []ChunkVector{testChunk("shared", chunk.ID("version"), 0, []float32{float32(i), 0})})
		}(i)
	}
	wg.Wait()
	close(errorsCh)
	successes := 0
	for err := range errorsCh {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrDocumentExists) {
			t.Fatalf("AddDocument error = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful adds = %d, want 1", successes)
	}

	searchErrors := make(chan error, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 20 {
			_, err := service.searchChunks(ctx, []float32{0, 0}, 1)
			if err != nil {
				searchErrors <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 20 {
			if err := service.replaceEncodedDocument(ctx, "shared", []ChunkVector{testChunk("shared", "version", 0, []float32{float32(i), 0})}); err != nil {
				searchErrors <- err
				return
			}
		}
	}()
	wg.Wait()
	close(searchErrors)
	for err := range searchErrors {
		t.Fatal(err)
	}
	if stats := service.Statistics(); stats.Documents != 1 || stats.LiveVectors != 1 || stats.PhysicalVectors != 21 {
		t.Fatalf("concurrent statistics = %+v", stats)
	}
}

func TestConcurrentSnapshotAndReplacementRemainCoherent(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "doc", []ChunkVector{testChunk("doc", "chunk-0", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 4)
	var wait sync.WaitGroup
	for worker := range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 25 {
				checkpoint, err := service.Snapshot(ctx)
				if err != nil {
					errCh <- err
					return
				}
				if len(checkpoint.Segment.Rows()) != 1 || checkpoint.Segment.Len() != 1 || checkpoint.Segment.Rows()[0].Chunk.DocID != "doc" {
					errCh <- fmt.Errorf("worker %d: incoherent checkpoint: %+v", worker, checkpoint)
					return
				}
				var version int
				if _, err := fmt.Sscanf(string(checkpoint.Segment.Rows()[0].Chunk.ID), "chunk-%d", &version); err != nil {
					errCh <- fmt.Errorf("worker %d: parse checkpoint row: %w", worker, err)
					return
				}
				value := make([]float32, 2)
				if err := checkpoint.Segment.Vectors().ReadVectorInto(ctx, 0, value); err != nil || value[0] != float32(version) {
					errCh <- fmt.Errorf("worker %d: row/vector mismatch: %+v/%v/%v", worker, checkpoint.Segment.Rows()[0], value, err)
					return
				}
			}
		}()
	}
	for version := 1; version <= 50; version++ {
		if err := service.replaceEncodedDocument(ctx, "doc", []ChunkVector{testChunk("doc", chunk.ID(fmt.Sprintf("chunk-%d", version)), 0, []float32{float32(version), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	wait.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestConcurrentReplaceAndDeleteAreLinearizable(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "doc", []ChunkVector{testChunk("doc", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	replaceResult := make(chan error, 1)
	deleteResult := make(chan bool, 1)
	go func() {
		<-start
		replaceResult <- service.replaceEncodedDocument(ctx, "doc", []ChunkVector{testChunk("doc", "new", 0, []float32{1, 0})})
	}()
	go func() {
		<-start
		deleteResult <- service.DeleteDocument("doc")
	}()
	close(start)
	replaceErr := <-replaceResult
	deleted := <-deleteResult
	if !deleted {
		t.Fatal("DeleteDocument returned false")
	}
	if replaceErr != nil && !errors.Is(replaceErr, ErrDocumentNotFound) {
		t.Fatalf("ReplaceDocument error = %v", replaceErr)
	}
	if stats := service.Statistics(); stats.Documents != 0 || stats.LiveVectors != 0 {
		t.Fatalf("final state is not deleted: %+v", stats)
	}
}
