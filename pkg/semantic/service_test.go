package semantic

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	vectorflat "github.com/dariasmyr/fts-engine/pkg/vector/flat"
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

func testChunk(docID fts.DocID, id chunk.ID, ordinal uint32, value []float32) ChunkVector {
	return ChunkVector{
		Ref:    chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, Ordinal: ordinal, StartByte: uint64(ordinal * 10), EndByte: uint64(ordinal*10 + 10)},
		Vector: value,
	}
}

func TestLifecycleChunkSearchGroupingAndStatistics(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.AddDocument(ctx, []ChunkVector{
		testChunk("doc-a", "a-1", 0, []float32{0, 0}),
		testChunk("doc-a", "a-2", 1, []float32{10, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc-b", "b-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}

	chunks, err := service.SearchChunks(ctx, []float32{0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks.Hits) != 3 || chunks.Hits[0].Ref.DocID != "doc-a" || chunks.Hits[1].Ref.DocID != "doc-b" {
		t.Fatalf("chunk hits = %+v", chunks.Hits)
	}
	documents, err := service.SearchDocuments(ctx, []float32{0, 0}, 2)
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
	if stats.Documents != 2 || stats.PhysicalVectors != 3 || stats.LiveVectors != 3 || stats.StaleVectors != 0 || stats.HighWatermark != 3 {
		t.Fatalf("statistics = %+v", stats)
	}

	badReplacement := []ChunkVector{testChunk("doc-a", "bad", 0, []float32{1})}
	if err := service.ReplaceDocument(ctx, badReplacement); !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Fatalf("replacement error = %v", err)
	}
	nearest, err := service.SearchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || nearest.Hits[0].Ref.ID != "a-1" {
		t.Fatalf("old version not preserved: %+v, %v", nearest, err)
	}
	if err := service.ReplaceDocument(ctx, []ChunkVector{testChunk("doc-a", "a-new", 0, []float32{3, 0})}); err != nil {
		t.Fatal(err)
	}
	nearest, err = service.SearchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if nearest.Hits[0].Ref.DocID != "doc-b" || nearest.Hits[1].Ref.ID != "a-new" {
		t.Fatalf("stale chunks leaked: %+v", nearest.Hits)
	}
	if !service.DeleteDocument("doc-b") || service.DeleteDocument("doc-b") {
		t.Fatal("DeleteDocument result mismatch")
	}
	stats = service.Statistics()
	if stats.Documents != 1 || stats.PhysicalVectors != 4 || stats.LiveVectors != 1 || stats.StaleVectors != 3 || stats.HighWatermark != 4 {
		t.Fatalf("post-update statistics = %+v", stats)
	}
}

func TestDocumentValidationAndExplicitMutationErrors(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.AddDocument(ctx, nil); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("empty batch error = %v", err)
	}
	mixed := []ChunkVector{
		testChunk("doc-a", "one", 0, []float32{0, 0}),
		testChunk("doc-b", "two", 1, []float32{1, 0}),
	}
	if err := service.AddDocument(ctx, mixed); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("mixed batch error = %v", err)
	}
	duplicate := []ChunkVector{
		testChunk("doc-a", "same", 0, []float32{0, 0}),
		testChunk("doc-a", "same", 1, []float32{1, 0}),
	}
	if err := service.AddDocument(ctx, duplicate); !errors.Is(err, ErrDuplicateChunkID) {
		t.Fatalf("duplicate chunk error = %v", err)
	}
	invalidRange := testChunk("doc-a", "bad-range", 0, []float32{0, 0})
	invalidRange.Ref.StartByte, invalidRange.Ref.EndByte = 10, 2
	if err := service.AddDocument(ctx, []ChunkVector{invalidRange}); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("range error = %v", err)
	}
	valid := []ChunkVector{testChunk("doc-a", "one", 0, []float32{0, 0})}
	if err := service.AddDocument(ctx, valid); err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(ctx, valid); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("duplicate document error = %v", err)
	}
	if err := service.ReplaceDocument(ctx, []ChunkVector{testChunk("missing", "one", 0, []float32{0, 0})}); !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("missing replacement error = %v", err)
	}
	if stats := service.Statistics(); stats.Documents != 1 || stats.PhysicalVectors != 1 || stats.HighWatermark != 1 {
		t.Fatalf("failed operations changed state: %+v", stats)
	}
}

func TestVectorIDsAreMonotonicAndExhaustionIsAtomic(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc-a", "a", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	service.DeleteDocument("doc-a")
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if stats := service.Statistics(); stats.HighWatermark != 2 || stats.LiveVectors != 1 || stats.StaleVectors != 1 {
		t.Fatalf("statistics = %+v", stats)
	}

	config := testConfig(2, 2)
	config.InitialVectorIDHighWatermark = VectorID(math.MaxUint64 - 1)
	exhausted, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	err = exhausted.AddDocument(ctx, []ChunkVector{
		testChunk("doc", "one", 0, []float32{0, 0}),
		testChunk("doc", "two", 1, []float32{1, 0}),
	})
	if !errors.Is(err, ErrVectorIDExhausted) {
		t.Fatalf("exhaustion error = %v", err)
	}
	if stats := exhausted.Statistics(); stats.PhysicalVectors != 0 || stats.HighWatermark != VectorID(math.MaxUint64-1) {
		t.Fatalf("exhausted state changed: %+v", stats)
	}
}

func TestCapacityFailedReplacementKeepsOldVersion(t *testing.T) {
	config := testConfig(2, 2)
	config.MaxVectors = 2
	config.MaxChunksPerDocument = 2
	config.MaxChunksPerDocumentHit = 2
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	err = service.ReplaceDocument(ctx, []ChunkVector{
		testChunk("doc", "new-1", 0, []float32{5, 0}),
		testChunk("doc", "new-2", 1, []float32{6, 0}),
	})
	if !errors.Is(err, vectorflat.ErrCapacityExceeded) {
		t.Fatalf("replacement error = %v", err)
	}
	result, err := service.SearchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Ref.ID != "old" {
		t.Fatalf("old version was not preserved: %+v", result.Hits)
	}
	if stats := service.Statistics(); stats.PhysicalVectors != 1 || stats.LiveVectors != 1 || stats.HighWatermark != 1 {
		t.Fatalf("failed replacement changed state: %+v", stats)
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
	if err := service.AddDocument(context.Background(), []ChunkVector{{Ref: whole.Ref, Vector: []float32{0, 0}}}); err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(context.Background(), []ChunkVector{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	result, err := service.SearchDocuments(context.Background(), []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Chunks[0].Ref.ID != chunk.WholeID || !result.GroupingIncomplete || result.CandidateChunks != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestExactGroupingUsesOneCompleteFlatScan(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.AddDocument(ctx, []ChunkVector{
		testChunk("doc-a", "a-1", 0, []float32{0, 0}),
		testChunk("doc-a", "a-2", 1, []float32{1, 0}),
		testChunk("doc-a", "a-3", 2, []float32{2, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	for i, docID := range []fts.DocID{"doc-b", "doc-c", "doc-d"} {
		if err := service.AddDocument(ctx, []ChunkVector{testChunk(docID, "whole", 0, []float32{float32(i + 3), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := service.SearchDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupingIncomplete || result.CandidateChunks != 6 || result.DistinctDocuments != 4 {
		t.Fatalf("grouping result = %+v", result)
	}
	if result.Stats.VisitedNodes != 6 || result.Stats.DistanceComputations != 6 {
		t.Fatalf("exact stats = %+v, want one complete six-row scan", result.Stats)
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
			errorsCh <- service.AddDocument(ctx, []ChunkVector{testChunk("shared", chunk.ID("version"), 0, []float32{float32(i), 0})})
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
			_, err := service.SearchChunks(ctx, []float32{0, 0}, 1)
			if err != nil {
				searchErrors <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 20 {
			if err := service.ReplaceDocument(ctx, []ChunkVector{testChunk("shared", "version", 0, []float32{float32(i), 0})}); err != nil {
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

func TestConcurrentReplaceAndDeleteAreLinearizable(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.AddDocument(ctx, []ChunkVector{testChunk("doc", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	replaceResult := make(chan error, 1)
	deleteResult := make(chan bool, 1)
	go func() {
		<-start
		replaceResult <- service.ReplaceDocument(ctx, []ChunkVector{testChunk("doc", "new", 0, []float32{1, 0})})
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
