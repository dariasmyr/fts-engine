package semantic

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type searchEncoder struct {
	descriptor PipelineDescriptor
	values     map[fts.DocID][]EncodedChunk
}

type countingSearchEncoder struct {
	searchEncoder
	calls int
}

type cancelingSearchEncoder struct {
	searchEncoder
	cancel context.CancelFunc
}

func (e cancelingSearchEncoder) Encode(ctx context.Context, document fts.Document) ([]EncodedChunk, error) {
	queries, err := e.searchEncoder.Encode(ctx, document)
	e.cancel()
	return queries, err
}

func (e *countingSearchEncoder) Encode(ctx context.Context, document fts.Document) ([]EncodedChunk, error) {
	e.calls++
	return e.searchEncoder.Encode(ctx, document)
}

func (e searchEncoder) Descriptor() PipelineDescriptor {
	return e.descriptor
}

func (e searchEncoder) Encode(_ context.Context, document fts.Document) ([]EncodedChunk, error) {
	return e.values[document.ID], nil
}

func newSearchEncoder(config Config, queryValues ...EncodedChunk) searchEncoder {
	return searchEncoder{
		descriptor: PipelineDescriptor{Embedding: config.Embedding, Chunking: config.Chunking},
		values: map[fts.DocID][]EncodedChunk{
			"query": queryValues,
		},
	}
}

// searchChunks exposes the internal ANN candidate stage to semantic package
// tests without restoring a public chunk-search API.
func (s *Service) searchChunks(ctx context.Context, query []float32, k int) (chunkSearchResult, error) {
	published := s.ReadView()
	return searchSegmentsChunks(ctx, published.calculator, published.segments, query, k, published.maxDocumentsPerSearch, vector.SearchOptions{})
}

func TestReadViewSearchDocumentsWithOptionsEmptyView(t *testing.T) {
	service := newTestService(t)
	config := service.config
	view := service.ReadView()
	encoder := newSearchEncoder(config, testChunk("query", "query", 0, []float32{0, 0}))

	result, err := view.SearchDocumentsWithOptions(context.Background(), encoder, fts.Document{ID: "query"}, 1, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Hits == nil || len(result.Hits) != 0 || result.DistinctDocuments != 0 || result.GroupingIncomplete {
		t.Fatalf("empty view result = %+v", result)
	}
}

func TestServiceSearchDocumentsWithOptionsMergesQueries(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for _, item := range []EncodedChunk{
		testChunk("doc-a", "a", 0, []float32{0, 0}),
		testChunk("doc-b", "b", 0, []float32{10, 0}),
	} {
		if err := addDocument(t, service, ctx, []EncodedChunk{item}); err != nil {
			t.Fatal(err)
		}
	}
	config := service.config
	encoder := newSearchEncoder(config,
		testChunk("query", "query-a", 0, []float32{0, 0}),
		testChunk("query", "query-b", 1, []float32{10, 0}),
	)

	result, err := service.SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 2, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.DistinctDocuments != 2 || len(result.Hits) != 2 || result.GroupingIncomplete {
		t.Fatalf("merged result = %+v", result)
	}
	if result.Hits[0].DocID != "doc-a" || result.Hits[1].DocID != "doc-b" {
		t.Fatalf("merged hit order = %+v", result.Hits)
	}
	if result.Hits[0].Distance != 0 || result.Hits[1].Distance != 0 {
		t.Fatalf("merged best distances = %+v", result.Hits)
	}
}

func TestServiceSearchDocumentsWithOptionsMergesExplanatoryChunks(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{
		testChunk("doc", "near-first", 0, []float32{0, 0}),
		testChunk("doc", "near-second", 1, []float32{10, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	encoder := newSearchEncoder(service.config,
		testChunk("query", "query-first", 0, []float32{0, 0}),
		testChunk("query", "query-second", 1, []float32{10, 0}),
	)

	result, err := service.SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 1, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || len(result.Hits[0].Chunks) != 2 {
		t.Fatalf("merged result = %+v", result)
	}
	if result.Hits[0].Chunks[0].Ref.ID != "near-first" || result.Hits[0].Chunks[1].Ref.ID != "near-second" ||
		result.Hits[0].Chunks[0].Distance != 0 || result.Hits[0].Chunks[1].Distance != 0 {
		t.Fatalf("merged explanatory chunks = %+v", result.Hits[0].Chunks)
	}
}

func TestServiceSearchDocumentsWithOptionsUsesRequestWideBudgets(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 4 {
		docID := fts.DocID("doc-" + string(rune('a'+i)))
		if err := addDocument(t, service, ctx, []EncodedChunk{testChunk(docID, searchChunkID(docID), 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	encoder := newSearchEncoder(service.config,
		testChunk("query", "query-first", 0, []float32{0, 0}),
		testChunk("query", "query-second", 1, []float32{3, 0}),
	)

	result, err := service.SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 2, SearchOptions{CandidateChunks: 2, VisitLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks > 2 || result.Stats.VisitedNodes > 2 || !result.GroupingIncomplete {
		t.Fatalf("request-wide budgets not enforced: %+v", result)
	}
}

func TestReadViewSearchDocumentsRejectsTooManyQueryChunks(t *testing.T) {
	config := testConfig(2, 4)
	config.Limits.MaxChunksPerDocument = 1
	config.Limits.MaxChunksPerDocumentHit = 1
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	encoder := newSearchEncoder(service.config,
		testChunk("query", "query-first", 0, []float32{0, 0}),
		testChunk("query", "query-second", 1, []float32{1, 0}),
	)

	_, err = service.SearchDocuments(context.Background(), encoder, fts.Document{ID: "query"}, 1)
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("SearchDocuments error = %v, want %v", err, ErrInvalidQuery)
	}
}

func TestServiceSearchDocumentsWithOptionsTieBreaksByDocumentID(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for _, docID := range []fts.DocID{"doc-z", "doc-a"} {
		if err := addDocument(t, service, ctx, []EncodedChunk{testChunk(docID, searchChunkID(docID), 0, []float32{0, 0})}); err != nil {
			t.Fatal(err)
		}
	}
	encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))

	result, err := service.SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 2, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.Hits[0].DocID != "doc-a" || result.Hits[1].DocID != "doc-z" {
		t.Fatalf("tie-ordered result = %+v", result.Hits)
	}
}

func TestServiceSearchDocumentsWithOptionsReportsCandidateBudgetIncomplete(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 3 {
		docID := fts.DocID("doc-" + string(rune('a'+i)))
		if err := addDocument(t, service, ctx, []EncodedChunk{testChunk(docID, searchChunkID(docID), 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))

	result, err := service.SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 2, SearchOptions{CandidateChunks: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks != 2 || result.DistinctDocuments != 2 || len(result.Hits) != 2 || !result.GroupingIncomplete {
		t.Fatalf("budget-limited result = %+v", result)
	}
}

func TestPublicSearchReportsDistinctDocumentsBeforeResultTruncation(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 4 {
		docID := fts.DocID("doc-" + string(rune('a'+i)))
		if err := addDocument(t, service, ctx, []EncodedChunk{testChunk(docID, searchChunkID(docID), 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))
	result, err := service.SearchDocuments(ctx, encoder, fts.Document{ID: "query"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.DistinctDocuments != 4 || result.CandidateChunks != 4 {
		t.Fatalf("public result = %+v", result)
	}
}

func TestServiceAndReadViewUseIdenticalDocumentSearch(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc", "chunk", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))
	fromService, err := service.SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 1, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fromView, err := service.ReadView().SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 1, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fromService, fromView) {
		t.Fatalf("service/view mismatch\nservice=%+v\nview=%+v", fromService, fromView)
	}
}

func searchChunkID(docID fts.DocID) chunk.ID {
	return chunk.ID(string(docID) + "-chunk")
}
