package semantic

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type searchEncoder struct {
	descriptor PipelineDescriptor
	values     map[fts.DocID][]ChunkVector
}

func (e searchEncoder) Descriptor() PipelineDescriptor {
	return e.descriptor
}

func (e searchEncoder) Encode(_ context.Context, document Document) ([]ChunkVector, error) {
	return e.values[document.ID], nil
}

func newSearchEncoder(config Config, queryValues ...ChunkVector) searchEncoder {
	return searchEncoder{
		descriptor: PipelineDescriptor{Embedding: config.Embedding, Chunking: config.Chunking},
		values: map[fts.DocID][]ChunkVector{
			"query": queryValues,
		},
	}
}

// searchChunks exposes the internal ANN candidate stage to semantic package
// tests without restoring a public chunk-search API.
func (s *Service) searchChunks(ctx context.Context, query []float32, k int) (chunkSearchResult, error) {
	s.mu.RLock()
	published := s.published
	maxK := s.config.MaxK
	s.mu.RUnlock()
	return searchSegmentsChunks(ctx, s.calculator, published.segments, query, k, maxK, vector.SearchOptions{})
}

func TestReadViewSearchDocumentsWithOptionsEmptyView(t *testing.T) {
	config := testConfig(3, 3)
	view := newEmptyReadView(SearchPolicy{
		MaxK:                    config.MaxK,
		MaxChunkCandidates:      config.MaxChunkCandidates,
		MaxChunksPerDocumentHit: config.MaxChunksPerDocumentHit,
	})
	encoder := newSearchEncoder(config, testChunk("query", "query", 0, []float32{0, 0}))

	result, err := view.SearchDocumentsWithOptions(context.Background(), encoder, Document{ID: "query"}, 1, SearchOptions{})
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
	for _, item := range []ChunkVector{
		testChunk("doc-a", "a", 0, []float32{0, 0}),
		testChunk("doc-b", "b", 0, []float32{10, 0}),
	} {
		if err := addDocument(t, service, ctx, []ChunkVector{item}); err != nil {
			t.Fatal(err)
		}
	}
	config := service.config
	encoder := newSearchEncoder(config,
		testChunk("query", "query-a", 0, []float32{0, 0}),
		testChunk("query", "query-b", 1, []float32{10, 0}),
	)

	result, err := service.SearchDocumentsWithOptions(ctx, encoder, Document{ID: "query"}, 2, SearchOptions{})
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

func TestServiceSearchDocumentsWithOptionsTieBreaksByDocumentID(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for _, docID := range []fts.DocID{"doc-z", "doc-a"} {
		if err := addDocument(t, service, ctx, []ChunkVector{testChunk(docID, searchChunkID(docID), 0, []float32{0, 0})}); err != nil {
			t.Fatal(err)
		}
	}
	encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))

	result, err := service.SearchDocumentsWithOptions(ctx, encoder, Document{ID: "query"}, 2, SearchOptions{})
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
		if err := addDocument(t, service, ctx, []ChunkVector{testChunk(docID, searchChunkID(docID), 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))

	result, err := service.SearchDocumentsWithOptions(ctx, encoder, Document{ID: "query"}, 2, SearchOptions{CandidateChunks: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks != 2 || result.DistinctDocuments != 2 || len(result.Hits) != 2 || !result.GroupingIncomplete {
		t.Fatalf("budget-limited result = %+v", result)
	}
}

func searchChunkID(docID fts.DocID) chunk.ID {
	return chunk.ID(string(docID) + "-chunk")
}
