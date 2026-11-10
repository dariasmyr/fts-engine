package semantic

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type serviceTestEncoder struct {
	value              []float32
	documentCalls      int
	queryCalls         int
	queryDocumentCalls int
}

func (e *serviceTestEncoder) EncodeDocument(_ context.Context, document fts.Document) ([]ChunkEmbedding, error) {
	e.documentCalls++
	return e.encoded(document.ID), nil
}

func (e *serviceTestEncoder) EncodeQuery(_ context.Context, _ string, _ QueryConfig) ([]ChunkEmbedding, error) {
	e.queryCalls++
	return e.encoded("_query"), nil
}

func (e *serviceTestEncoder) EncodeQueryDocument(_ context.Context, document fts.Document, _ QueryConfig) ([]ChunkEmbedding, error) {
	e.queryDocumentCalls++
	return e.encoded(document.ID), nil
}

func (e *serviceTestEncoder) encoded(docID fts.DocID) []ChunkEmbedding {
	return []ChunkEmbedding{{
		Ref:       Ref{ID: "whole", DocID: docID, Field: fts.DefaultField, EndByte: 1},
		Embedding: append([]float32(nil), e.value...),
	}}
}

func TestServiceUsesSeparateDocumentAndQueryEncoders(t *testing.T) {
	documents := &serviceTestEncoder{value: []float32{1, 0}}
	queries := &serviceTestEncoder{value: []float32{1, 0}}
	index, err := NewIndex(serviceTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(index, documents, queries)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(t.Context(), fts.Document{ID: "doc", Fields: map[string]fts.FieldData{"body": {Text: "doc"}}}); err != nil {
		t.Fatal(err)
	}
	pending, err := service.Search(t.Context(), "query", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Hits) != 0 {
		t.Fatalf("Search() before Commit hits = %+v, want none", pending.Hits)
	}
	if err := service.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SearchDocuments(t.Context(), fts.Document{ID: "query", Fields: map[string]fts.FieldData{"body": {Text: "query"}}}, 1); err != nil {
		t.Fatal(err)
	}
	committed, err := service.Search(t.Context(), "query", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed.Hits) != 1 || committed.Hits[0].DocID != "doc" {
		t.Fatalf("Search() after Commit hits = %+v, want doc", committed.Hits)
	}
	if documents.documentCalls != 1 || queries.queryCalls != 2 || queries.queryDocumentCalls != 1 {
		t.Fatalf(
			"encoder calls: documents=%d queries=%d query documents=%d",
			documents.documentCalls,
			queries.queryCalls,
			queries.queryDocumentCalls,
		)
	}
}

func serviceTestConfig() SemanticConfig {
	return SemanticConfig{
		Model: ModelConfig{ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 16},
		Index: IndexConfig{
			Metric: vector.MetricCosine,
			Limits: LimitsConfig{MaxLiveVectors: 8, MaxChunksPerDocument: 2},
			Search: SearchConfig{MaxResults: 2, MaxCandidateChunks: 8, MaxChunksPerResult: 1},
		},
		Chunking: ChunkingConfig{Kind: ChunkingKindCustom},
	}
}
