package semantic

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type serviceTestEncoder struct {
	schema             Schema
	value              []float32
	documentCalls      int
	queryCalls         int
	queryDocumentCalls int
}

type serviceTestRetrievalEncoder struct {
	*serviceTestEncoder
	retrieval string
}

func (e *serviceTestRetrievalEncoder) RetrievalFingerprint() string { return e.retrieval }

func (e *serviceTestEncoder) Schema() Schema { return e.schema }

func (e *serviceTestEncoder) EncodeDocument(_ context.Context, document fts.Document) ([]ChunkEmbedding, error) {
	e.documentCalls++
	return e.encoded(document.ID), nil
}

func (e *serviceTestEncoder) EncodeQuery(_ context.Context, _ string) ([]ChunkEmbedding, error) {
	e.queryCalls++
	return e.encoded("_query"), nil
}

func (e *serviceTestEncoder) EncodeQueryDocument(_ context.Context, document fts.Document) ([]ChunkEmbedding, error) {
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
	schema := serviceTestSchema(t)
	documents := &serviceTestEncoder{schema: schema, value: []float32{1, 0}}
	queries := &serviceTestEncoder{schema: schema, value: []float32{1, 0}}
	index, err := NewIndex(serviceTestConfig(schema))
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

func TestNewServiceValidatesSchemas(t *testing.T) {
	schema := serviceTestSchema(t)
	index, err := NewIndex(serviceTestConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	valid := &serviceTestEncoder{schema: schema, value: []float32{1, 0}}
	invalidSchema := schema
	invalidSchema.Chunking.Fingerprint = "different"
	invalid := &serviceTestEncoder{schema: invalidSchema, value: []float32{1, 0}}
	if _, err := NewService(index, valid, invalid); err == nil {
		t.Fatal("NewService() error = nil for mismatched query schema")
	}
}

func TestNewServiceValidatesRetrievalFingerprint(t *testing.T) {
	schema := serviceTestSchema(t)
	index, err := NewIndex(serviceTestConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	documents := &serviceTestRetrievalEncoder{
		serviceTestEncoder: &serviceTestEncoder{schema: schema, value: []float32{1, 0}},
		retrieval:          "retrieval-v1",
	}
	queries := &serviceTestRetrievalEncoder{
		serviceTestEncoder: &serviceTestEncoder{schema: schema, value: []float32{1, 0}},
		retrieval:          "retrieval-v2",
	}
	if _, err := NewService(index, documents, queries); err != ErrEmbeddingMismatch {
		t.Fatalf("NewService() error = %v, want ErrEmbeddingMismatch", err)
	}
}

func serviceTestSchema(t *testing.T) Schema {
	t.Helper()
	embedding, err := NewEmbeddingSchema("model", "v1", "pipeline", 2, vector.MetricCosine, 1)
	if err != nil {
		t.Fatal(err)
	}
	return Schema{Embedding: embedding, Chunking: ChunkingSchema{ID: "whole", Version: 1, Fingerprint: "whole-v1"}}
}

func serviceTestConfig(schema Schema) IndexConfig {
	return IndexConfig{Schema: schema, Limits: Limits{
		MaxLiveVectors: 8, MaxChunksPerDocument: 2, MaxDocumentsPerSearch: 2,
		MaxChunkCandidates: 8, MaxChunksPerDocumentHit: 1,
	}}
}
