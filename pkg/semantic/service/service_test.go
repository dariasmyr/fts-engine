package service

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type ChunkEmbedding = semantic.ChunkEmbedding
type Ref = semantic.Ref
type IndexConfig = semantic.IndexConfig
type OperationalLimits = semantic.OperationalLimits
type CapacityLimits = semantic.CapacityLimits
type IngestionLimits = semantic.IngestionLimits
type SearchLimits = semantic.SearchLimits
type QueryConfig = semantic.QueryConfig

var ErrInvalidQueryConfig = semantic.ErrInvalidQueryConfig

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
	documents := &serviceTestEncoder{value: []float32{1, 0}}
	queries := &serviceTestEncoder{value: []float32{1, 0}}
	index, err := semantic.NewIndex(serviceTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(index, documents, queries)
	if err != nil {
		t.Fatal(err)
	}
	if config, ok := service.Config(); ok || config != (Config{}) {
		t.Fatalf("Config() = %+v, %v; want unavailable", config, ok)
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

func TestServiceValidatesSearchBeforeEncoding(t *testing.T) {
	t.Parallel()

	encoder := &serviceTestEncoder{value: []float32{1, 0}}
	index, err := semantic.NewIndex(serviceTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(index, encoder, encoder)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.SearchWithConfig(t.Context(), "query", 3, QueryConfig{}); err == nil {
		t.Fatal("SearchWithConfig() with excessive result count returned nil error")
	}
	if _, err := service.SearchWithConfig(t.Context(), "query", 1, QueryConfig{CandidateChunks: 9}); err == nil {
		t.Fatal("SearchWithConfig() with excessive candidate budget returned nil error")
	}
	if encoder.queryCalls != 0 {
		t.Fatalf("query encoder calls = %d, want 0", encoder.queryCalls)
	}
}

func TestServiceExportsDetachedCommittedState(t *testing.T) {
	encoder := &serviceTestEncoder{value: []float32{1, 0}}
	index, err := semantic.NewIndex(serviceTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(index, encoder, encoder)
	if err != nil {
		t.Fatal(err)
	}
	if service.IndexConfig() != index.Config() {
		t.Fatalf("IndexConfig() = %+v, want %+v", service.IndexConfig(), index.Config())
	}
	if err := service.AddDocument(t.Context(), fts.Document{
		ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "x"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	exported, err := service.ExportState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	exported.Segments[0].Rows[0].Chunk.DocID = "changed"
	exported.Segments[0].LivenessWords[0] = 0
	exported.Segments = nil

	again, err := service.ExportState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Segments) != 1 || again.Segments[0].Rows[0].Chunk.DocID != "doc" ||
		again.Segments[0].LivenessWords[0] == 0 {
		t.Fatalf("mutating exported state changed Service state: %+v", again.Segments)
	}
}

func TestServiceCommitWithStatePreservesPendingOnFailure(t *testing.T) {
	encoder := &serviceTestEncoder{value: []float32{1, 0}}
	index, err := semantic.NewIndex(serviceTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(index, encoder, encoder)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(t.Context(), fts.Document{
		ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "x"}},
	}); err != nil {
		t.Fatal(err)
	}
	want := errors.New("reject candidate")
	err = service.CommitWithState(t.Context(), func(state *semantic.CommittedIndexState) error {
		if state.Config != service.IndexConfig() || state.Revision != 1 {
			t.Fatalf("candidate = config %+v, revision %d", state.Config, state.Revision)
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("CommitWithState() error = %v, want callback error", err)
	}
	if _, err := service.ExportState(t.Context()); !errors.Is(err, semantic.ErrPendingMutations) {
		t.Fatalf("ExportState() error = %v, want ErrPendingMutations", err)
	}
	result, err := service.Search(t.Context(), "query", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("failed candidate became searchable: %+v", result.Hits)
	}

	if err := service.CommitWithState(t.Context(), func(state *semantic.CommittedIndexState) error {
		state.Segments[0].Rows[0].Chunk.DocID = "changed"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err = service.Search(t.Context(), "query", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != "doc" {
		t.Fatalf("Search() hits = %+v, want detached original doc", result.Hits)
	}
}

func serviceTestConfig() IndexConfig {
	return IndexConfig{
		Dimensions: 2,
		Metric:     vector.MetricCosine,
		Operational: OperationalLimits{
			Capacity:  CapacityLimits{MaxLiveVectors: 8},
			Ingestion: IngestionLimits{MaxChunksPerDocument: 2},
			Search:    SearchLimits{MaxResults: 2, MaxCandidateChunks: 8, MaxChunksPerResult: 1},
		},
	}
}
