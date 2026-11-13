package main

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
)

func TestServiceAdapterSearchSemantic(t *testing.T) {
	t.Parallel()

	client, err := embedding.NewClient(embedding.Model{
		ID: "test", Revision: "v1", Dimensions: 2, ContextTokens: 512,
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i, text := range texts {
			if text == "river hotel" {
				vectors[i] = []float32{1, 0}
			} else {
				vectors[i] = []float32{0, 1}
			}
		}
		return vectors, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	serviceConfig, err := service.DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	semanticService, err := service.New(client, serviceConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := addSemanticDocument(t.Context(), semanticService, models.Document{
		ID: "hotel", DocumentBase: models.DocumentBase{Title: "river hotel"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := addSemanticDocument(t.Context(), semanticService, models.Document{
		ID: "ship", DocumentBase: models.DocumentBase{Title: "cargo ship"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := semanticService.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}

	adapter := &serviceAdapter{semanticService: semanticService}
	result, err := adapter.SearchSemantic(t.Context(), "river hotel", 2)
	if err != nil {
		t.Fatalf("SearchSemantic() error = %v", err)
	}
	if len(result.ResultData) != 2 || result.ResultData[0].ID != "hotel" {
		t.Fatalf("SearchSemantic() results = %+v, want hotel first", result.ResultData)
	}
	if result.ResultData[0].Distance == nil {
		t.Fatal("SearchSemantic() did not project distance")
	}
	if result.Diagnostics == nil || result.Diagnostics.ExecutionStrategy != "hnsw" {
		t.Fatalf("SearchSemantic() diagnostics = %+v", result.Diagnostics)
	}
}
