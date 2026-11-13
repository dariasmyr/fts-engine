package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
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

func TestPopulateSemanticDocumentsCheckpointsAndResumes(t *testing.T) {
	t.Parallel()

	client, err := embedding.NewClient(embedding.Model{
		ID: "test", Revision: "v1", Dimensions: 2, ContextTokens: 512,
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, float32(i)}
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
	root := filepath.Join(t.TempDir(), "semantic")
	runtimeState := &semanticRuntime{
		service: semanticService, persistencePath: root, persistence: true,
		saveOnBuild: true, workers: 2, checkpointDocs: 2,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	documents := []models.Document{
		{ID: "one", DocumentBase: models.DocumentBase{Title: "one"}},
		{ID: "two", DocumentBase: models.DocumentBase{Title: "two"}},
		{ID: "three", DocumentBase: models.DocumentBase{Title: "three"}},
	}
	if err := populateSemanticDocuments(t.Context(), log, runtimeState, documents); err != nil {
		t.Fatal(err)
	}
	if runtimeState.generation.ID != 2 {
		t.Fatalf("generation = %d, want 2", runtimeState.generation.ID)
	}

	loader, err := semanticpersist.NewLoader(root, semanticpersist.LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, generationID, err := service.Load(t.Context(), loader, client)
	if err != nil {
		t.Fatal(err)
	}
	resumed := &semanticRuntime{
		service: loaded, generation: semanticpersist.Generation{ID: generationID},
		persistencePath: root, persistence: true, saveOnBuild: true,
		workers: 2, checkpointDocs: 2, loadedFromDisk: true,
	}
	documents = append(documents, models.Document{ID: "four", DocumentBase: models.DocumentBase{Title: "four"}})
	if err := populateSemanticDocuments(t.Context(), log, resumed, documents); err != nil {
		t.Fatal(err)
	}
	if resumed.generation.ID != 3 {
		t.Fatalf("resumed generation = %d, want 3", resumed.generation.ID)
	}
	if stats := resumed.service.Statistics(); stats.Documents != 4 {
		t.Fatalf("documents = %d, want 4", stats.Documents)
	}
}
