package semanticpersist

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
)

func TestLowLevelSaveFailureAfterExplicitCommitKeepsPreviousCurrent(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client, err := embedding.NewClient(embedding.Model{
		ID: "failure-test", Revision: "v1", Dimensions: 2, ContextTokens: 512,
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i, text := range texts {
			if strings.Contains(text, "beta") {
				vectors[i] = []float32{0, 1}
			} else {
				vectors[i] = []float32{1, 0}
			}
		}
		return vectors, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config, err := service.DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	semanticService, err := service.New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := semanticService.AddDocument(ctx, persistenceDocument("alpha")); err != nil {
		t.Fatal(err)
	}
	if err := semanticService.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = SaveService(ctx, root, semanticService, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := semanticService.AddDocument(ctx, persistenceDocument("beta")); err != nil {
		t.Fatal(err)
	}
	if err := semanticService.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = SaveService(ctx, root, semanticService, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("Save() error = %v, want ErrStaleGeneration", err)
	}
	result, err := semanticService.Search(ctx, "beta", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != "beta" {
		t.Fatalf("in-process Search() hits = %+v, want beta", result.Hits)
	}
	loader, err := NewLoader(root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, err := service.Load(ctx, loader, client)
	if err != nil {
		t.Fatal(err)
	}
	result, err = loaded.Search(ctx, "beta", 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range result.Hits {
		if hit.DocID == "beta" {
			t.Fatalf("loaded Search() includes unsaved document: %+v", result.Hits)
		}
	}
}

func persistenceDocument(id string) fts.Document {
	return fts.Document{
		ID: fts.DocID(id),
		Fields: map[string]fts.FieldData{
			fts.DefaultField: {Text: id},
		},
	}
}
