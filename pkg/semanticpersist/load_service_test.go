package semanticpersist_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
)

func TestCommitAndSaveLoadServiceRoundTrip(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	service, err := semantic.New(client, persistentTestConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(ctx, testDocument("alpha", "alpha")); err != nil {
		t.Fatal(err)
	}
	generation, err := semanticpersist.CommitAndSave(
		ctx,
		root,
		service.Index(),
		semanticpersist.SaveConfig{
			ExpectedGeneration: &semanticpersist.Generation{},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if generation.ID != 1 {
		t.Fatalf("generation = %d, want 1", generation.ID)
	}
	assertSearchHit(t, service, "alpha", "alpha")

	loaded, loadedGeneration, err := semanticpersist.LoadService(
		ctx,
		root,
		client,
		semanticpersist.LoadConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if loadedGeneration != generation {
		t.Fatalf("loaded generation = %d, want %d", loadedGeneration.ID, generation.ID)
	}
	assertSearchHit(t, loaded, "alpha", "alpha")

	if err := loaded.AddDocument(ctx, testDocument("beta", "beta")); err != nil {
		t.Fatal(err)
	}
	next, err := semanticpersist.CommitAndSave(
		ctx,
		root,
		loaded.Index(),
		semanticpersist.SaveConfig{
			ExpectedGeneration: &loadedGeneration,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if next.ID != 2 {
		t.Fatalf("next generation = %d, want 2", next.ID)
	}

	_, err = semanticpersist.Save(
		ctx,
		root,
		service.Index(),
		semanticpersist.SaveConfig{ExpectedGeneration: &semanticpersist.Generation{}},
	)
	if !errors.Is(err, semanticpersist.ErrStaleGeneration) {
		t.Fatalf("first Save() over existing data error = %v, want ErrStaleGeneration", err)
	}
}

func TestLoadServiceRestoresPersistedSemanticConfig(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	config := persistentTestConfig(t, client)
	config.Chunking.Builtin = chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	service, err := semantic.New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.Save(
		ctx,
		root,
		service.Index(),
		semanticpersist.SaveConfig{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}

	loaded, _, err := semanticpersist.LoadService(
		ctx,
		root,
		client,
		semanticpersist.LoadConfig{},
	)
	if err != nil {
		t.Fatalf("LoadService(): %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadService() returned nil service")
	}
	if loaded.Config() != service.Config() {
		t.Fatalf("loaded config = %+v, want %+v", loaded.Config(), service.Config())
	}

	_, _, err = semanticpersist.LoadService(
		ctx,
		root,
		persistentTestClient(t, "v2"),
		semanticpersist.LoadConfig{},
	)
	if !errors.Is(err, semantic.ErrModelServiceUnavailable) {
		t.Fatalf("LoadService() mismatch error = %v, want semantic.ErrModelServiceUnavailable", err)
	}
}

func TestLoadServicePreservesCancellationAndPersistenceLimits(t *testing.T) {
	client := persistentTestClient(t, "v1")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := semanticpersist.LoadService(
		ctx,
		t.TempDir(),
		client,
		semanticpersist.LoadConfig{},
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadService() error = %v, want context canceled", err)
	}

	root := t.TempDir()
	service, err := semantic.New(client, persistentTestConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.Save(
		t.Context(),
		root,
		service.Index(),
		semanticpersist.SaveConfig{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}
	limits := semanticpersist.DefaultConfig()
	limits.MaxDimensions = 1
	_, _, err = semanticpersist.LoadService(
		t.Context(),
		root,
		client,
		semanticpersist.LoadConfig{Config: limits},
	)
	if !errors.Is(err, semanticpersist.ErrLimitExceeded) {
		t.Fatalf("LoadService() limit error = %v, want ErrLimitExceeded", err)
	}
}

func persistentTestClient(t *testing.T, revision string) embedding.Client {
	t.Helper()
	client, err := embedding.NewClient(embedding.Model{
		ID: "persistent-test", Revision: revision, Dimensions: 2, ContextTokens: 512,
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i, text := range texts {
			switch {
			case strings.Contains(text, "alpha"):
				vectors[i] = []float32{1, 0}
			case strings.Contains(text, "beta"):
				vectors[i] = []float32{0, 1}
			default:
				vectors[i] = []float32{1, 1}
			}
		}
		return vectors, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func persistentTestConfig(t *testing.T, client embedding.Client) semantic.SemanticConfig {
	t.Helper()
	config, err := semantic.DefaultSemanticConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func testDocument(id, text string) fts.Document {
	return fts.Document{
		ID: fts.DocID(id),
		Fields: map[string]fts.FieldData{
			fts.DefaultField: {Text: text},
		},
	}
}

func assertSearchHit(t *testing.T, service *semantic.Service, query string, want fts.DocID) {
	t.Helper()
	result, err := service.Search(t.Context(), query, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != want {
		t.Fatalf("Search(%q) hits = %+v, want %q", query, result.Hits, want)
	}
}
