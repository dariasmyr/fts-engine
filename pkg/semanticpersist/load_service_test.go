package semanticpersist_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestCommitAndSaveLoadServiceRoundTrip(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	service, err := semantic.New(client)
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
		semanticpersist.SaveOptions{
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
		semanticpersist.LoadOptions{},
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
		semanticpersist.SaveOptions{
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
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	)
	if !errors.Is(err, semanticpersist.ErrStaleGeneration) {
		t.Fatalf("first Save() over existing data error = %v, want ErrStaleGeneration", err)
	}
}

func TestLoadServiceDerivesCompatibilityFromModelAndSemanticOptions(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	service, err := semantic.New(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.Save(
		ctx,
		root,
		service.Index(),
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}

	loaded, _, err := semanticpersist.LoadService(
		ctx,
		root,
		client,
		semanticpersist.LoadOptions{},
		semantic.WithQueryConfig(semantic.QueryConfig{
			MaxBytes: 16, LongQuery: semantic.LongQueryTruncate,
		}),
	)
	if err != nil {
		t.Fatalf("LoadService() with query-only change: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadService() returned nil service")
	}

	_, _, err = semanticpersist.LoadService(
		ctx,
		root,
		client,
		semanticpersist.LoadOptions{},
		semantic.WithMetric(vector.MetricL2Squared),
	)
	if !errors.Is(err, semantic.ErrEmbeddingMismatch) {
		t.Fatalf("LoadService() mismatch error = %v, want semantic.ErrEmbeddingMismatch", err)
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
		semanticpersist.LoadOptions{},
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadService() error = %v, want context canceled", err)
	}

	root := t.TempDir()
	service, err := semantic.New(client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.Save(
		t.Context(),
		root,
		service.Index(),
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}
	limits := semanticpersist.DefaultLimits()
	limits.MaxDimensions = 1
	_, _, err = semanticpersist.LoadService(
		t.Context(),
		root,
		client,
		semanticpersist.LoadOptions{Limits: limits},
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
