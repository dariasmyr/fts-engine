package semanticpersist_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
)

func TestCommitAndSaveServiceLoadRoundTrip(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	config := persistentTestConfig(t, client)
	semanticService, err := service.New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := semanticService.AddDocument(ctx, testDocument("alpha", "alpha")); err != nil {
		t.Fatal(err)
	}
	generation, err := semanticpersist.CommitAndSaveService(
		ctx,
		root,
		semanticService,
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
	assertSearchHit(t, semanticService, "alpha", "alpha")

	loader, err := semanticpersist.NewLoader(root, semanticpersist.LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, loadedGenerationID, err := service.Load(ctx, loader, client)
	if err != nil {
		t.Fatal(err)
	}
	loadedGeneration := semanticpersist.Generation{ID: loadedGenerationID}
	if loadedGeneration != generation {
		t.Fatalf("loaded generation = %d, want %d", loadedGeneration.ID, generation.ID)
	}
	assertSearchHit(t, loaded, "alpha", "alpha")

	if err := loaded.AddDocument(ctx, testDocument("beta", "beta")); err != nil {
		t.Fatal(err)
	}
	next, err := semanticpersist.CommitAndSaveService(
		ctx,
		root,
		loaded,
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
		exportedServiceIndex(t, ctx, semanticService),
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	)
	if !errors.Is(err, semanticpersist.ErrStaleGeneration) {
		t.Fatalf("first Save() over existing data error = %v, want ErrStaleGeneration", err)
	}
}

func TestServiceLoadRestoresSavedConfig(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	config := persistentTestConfig(t, client)
	config.Pipeline.Chunking = chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	semanticService, err := service.New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.SaveService(
		ctx,
		root,
		semanticService,
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}

	loaded, _, err := loadPersistedService(
		ctx,
		root,
		client,
		semanticpersist.LoadOptions{},
	)
	if err != nil {
		t.Fatalf("ServiceLoad(): %v", err)
	}
	if loaded == nil {
		t.Fatal("ServiceLoad() returned nil service")
	}
	loadedConfig, loadedOK := loaded.Config()
	if !loadedOK || loadedConfig != config {
		t.Fatalf("loaded config = %+v, want saved %+v", loadedConfig, config)
	}

	_, _, err = loadPersistedService(
		ctx,
		root,
		persistentTestClient(t, "v2"),
		semanticpersist.LoadOptions{},
	)
	if !errors.Is(err, service.ErrModelServiceUnavailable) {
		t.Fatalf("ServiceLoad() mismatch error = %v, want service.ErrModelServiceUnavailable", err)
	}
}

func TestServiceLoadRoundTripsUnicodeMetadata(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	semanticService, err := service.New(client, persistentTestConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	document := fts.Document{
		ID: "документ",
		Fields: map[string]fts.FieldData{
			"заголовок": {Text: "привет"},
		},
	}
	if err := semanticService.AddDocument(ctx, document); err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.CommitAndSaveService(
		ctx,
		root,
		semanticService,
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}

	loader, err := semanticpersist.NewLoader(root, semanticpersist.LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, err := service.Load(ctx, loader, client)
	if err != nil {
		t.Fatal(err)
	}
	state, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Segments) != 1 {
		t.Fatalf("loaded segments = %d, want 1", len(state.Segments))
	}
	if len(state.Segments[0].Rows) != 1 {
		t.Fatalf("loaded rows = %d, want 1", len(state.Segments[0].Rows))
	}
	ref := state.Segments[0].Rows[0].Chunk
	if ref.DocID != document.ID || ref.Field != "заголовок" || ref.ID != "заголовок/chunk-000000" {
		t.Fatalf("loaded ref = %+v", ref)
	}
}

func TestServiceLoadChecksModelBeforeSegmentObjects(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	semanticService, err := service.New(client, persistentTestConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	if err := semanticService.AddDocument(ctx, testDocument("alpha", "alpha")); err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.CommitAndSaveService(
		ctx,
		root,
		semanticService,
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}

	var vectorsPath string
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Name() == "vectors.bin" {
			vectorsPath = path
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if vectorsPath == "" {
		t.Fatal("vectors.bin not found")
	}
	if err := os.WriteFile(vectorsPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := loadPersistedService(
		ctx,
		root,
		persistentTestClient(t, "v2"),
		semanticpersist.LoadOptions{},
	); !errors.Is(err, service.ErrModelServiceUnavailable) {
		t.Fatalf("ServiceLoad() mismatch error = %v, want service.ErrModelServiceUnavailable", err)
	}
	if _, _, err := loadPersistedService(
		ctx,
		root,
		client,
		semanticpersist.LoadOptions{},
	); !errors.Is(err, semanticpersist.ErrCorrupt) {
		t.Fatalf("ServiceLoad() segment error = %v, want ErrCorrupt", err)
	}
}

func TestServiceLoadChecksMetadataBeforeModel(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	semanticService, err := service.New(client, persistentTestConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.SaveService(
		ctx,
		root,
		semanticService,
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}

	var statePath string
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Name() == "semantic-state.bin" {
			statePath = path
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if statePath == "" {
		t.Fatal("semantic-state.bin not found")
	}
	if err := os.WriteFile(statePath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := loadPersistedService(
		ctx,
		root,
		persistentTestClient(t, "v2"),
		semanticpersist.LoadOptions{},
	); !errors.Is(err, semanticpersist.ErrCorrupt) {
		t.Fatalf("ServiceLoad() error = %v, want ErrCorrupt", err)
	}
}

func TestServiceLoadRejectsRawIndexGeneration(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	client := persistentTestClient(t, "v1")
	semanticService, err := service.New(client, persistentTestConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.Save(
		ctx,
		root,
		exportedServiceIndex(t, ctx, semanticService),
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}

	if _, _, err := loadPersistedService(
		ctx,
		root,
		client,
		semanticpersist.LoadOptions{},
	); !errors.Is(err, service.ErrModelServiceUnavailable) {
		t.Fatalf("ServiceLoad() error = %v, want service.ErrModelServiceUnavailable", err)
	}
}

func TestServiceLoadPreservesCancellationAndPersistenceLimits(t *testing.T) {
	client := persistentTestClient(t, "v1")
	config := persistentTestConfig(t, client)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := loadPersistedService(
		ctx,
		t.TempDir(),
		client,
		semanticpersist.LoadOptions{},
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("ServiceLoad() error = %v, want context canceled", err)
	}

	root := t.TempDir()
	semanticService, err := service.New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := semanticpersist.SaveService(
		t.Context(),
		root,
		semanticService,
		semanticpersist.SaveOptions{ExpectedGeneration: &semanticpersist.Generation{}},
	); err != nil {
		t.Fatal(err)
	}
	limits := semanticpersist.DefaultLimits()
	limits.MaxDimensions = 1
	_, _, err = loadPersistedService(
		t.Context(),
		root,
		client,
		semanticpersist.LoadOptions{Limits: limits},
	)
	if !errors.Is(err, semanticpersist.ErrLimitExceeded) {
		t.Fatalf("ServiceLoad() limit error = %v, want ErrLimitExceeded", err)
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

func persistentTestConfig(t *testing.T, client embedding.Client) service.Config {
	t.Helper()
	config, err := service.DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func loadPersistedService(
	ctx context.Context,
	root string,
	client embedding.Client,
	options semanticpersist.LoadOptions,
) (*service.Service, semanticpersist.Generation, error) {
	loader, err := semanticpersist.NewLoader(root, options)
	if err != nil {
		return nil, semanticpersist.Generation{}, err
	}
	loaded, generationID, err := service.Load(ctx, loader, client)
	return loaded, semanticpersist.Generation{ID: generationID}, err
}

func exportedServiceIndex(
	t *testing.T,
	ctx context.Context,
	semanticService *service.Service,
) *semantic.Index {
	t.Helper()
	state, err := semanticService.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	index, err := semantic.Restore(ctx, *state)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func testDocument(id, text string) fts.Document {
	return fts.Document{
		ID: fts.DocID(id),
		Fields: map[string]fts.FieldData{
			fts.DefaultField: {Text: text},
		},
	}
}

func assertSearchHit(t *testing.T, semanticService *service.Service, query string, want fts.DocID) {
	t.Helper()
	result, err := semanticService.Search(t.Context(), query, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != want {
		t.Fatalf("Search(%q) hits = %+v, want %q", query, result.Hits, want)
	}
}
