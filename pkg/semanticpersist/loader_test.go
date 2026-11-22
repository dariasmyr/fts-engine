package semanticpersist

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
)

func TestNewLoaderValidatesWithoutFilesystemIO(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-created")
	loader, err := NewLoader(root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if loader.root != root || loader.options != (LoadOptions{}) {
		t.Fatalf("loader = %+v", loader)
	}
	if err := newLayout(root).validate(); err == nil {
		t.Fatal("NewLoader() created the persistence layout")
	}

	if _, err := NewLoader("", LoadOptions{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("NewLoader() empty root error = %v, want ErrCorrupt", err)
	}
	limits := DefaultLimits()
	limits.MaxDimensions = -1
	if _, err := NewLoader(root, LoadOptions{Limits: limits}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("NewLoader() limits error = %v, want ErrLimitExceeded", err)
	}
}

func TestLoaderKeepsOneOperationLockAcrossPreflightAndRestore(t *testing.T) {
	root, client := saveLoaderTestService(t)
	loader, err := NewLoader(root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}

	preflightCalls := 0
	loaded, err := loader.LoadServiceState(t.Context(), func(metadata service.RestoreMetadata) error {
		preflightCalls++
		lock, err := storage.AcquireOperationLock(newLayout(root).lock)
		if err == nil {
			_ = lock.Close()
			t.Fatal("persistence lock was not held during preflight")
		}
		if !errors.Is(err, storage.ErrLocked) {
			t.Fatalf("lock error = %v, want ErrLocked", err)
		}
		if metadata.IndexID == (semantic.IndexID{}) || metadata.GenerationID != 1 ||
			metadata.IndexConfig.Dimensions != metadata.Pipeline.Model.Dimensions {
			t.Fatalf("metadata = %+v", metadata)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if preflightCalls != 1 {
		t.Fatalf("preflight calls = %d, want 1", preflightCalls)
	}
	if loaded.Index == nil || loaded.GenerationID != 1 || loaded.Index.Config().Dimensions != client.Model().Dimensions {
		t.Fatalf("loaded state = %+v", loaded)
	}
}

func TestLoaderReleasesLockOnPreflightFailure(t *testing.T) {
	root, _ := saveLoaderTestService(t)
	loader, err := NewLoader(root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("preflight failed")
	if _, err := loader.LoadServiceState(t.Context(), func(service.RestoreMetadata) error {
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("LoadServiceState() error = %v, want preflight error", err)
	}

	if _, err := loader.LoadServiceState(t.Context(), func(service.RestoreMetadata) error {
		return nil
	}); err != nil {
		t.Fatalf("LoadServiceState() after failure: %v", err)
	}
}

func TestLoaderRejectsInvalidCallback(t *testing.T) {
	root, _ := saveLoaderTestService(t)
	loader, err := NewLoader(root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.LoadServiceState(t.Context(), nil); !errors.Is(err, service.ErrInvalidLoadedState) {
		t.Fatalf("LoadServiceState() nil callback error = %v, want ErrInvalidLoadedState", err)
	}
	var nilLoader *Loader
	if _, err := nilLoader.LoadServiceState(t.Context(), func(service.RestoreMetadata) error {
		return nil
	}); !errors.Is(err, service.ErrInvalidLoadedState) {
		t.Fatalf("nil Loader error = %v, want ErrInvalidLoadedState", err)
	}
}

func saveLoaderTestService(t *testing.T) (string, embedding.Client) {
	t.Helper()
	client, err := embedding.NewClient(embedding.Model{
		ID: "loader-test", Revision: "v1", Dimensions: 2, ContextTokens: 128,
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
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
	if err := semanticService.AddDocument(t.Context(), fts.Document{
		ID: "doc",
		Fields: map[string]fts.FieldData{
			fts.DefaultField: {Text: "document"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if _, err := CommitAndSaveService(
		t.Context(),
		root,
		semanticService,
		SaveOptions{ExpectedGeneration: &Generation{}},
	); err != nil {
		t.Fatal(err)
	}
	return root, client
}
