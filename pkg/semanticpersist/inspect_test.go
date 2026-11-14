package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
)

func TestInspectReturnsDetachedRawIndexContract(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	generation, err := Save(ctx, root, index, SaveOptions{ExpectedGeneration: &Generation{}})
	if err != nil {
		t.Fatal(err)
	}
	state, err := index.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}

	info, err := Inspect(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Generation != generation || info.IndexID != state.ID || info.IndexConfig != index.Config() {
		t.Fatalf("Inspect() = %+v, want generation=%+v indexID=%x config=%+v", info, generation, state.ID, index.Config())
	}
	if info.HasPipeline || info.Pipeline != (service.PipelineConfig{}) {
		t.Fatalf("raw pipeline = %+v/%v, want zero/false", info.Pipeline, info.HasPipeline)
	}
}

func TestInspectReturnsDetachedModelBackedContract(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	client := newLineageTestClient(t)
	config, err := service.DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	semanticService, err := service.New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := SaveService(ctx, root, semanticService, SaveOptions{ExpectedGeneration: &Generation{}})
	if err != nil {
		t.Fatal(err)
	}

	info, err := Inspect(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Generation != generation || info.IndexConfig != config.Index || !info.HasPipeline || info.Pipeline != config.Pipeline {
		t.Fatalf("Inspect() = %+v, want generation=%+v config=%+v", info, generation, config)
	}

	info.Pipeline.Model.ID = "mutated"
	again, err := Inspect(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if again.Pipeline != config.Pipeline {
		t.Fatalf("persisted pipeline changed through result: %+v", again.Pipeline)
	}
}

func TestInspectDoesNotOpenSegmentObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	addTestSegment(t, ctx, index)
	if _, err := Save(ctx, root, index, SaveOptions{ExpectedGeneration: &Generation{}}); err != nil {
		t.Fatal(err)
	}
	vectorsPath := findStoredFile(t, root, vectorsFileName)
	if err := os.WriteFile(vectorsPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Inspect(ctx, root, LoadOptions{}); err != nil {
		t.Fatalf("Inspect() opened segment objects: %v", err)
	}
	if _, _, err := Load(ctx, root, LoadOptions{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Load() error = %v, want ErrCorrupt", err)
	}
}

func TestInspectLimitsOnlyRejectStoredContract(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	if _, err := Save(ctx, root, index, SaveOptions{ExpectedGeneration: &Generation{}}); err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.MaxDimensions = 1
	if _, err := Inspect(ctx, root, LoadOptions{Limits: limits}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("Inspect() error = %v, want ErrLimitExceeded", err)
	}

	info, err := Inspect(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.IndexConfig != index.Config() {
		t.Fatalf("stored config = %+v, want %+v", info.IndexConfig, index.Config())
	}
}

func findStoredFile(t *testing.T, root, name string) string {
	t.Helper()

	var found string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Name() == name {
			found = path
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found == "" {
		t.Fatalf("%s not found", name)
	}
	return found
}
