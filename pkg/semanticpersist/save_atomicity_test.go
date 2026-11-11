package semanticpersist

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

func TestSaveDoesNotReuseSegmentFromIndependentIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	first := newTestIndex(t)
	addTestSegmentWithVector(t, ctx, first, []float32{1, 2})
	generation := saveTestGenerations(t, ctx, root, first, 1)[0]

	second := newTestIndex(t)
	addTestSegmentWithVector(t, ctx, second, []float32{9, 8})
	if _, err := Save(ctx, root, second, SaveConfig{ExpectedGeneration: &generation}); err != nil {
		t.Fatal(err)
	}

	loaded, _, err := Load(ctx, root, LoadConfig{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]float32, 2)
	if err := state.Segments[0].Vectors.ReadVectorInto(ctx, 0, got); err != nil {
		t.Fatal(err)
	}
	if got[0] != 9 || got[1] != 8 {
		t.Fatalf("loaded vector = %v, want [9 8]", got)
	}
}

func TestSaveChecksExpectedGenerationBeforeSegmentObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	addTestSegment(t, ctx, index)
	saveTestGenerations(t, ctx, root, index, 1)

	l := newLayout(root)
	entries, err := os.ReadDir(l.segments)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("segment objects = %d, want 1", len(entries))
	}
	if err := os.WriteFile(filepath.Join(l.segments, entries[0].Name(), vectorsFileName), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = Save(ctx, root, index, SaveConfig{ExpectedGeneration: &Generation{}})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("Save() error = %v, want ErrStaleGeneration", err)
	}
}

func TestSaveFailureBeforeCurrentReplacementKeepsPreviousGeneration(t *testing.T) {
	tests := []struct {
		name string
		step saveStep
	}{
		{name: "write vectors", step: stepWriteVectors},
		{name: "write graph", step: stepWriteGraph},
		{name: "sync segment", step: stepSyncSegment},
		{name: "rename segment", step: stepRenameSegment},
		{name: "sync reused segments", step: stepSyncReusedSegments},
		{name: "write state", step: stepWriteState},
		{name: "write manifest", step: stepWriteManifest},
		{name: "generation fsync", step: stepSyncGeneration},
		{name: "rename generation", step: stepRenameGeneration},
		{name: "write CURRENT", step: stepWriteCurrent},
		{name: "CURRENT rename", step: stepReplaceCurrent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			root := t.TempDir()
			index := newTestIndex(t)
			addTestSegment(t, ctx, index)
			generations := saveTestGenerations(t, ctx, root, index, 1)
			previous := generations[0]
			addTestDocumentSegment(t, ctx, index, "doc-2", []float32{3, 4})

			failed := errors.New("injected save failure")
			cfg, err := newSettings(Config{})
			if err != nil {
				t.Fatal(err)
			}
			cfg.hooks.before = func(step saveStep) error {
				if step == tt.step {
					return failed
				}
				return nil
			}
			_, err = save(ctx, root, index, previous.ID, cfg)
			if !errors.Is(err, failed) {
				t.Fatalf("save() error = %v, want injected failure", err)
			}

			head, err := newCurrentFile(
				newLayout(root),
				cfg,
			).read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if head.GenerationID != previous.ID {
				t.Fatalf("CURRENT generation = %d, want %d", head.GenerationID, previous.ID)
			}

			_, loadedGeneration, err := Load(ctx, root, LoadConfig{})
			if err != nil {
				t.Fatalf("Load() after failed save: %v", err)
			}
			if loadedGeneration != previous {
				t.Fatalf("loaded generation = %d, want %d", loadedGeneration.ID, previous.ID)
			}
		})
	}
}

func TestSaveRetrySkipsInstalledOrphanGeneration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	addTestSegment(t, ctx, index)
	previous := saveTestGenerations(t, ctx, root, index, 1)[0]

	failed := errors.New("after generation rename")
	cfg, err := newSettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.hooks.after = func(step saveStep) error {
		if step == stepRenameGeneration {
			return failed
		}
		return nil
	}
	if _, err := save(ctx, root, index, previous.ID, cfg); !errors.Is(err, failed) {
		t.Fatalf("save() error = %v, want injected failure", err)
	}

	next, err := Save(ctx, root, index, SaveConfig{ExpectedGeneration: &previous})
	if err != nil {
		t.Fatal(err)
	}
	if next.ID != previous.ID+2 {
		t.Fatalf("retry generation = %d, want %d", next.ID, previous.ID+2)
	}
}

func TestSaveFailureAfterCurrentReplacementIsIndeterminate(t *testing.T) {
	tests := []struct {
		name   string
		before saveStep
		after  saveStep
	}{
		{name: "after CURRENT replacement", after: stepReplaceCurrent},
		{name: "before root sync", before: stepSyncRoot},
		{name: "after root sync", after: stepSyncRoot},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			root := t.TempDir()
			index := newTestIndex(t)
			previous := saveTestGenerations(t, ctx, root, index, 1)[0]

			failed := errors.New("injected committed failure")
			cfg, err := newSettings(Config{})
			if err != nil {
				t.Fatal(err)
			}
			cfg.hooks.before = func(step saveStep) error {
				if step == tt.before {
					return failed
				}
				return nil
			}
			cfg.hooks.after = func(step saveStep) error {
				if step == tt.after {
					return failed
				}
				return nil
			}
			if _, err := save(ctx, root, index, previous.ID, cfg); !errors.Is(err, ErrIndeterminate) {
				t.Fatalf("save() error = %v, want ErrIndeterminate", err)
			}
			_, generation, err := Load(ctx, root, LoadConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if generation.ID != previous.ID+1 {
				t.Fatalf("loaded generation = %d, want %d", generation.ID, previous.ID+1)
			}
		})
	}
}

func TestSaveRejectsExhaustedGenerationID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	previous := saveTestGenerations(t, ctx, root, index, 1)[0]
	if err := os.Mkdir(newLayout(root).generation(math.MaxUint64), 0o700); err != nil {
		t.Fatal(err)
	}

	if _, err := Save(ctx, root, index, SaveConfig{ExpectedGeneration: &previous}); !errors.Is(err, ErrGenerationExhausted) {
		t.Fatalf("Save() error = %v, want ErrGenerationExhausted", err)
	}
}

func addTestSegmentWithVector(t *testing.T, ctx context.Context, index *semantic.Index, embedding []float32) {
	addTestDocumentSegment(t, ctx, index, "doc", embedding)
}

func addTestDocumentSegment(
	t *testing.T,
	ctx context.Context,
	index *semantic.Index,
	docID fts.DocID,
	embedding []float32,
) {
	t.Helper()

	err := index.AddDocument(ctx, docID, []semantic.ChunkEmbedding{{
		Ref: semantic.Ref{
			ID:      "chunk",
			DocID:   docID,
			Field:   fts.DefaultField,
			EndByte: 1,
		},
		Embedding: embedding,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
