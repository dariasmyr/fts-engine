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

func TestSaveRejectsIndependentIndexLineage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	first := newTestIndex(t)
	addTestSegmentWithVector(t, ctx, first, []float32{1, 2})
	generation := saveTestGenerations(t, ctx, root, first, 1)[0]

	second := newTestIndex(t)
	addTestSegmentWithVector(t, ctx, second, []float32{9, 8})
	if _, err := Save(ctx, root, second, SaveConfig{ExpectedGeneration: &generation}); !errors.Is(err, ErrIndexIdentityMismatch) {
		t.Fatalf("Save() error = %v, want ErrIndexIdentityMismatch", err)
	}

	loaded, loadedGeneration, err := Load(ctx, root, LoadConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if loadedGeneration != generation {
		t.Fatalf("loaded generation = %d, want %d", loadedGeneration.ID, generation.ID)
	}
	state, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]float32, 2)
	if err := state.Segments[0].Vectors.ReadVectorInto(ctx, 0, got); err != nil {
		t.Fatal(err)
	}
	if got[0] != 1 || got[1] != 2 {
		t.Fatalf("loaded vector = %v, want [1 2]", got)
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

func TestSaveWithStaleGenerationDoesNotCreateStorageRoot(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "store")
	_, err := Save(context.Background(), root, newTestIndex(t), SaveConfig{
		ExpectedGeneration: &Generation{ID: 1},
	})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("Save() error = %v, want ErrStaleGeneration", err)
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("storage root error = %v, want os.ErrNotExist", err)
	}
}

func TestSaveFailureBeforeCurrentReplacementKeepsPreviousGeneration(t *testing.T) {
	tests := []struct {
		name       string
		checkpoint saveCheckpoint
	}{
		{name: "write vectors", checkpoint: checkpointWriteVectors},
		{name: "write graph", checkpoint: checkpointWriteGraph},
		{name: "sync segment", checkpoint: checkpointSyncSegment},
		{name: "rename segment", checkpoint: checkpointRenameSegment},
		{name: "sync reused segments", checkpoint: checkpointSyncReusedSegments},
		{name: "write state", checkpoint: checkpointWriteState},
		{name: "write manifest", checkpoint: checkpointWriteManifest},
		{name: "generation fsync", checkpoint: checkpointSyncGeneration},
		{name: "rename generation", checkpoint: checkpointRenameGeneration},
		{name: "write CURRENT", checkpoint: checkpointWriteCurrent},
		{name: "CURRENT rename", checkpoint: checkpointReplaceCurrent},
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
			cfg.checkpoints.probe = saveFaultInjector{
				beforeStep: func(checkpoint saveCheckpoint) error {
					if checkpoint == tt.checkpoint {
						return failed
					}
					return nil
				},
			}
			_, err = save(ctx, root, index, nil, previous.ID, cfg)
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
	cfg.checkpoints.probe = saveFaultInjector{
		afterStep: func(checkpoint saveCheckpoint) error {
			if checkpoint == checkpointRenameGeneration {
				return failed
			}
			return nil
		},
	}
	if _, err := save(ctx, root, index, nil, previous.ID, cfg); !errors.Is(err, failed) {
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
		before saveCheckpoint
		after  saveCheckpoint
	}{
		{name: "after CURRENT replacement", after: checkpointReplaceCurrent},
		{name: "before root sync", before: checkpointSyncRoot},
		{name: "after root sync", after: checkpointSyncRoot},
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
			cfg.checkpoints.probe = saveFaultInjector{
				beforeStep: func(checkpoint saveCheckpoint) error {
					if checkpoint == tt.before {
						return failed
					}
					return nil
				},
				afterStep: func(checkpoint saveCheckpoint) error {
					if checkpoint == tt.after {
						return failed
					}
					return nil
				},
			}
			if _, err := save(ctx, root, index, nil, previous.ID, cfg); !errors.Is(err, ErrIndeterminate) {
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
