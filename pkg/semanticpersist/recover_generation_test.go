package semanticpersist

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
)

func TestRecoverGenerationMakesValidatedGenerationCurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	generations := saveTestGenerations(t, ctx, root, newTestIndex(t), 2)

	cfg, err := newSettings(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := RecoverGeneration(ctx, root, RecoverOptions{Generation: Generation{ID: generations[0].ID}}); err != nil {
		t.Fatal(err)
	}
	head, err := newCurrentFile(newLayout(root), cfg).read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head.GenerationID != generations[0].ID {
		t.Fatalf("current generation = %d, want %d", head.GenerationID, generations[0].ID)
	}
}

func TestRecoverGenerationRequiresExclusiveOperationLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	generation := saveTestGenerations(t, ctx, root, newTestIndex(t), 1)[0]
	lock, err := storage.AcquireOperationLock(newLayout(root).lock)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	if err := RecoverGeneration(ctx, root, RecoverOptions{Generation: Generation{ID: generation.ID}}); !errors.Is(err, ErrLocked) {
		t.Fatalf("RecoverGeneration() error = %v, want ErrLocked", err)
	}
}

func TestRecoverGenerationRejectsDifferentIndexLineage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	current := saveTestGenerations(t, ctx, root, newTestIndex(t), 1)[0]

	candidateState, err := newTestIndex(t).ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := buildPersistenceState(ctx, candidateState, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := newSettings(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	const candidateID = 2
	if _, err := newGenerationFiles(newLayout(root), cfg).write(
		ctx,
		candidateID,
		candidate.state,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	err = RecoverGeneration(ctx, root, RecoverOptions{Generation: Generation{ID: candidateID}})
	if !errors.Is(err, ErrIndexIdentityMismatch) {
		t.Fatalf("RecoverGeneration() error = %v, want ErrIndexIdentityMismatch", err)
	}
	head, err := newCurrentFile(newLayout(root), cfg).read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head.GenerationID != current.ID {
		t.Fatalf("CURRENT generation = %d, want %d", head.GenerationID, current.ID)
	}
}

func TestSaveContinuesAfterRecoveringOlderGeneration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	generations := saveTestGenerations(t, ctx, root, index, 2)

	if err := RecoverGeneration(ctx, root, RecoverOptions{Generation: Generation{ID: generations[0].ID}}); err != nil {
		t.Fatal(err)
	}
	loaded, loadedGeneration, err := Load(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next, err := Save(ctx, root, loaded, SaveOptions{ExpectedGeneration: &loadedGeneration})
	if err != nil {
		t.Fatal(err)
	}
	if next.ID <= generations[1].ID {
		t.Fatalf("next generation = %d, want greater than %d", next.ID, generations[1].ID)
	}
}
