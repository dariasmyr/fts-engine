package semanticpersist

import (
	"context"
	"testing"
)

func TestRecoverGenerationMakesValidatedGenerationCurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	generations := publishTestGenerations(t, ctx, root, newTestIndex(t), 2)

	options := PublishOptions{Durability: DurabilityAsynchronous}
	if err := normalizeOptions(&options); err != nil {
		t.Fatal(err)
	}
	if err := RecoverGeneration(ctx, root, generations[0].ID, options); err != nil {
		t.Fatal(err)
	}
	head, err := newHeadStore(newLayout(root), options).read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head.GenerationID != generations[0].ID {
		t.Fatalf("current generation = %d, want %d", head.GenerationID, generations[0].ID)
	}
}
