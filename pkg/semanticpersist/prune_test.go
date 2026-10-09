package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

func TestPruneRetainsCurrentAndLatestValidGenerations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	generations := publishTestGenerations(t, ctx, root, index, 3)

	result, err := Prune(ctx, root, PruneOptions{RetainGenerations: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.RemovedGenerations != 1 {
		t.Fatalf("removed generations = %d, want 1", result.RemovedGenerations)
	}
	if _, err := os.Lstat(newLayout(root).generation(generations[0].ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest generation error = %v, want os.ErrNotExist", err)
	}
	for _, generation := range generations[1:] {
		if err := validateDirectory(newLayout(root).generation(generation.ID)); err != nil {
			t.Fatalf("retained generation %d: %v", generation.ID, err)
		}
	}
	store, err := Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPruneSkipsCorruptGenerationWhenSelectingRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	generations := publishTestGenerations(t, ctx, root, newTestIndex(t), 3)
	if err := os.WriteFile(newLayout(root).manifest(generations[1].ID), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := Prune(ctx, root, PruneOptions{RetainGenerations: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.RemovedGenerations != 1 {
		t.Fatalf("removed generations = %d, want 1", result.RemovedGenerations)
	}
	if _, err := os.Lstat(newLayout(root).generation(generations[1].ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt generation error = %v, want os.ErrNotExist", err)
	}
	for _, generation := range []Generation{generations[0], generations[2]} {
		if err := validateDirectory(newLayout(root).generation(generation.ID)); err != nil {
			t.Fatalf("retained generation %d: %v", generation.ID, err)
		}
	}
}

func TestPrunePreservesReusedSegmentObject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	addTestSegment(t, ctx, index)
	publishTestGenerations(t, ctx, root, index, 2)

	entries, err := os.ReadDir(newLayout(root).segments)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !validObjectID(entries[0].Name()) {
		t.Fatalf("segment entries = %v, want one object", entries)
	}
	objectPath := newLayout(root).segment(entries[0].Name())

	result, err := Prune(ctx, root, PruneOptions{RetainGenerations: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.RemovedGenerations != 1 || result.RemovedSegmentObjects != 0 {
		t.Fatalf("result = %+v, want one generation and no objects removed", result)
	}
	if err := validateDirectory(objectPath); err != nil {
		t.Fatalf("reused object: %v", err)
	}
}

func TestPruneRejectsCorruptCurrentBeforeDeletion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	generations := publishTestGenerations(t, ctx, root, newTestIndex(t), 2)
	if err := os.WriteFile(newLayout(root).manifest(generations[1].ID), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Prune(ctx, root, PruneOptions{RetainGenerations: 1}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Prune error = %v, want ErrCorrupt", err)
	}
	if err := validateDirectory(newLayout(root).generation(generations[0].ID)); err != nil {
		t.Fatalf("older generation was changed: %v", err)
	}
}

func TestPruneRemovesRecognizedOrphansAndTempsOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	publishTestGenerations(t, ctx, root, newTestIndex(t), 1)
	l := newLayout(root)

	makeKnownContainer(t, l.generation(2), manifestFileName, stateFileName)
	makeKnownContainer(t, filepath.Join(l.generations, ".tmp-gen-test"), manifestFileName, stateFileName)
	makeKnownContainer(t, filepath.Join(l.segments, ".tmp-seg-test"), vectorsFileName, graphFileName)
	orphanObject := "seg-" + strings.Repeat("a", 64)
	makeKnownContainer(t, l.segment(orphanObject), vectorsFileName, graphFileName)
	rootTemp := filepath.Join(root, ".tmp-current-test")
	if err := os.WriteFile(rootTemp, []byte("temp"), 0o600); err != nil {
		t.Fatal(err)
	}

	unknownRoot := filepath.Join(root, "notes.txt")
	unknownGeneration := filepath.Join(l.generations, "keep-me")
	unknownObject := filepath.Join(l.segments, "keep-me")
	for _, path := range []string{unknownRoot, unknownGeneration, unknownObject} {
		if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	blockedGeneration := l.generation(3)
	makeKnownContainer(t, blockedGeneration, manifestFileName, stateFileName, "unknown")
	symlink := filepath.Join(l.segments, ".tmp-seg-link")
	if err := os.Symlink(unknownRoot, symlink); err != nil {
		t.Fatal(err)
	}

	result, err := Prune(ctx, root, PruneOptions{RetainGenerations: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result != (PruneResult{RemovedGenerations: 1, RemovedSegmentObjects: 1, RemovedTemporaryEntries: 3}) {
		t.Fatalf("result = %+v", result)
	}
	for _, path := range []string{l.generation(2), filepath.Join(l.generations, ".tmp-gen-test"), filepath.Join(l.segments, ".tmp-seg-test"), l.segment(orphanObject), rootTemp} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("removed path %q error = %v, want os.ErrNotExist", path, err)
		}
	}
	for _, path := range []string{unknownRoot, unknownGeneration, unknownObject, blockedGeneration, symlink} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("preserved path %q: %v", path, err)
		}
	}
}

func TestPruneCancellationDoesNotDelete(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	generations := publishTestGenerations(t, context.Background(), root, newTestIndex(t), 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := Prune(ctx, root, PruneOptions{RetainGenerations: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Prune error = %v, want context.Canceled", err)
	}
	for _, generation := range generations {
		if err := validateDirectory(newLayout(root).generation(generation.ID)); err != nil {
			t.Fatalf("generation %d was changed: %v", generation.ID, err)
		}
	}
}

func TestPruneRequiresExclusiveStoreLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	generations := publishTestGenerations(t, ctx, root, newTestIndex(t), 2)
	store, err := Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := Prune(ctx, root, PruneOptions{RetainGenerations: 1}); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("Prune error = %v, want ErrStoreLocked", err)
	}
	for _, generation := range generations {
		if err := validateDirectory(newLayout(root).generation(generation.ID)); err != nil {
			t.Fatalf("generation %d was changed: %v", generation.ID, err)
		}
	}
}

func TestPruneRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	publishTestGenerations(t, context.Background(), root, newTestIndex(t), 1)
	tests := []struct {
		name    string
		options PruneOptions
	}{
		{name: "zero retention", options: PruneOptions{}},
		{name: "negative retention", options: PruneOptions{RetainGenerations: -1}},
		{name: "durability", options: PruneOptions{RetainGenerations: 1, Durability: DurabilityMode(255)}},
		{name: "limits", options: PruneOptions{RetainGenerations: 1, Limits: Limits{MaxFileBytes: ^uint64(0)}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Prune(context.Background(), root, tt.options); err == nil {
				t.Fatal("Prune error = nil, want error")
			}
		})
	}
}

func publishTestGenerations(t *testing.T, ctx context.Context, root string, index *semantic.Index, count int) []Generation {
	t.Helper()

	result := make([]Generation, 0, count)
	expected := Generation{}
	for range count {
		generation, err := Publish(ctx, root, index, PublishOptions{
			Durability: DurabilityAsynchronous, ExpectedGeneration: &expected,
		})
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, generation)
		expected = generation
	}
	return result
}

func addTestSegment(t *testing.T, ctx context.Context, index *semantic.Index) {
	t.Helper()

	err := index.Add(ctx, "doc", []semantic.EncodedChunk{{
		Ref:    chunk.Ref{ID: "chunk", DocID: fts.DocID("doc"), Field: fts.DefaultField, EndByte: 1},
		Vector: []float32{1, 2},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func makeKnownContainer(t *testing.T, path string, names ...string) {
	t.Helper()

	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(path, name), []byte("test"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
