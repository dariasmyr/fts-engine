package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestPruneRetainsCurrentAndLatestValidGenerations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	generations := saveTestGenerations(t, ctx, root, index, 3)

	result, err := pruneForTest(ctx, root, 2, Config{})
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
	if _, _, err := Load(ctx, root, LoadConfig{}); err != nil {
		t.Fatal(err)
	}
}

func TestPruneSkipsCorruptGenerationWhenSelectingRetention(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	generations := saveTestGenerations(t, ctx, root, newTestIndex(t), 3)
	if err := os.WriteFile(newLayout(root).manifest(generations[1].ID), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := pruneForTest(ctx, root, 2, Config{})
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
	saveTestGenerations(t, ctx, root, index, 2)

	entries, err := os.ReadDir(newLayout(root).segments)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !validObjectID(entries[0].Name()) {
		t.Fatalf("segment entries = %v, want one object", entries)
	}
	objectPath := newLayout(root).segment(entries[0].Name())

	result, err := pruneForTest(ctx, root, 1, Config{})
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
	generations := saveTestGenerations(t, ctx, root, newTestIndex(t), 2)
	if err := os.WriteFile(newLayout(root).manifest(generations[1].ID), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := pruneForTest(ctx, root, 1, Config{}); !errors.Is(err, ErrCorrupt) {
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
	saveTestGenerations(t, ctx, root, newTestIndex(t), 1)
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

	result, err := pruneForTest(ctx, root, 1, Config{})
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
	generations := saveTestGenerations(t, context.Background(), root, newTestIndex(t), 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := pruneForTest(ctx, root, 1, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Prune error = %v, want context.Canceled", err)
	}
	for _, generation := range generations {
		if err := validateDirectory(newLayout(root).generation(generation.ID)); err != nil {
			t.Fatalf("generation %d was changed: %v", generation.ID, err)
		}
	}
}

func TestPruneRequiresExclusiveOperationLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	generations := saveTestGenerations(t, ctx, root, newTestIndex(t), 2)
	lock, err := storage.AcquireOperationLock(newLayout(root).lock)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	if _, err := pruneForTest(ctx, root, 1, Config{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("Prune error = %v, want ErrLocked", err)
	}
	for _, generation := range generations {
		if err := validateDirectory(newLayout(root).generation(generation.ID)); err != nil {
			t.Fatalf("generation %d was changed: %v", generation.ID, err)
		}
	}
}

func TestLoadRequiresExclusiveOperationLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	saveTestGenerations(t, ctx, root, newTestIndex(t), 1)
	lock, err := storage.AcquireOperationLock(newLayout(root).lock)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	if _, _, err := Load(ctx, root, LoadConfig{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("Load() error = %v, want ErrLocked", err)
	}
}

func TestSaveRequiresExclusiveOperationLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	generation := saveTestGenerations(t, ctx, root, index, 1)[0]
	lock, err := storage.AcquireOperationLock(newLayout(root).lock)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	_, err = Save(ctx, root, index, SaveConfig{ExpectedGeneration: &generation})
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("Save() error = %v, want ErrLocked", err)
	}
}

func TestPruneRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	saveTestGenerations(t, context.Background(), root, newTestIndex(t), 1)
	tests := []struct {
		name      string
		retention int
		limits    Config
	}{
		{name: "zero retention"},
		{name: "negative retention", retention: -1},
		{name: "limits", retention: 1, limits: Config{MaxFileBytes: ^uint64(0)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := pruneForTest(context.Background(), root, tt.retention, tt.limits); err == nil {
				t.Fatal("Prune error = nil, want error")
			}
		})
	}
}

func saveTestGenerations(t *testing.T, ctx context.Context, root string, index *semantic.Index, count int) []Generation {
	t.Helper()

	result := make([]Generation, 0, count)
	expected := Generation{}
	for range count {
		generation, err := Save(ctx, root, index, SaveConfig{ExpectedGeneration: &expected})
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, generation)
		expected = generation
	}
	return result
}

func pruneForTest(
	ctx context.Context,
	root string,
	retainGenerations int,
	limits Config,
) (PruneResult, error) {
	return Prune(ctx, root, PruneConfig{
		RetainGenerations: retainGenerations,
		Config:            limits,
	})
}

func newTestIndex(t *testing.T) *semantic.Index {
	t.Helper()

	index, err := semantic.NewIndex(semantic.SemanticConfig{
		Model: semantic.ModelConfig{ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 16},
		Index: semantic.IndexConfig{
			Metric: vector.MetricL2Squared,
			Limits: semantic.LimitsConfig{MaxLiveVectors: 16, MaxChunksPerDocument: 2},
			Search: semantic.SearchConfig{MaxResults: 4, MaxCandidateChunks: 16, MaxChunksPerResult: 2},
		},
		Chunking: semantic.ChunkingConfig{Kind: semantic.ChunkingKindExternal},
	})
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func addTestSegment(t *testing.T, ctx context.Context, index *semantic.Index) {
	t.Helper()

	err := index.AddDocument(ctx, "doc", []semantic.ChunkEmbedding{{
		Ref:       semantic.Ref{ID: "chunk", DocID: fts.DocID("doc"), Field: fts.DefaultField, EndByte: 1},
		Embedding: []float32{1, 2},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Commit(ctx); err != nil {
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
