package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/demo/internal/config"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
)

func TestRunSemanticCompactionSavesThenVerifiesAndPrunesOnSkippedRun(t *testing.T) {
	ctx := t.Context()
	semanticService := newCompactionTestService(t)
	addCompactionTestDocument(t, semanticService, "one", "first")
	if err := semanticService.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	addCompactionTestDocument(t, semanticService, "two", "second")
	if err := semanticService.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := semanticService.Statistics().Segments; got != 2 {
		t.Fatalf("segments before save = %d, want 2", got)
	}

	root := filepath.Join(t.TempDir(), "semantic")
	generation, err := semanticpersist.SaveService(ctx, root, semanticService, semanticpersist.SaveOptions{
		ExpectedGeneration: &semanticpersist.Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeState := &semanticRuntime{
		service:           semanticService,
		generation:        generation,
		loadedFromDisk:    true,
		persistencePath:   root,
		persistence:       true,
		persistenceLimits: semanticpersist.DefaultLimits(),
		compaction: config.SemanticCompactionConfig{
			Enabled: true,
			Prune:   false,
		},
	}

	report, err := runSemanticCompaction(
		ctx,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		runtimeState,
	)
	if err != nil {
		t.Fatalf("runSemanticCompaction() error = %v", err)
	}
	if report.compact.statsBefore.Segments != 2 || report.compact.statsAfter.Segments != 1 {
		t.Fatalf("segments before/after = %d/%d, want 2/1",
			report.compact.statsBefore.Segments, report.compact.statsAfter.Segments)
	}
	if report.generation.ID != generation.ID+1 {
		t.Fatalf("generation = %d, want %d", report.generation.ID, generation.ID+1)
	}
	if report.pruneResult.RemovedGenerations != 0 || report.pruneResult.RemovedSegmentObjects != 0 {
		t.Fatalf("prune result = %+v, want no pruning during safe compaction", report.pruneResult)
	}

	entries, err := os.ReadDir(filepath.Join(root, "objects", "segments"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("persisted segment objects = %d, want 3 before verified pruning", len(entries))
	}

	runtimeState.compaction.Prune = true
	second, err := runSemanticCompaction(
		ctx,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		runtimeState,
	)
	if err != nil {
		t.Fatalf("second runSemanticCompaction() error = %v", err)
	}
	if !second.skipped {
		t.Fatal("second runSemanticCompaction() did not skip compacted index")
	}
	if second.generation != report.generation {
		t.Fatalf("second generation = %+v, want unchanged %+v", second.generation, report.generation)
	}
	if second.pruneResult.RemovedGenerations != 1 || second.pruneResult.RemovedSegmentObjects != 2 {
		t.Fatalf("prune result = %+v, want one generation and two segment objects removed", second.pruneResult)
	}
	entries, err = os.ReadDir(filepath.Join(root, "objects", "segments"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("persisted segment objects = %d, want 1 after verified pruning", len(entries))
	}
}

func TestVerifySemanticGenerationRejectsUnexpectedGeneration(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	semanticService := newCompactionTestService(t)
	addCompactionTestDocument(t, semanticService, "one", "first")
	if err := semanticService.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "semantic")
	generation, err := semanticpersist.SaveService(ctx, root, semanticService, semanticpersist.SaveOptions{
		ExpectedGeneration: &semanticpersist.Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = verifySemanticGeneration(
		ctx,
		root,
		semanticpersist.Generation{ID: generation.ID + 1},
		semanticService.Statistics(),
		semanticpersist.DefaultLimits(),
	)
	if err == nil {
		t.Fatal("verifySemanticGeneration() error = nil, want generation mismatch")
	}
}

func TestSemanticIndexIsCompacted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stats semantic.Statistics
		want  bool
	}{
		{name: "empty", stats: semantic.Statistics{}, want: true},
		{name: "one clean segment", stats: semantic.Statistics{Segments: 1}, want: true},
		{name: "one stale segment", stats: semantic.Statistics{Segments: 1, StaleVectors: 1}},
		{name: "multiple segments", stats: semantic.Statistics{Segments: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := semanticIndexIsCompacted(tt.stats); got != tt.want {
				t.Fatalf("semanticIndexIsCompacted(%+v) = %v, want %v", tt.stats, got, tt.want)
			}
		})
	}
}

func newCompactionTestService(t *testing.T) *service.Service {
	t.Helper()
	client, err := embedding.NewClient(embedding.Model{
		ID: "compaction-test", Revision: "v1", Dimensions: 2, ContextTokens: 32,
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i, text := range texts {
			if text == "first" {
				vectors[i] = []float32{1, 1}
			} else {
				vectors[i] = []float32{2, 1}
			}
		}
		return vectors, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	serviceConfig, err := service.DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	semanticService, err := service.New(client, serviceConfig)
	if err != nil {
		t.Fatal(err)
	}
	return semanticService
}

func addCompactionTestDocument(t *testing.T, semanticService *service.Service, id, text string) {
	t.Helper()
	if err := semanticService.AddDocument(t.Context(), fts.Document{
		ID: fts.DocID(id),
		Fields: map[string]fts.FieldData{
			fts.DefaultField: {Text: text},
		},
	}); err != nil {
		t.Fatal(err)
	}
}
