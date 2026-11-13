package semanticpersist

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestValidateImmutableLineage(t *testing.T) {
	t.Parallel()

	index := newTestIndex(t)
	pipeline := pipelineConfigToFormat(service.PipelineConfig{
		Model: embedding.Model{
			ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 128,
			DocumentPrefix: "document: ", QueryPrefix: "query: ",
		},
		Chunking:                chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4},
		EncodingBehaviorVersion: service.CurrentEncodingBehaviorVersion,
		Operational: service.PipelineOperationalLimits{Encoding: service.EncodingLimits{
			MaxFields: 4, MaxSourceBytes: 1024, MaxEmbeddingBytes: 64,
		}},
	})
	base := generationformat.IndexState{
		ID:       [16]byte{1},
		Config:   indexConfigToFormat(index.Config()),
		Pipeline: &pipeline,
	}

	tests := []struct {
		name   string
		mutate func(*generationformat.IndexState)
		want   error
	}{
		{
			name: "index identity",
			mutate: func(state *generationformat.IndexState) {
				state.ID[0]++
			},
			want: ErrIndexIdentityMismatch,
		},
		{
			name: "index dimensions",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Dimensions++
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "index metric",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Metric = uint8(vector.MetricCosine)
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "index build config",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Build.Seed++
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "index live vector limit",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Operational.Capacity.MaxLiveVectors++
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "index segment limit",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Operational.Capacity.MaxSegments++
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "index ingestion limit",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Operational.Ingestion.MaxChunksPerDocument++
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "index result limit",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Operational.Search.MaxResults++
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "index search ceiling",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Operational.Search.MaxEfSearch++
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "index search default",
			mutate: func(state *generationformat.IndexState) {
				state.Config.Operational.SearchDefaults.EfSearch++
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "persistence mode",
			mutate: func(state *generationformat.IndexState) {
				state.Pipeline = nil
			},
			want: ErrPersistenceModeMismatch,
		},
		{
			name: "pipeline model ID",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.Model.ID = "other-model"
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "pipeline model revision",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.Model.Revision = "v2"
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "pipeline document prefix",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.Model.DocumentPrefix = "doc: "
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "pipeline query prefix",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.Model.QueryPrefix = "search: "
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "pipeline chunking",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.Chunking.TargetBytes++
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "pipeline behavior version",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.EncodingBehaviorVersion++
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "pipeline field limit",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.Operational.Encoding.MaxFields++
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "pipeline source limit",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.Operational.Encoding.MaxSourceBytes++
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
		{
			name: "pipeline embedding limit",
			mutate: func(state *generationformat.IndexState) {
				pipeline := *state.Pipeline
				pipeline.Operational.Encoding.MaxEmbeddingBytes++
				state.Pipeline = &pipeline
			},
			want: ErrImmutableConfigMismatch,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			candidate := base
			pipeline := *base.Pipeline
			candidate.Pipeline = &pipeline
			test.mutate(&candidate)
			if err := validateImmutableLineage(base, candidate); !errors.Is(err, test.want) {
				t.Fatalf("validateImmutableLineage() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestSaveRejectsImmutableIndexConfigChangeBeforeWriting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	first, err := Save(ctx, root, index, SaveOptions{ExpectedGeneration: &Generation{}})
	if err != nil {
		t.Fatal(err)
	}

	state, err := index.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state.Config.Operational.Capacity.MaxLiveVectors++
	changed, err := semantic.Restore(ctx, *state)
	if err != nil {
		t.Fatal(err)
	}
	addTestDocumentSegment(t, ctx, changed, "new", []float32{2, 1})

	beforeGenerations := directoryEntryNames(t, newLayout(root).generations)
	beforeSegments := directoryEntryNames(t, newLayout(root).segments)
	if _, err := Save(ctx, root, changed, SaveOptions{ExpectedGeneration: &first}); !errors.Is(err, ErrImmutableConfigMismatch) {
		t.Fatalf("Save() error = %v, want ErrImmutableConfigMismatch", err)
	}
	afterGenerations := directoryEntryNames(t, newLayout(root).generations)
	afterSegments := directoryEntryNames(t, newLayout(root).segments)
	if len(afterGenerations) != len(beforeGenerations) {
		t.Fatalf("generation count after rejected save = %d, want %d", len(afterGenerations), len(beforeGenerations))
	}
	if len(afterSegments) != len(beforeSegments) {
		t.Fatalf("segment count after rejected save = %d, want %d", len(afterSegments), len(beforeSegments))
	}
	_, current, err := Load(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if current != first {
		t.Fatalf("CURRENT generation = %d, want %d", current.ID, first.ID)
	}
}

func TestSaveRejectsPipelineAndPersistenceModeChanges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := newLineageTestClient(t)
	config, err := service.DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	semanticService, err := service.New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("pipeline config", func(t *testing.T) {
		root := t.TempDir()
		first, err := SaveService(ctx, root, semanticService, SaveOptions{ExpectedGeneration: &Generation{}})
		if err != nil {
			t.Fatal(err)
		}
		changedConfig := config
		changedConfig.Pipeline.Operational.Encoding.MaxSourceBytes++
		changedIndex := serviceIndexCopy(t, ctx, semanticService)
		changedService, _, err := service.Load(ctx, lineageTestLoader{state: service.LoadedState{
			Index:        changedIndex,
			Pipeline:     changedConfig.Pipeline,
			GenerationID: 1,
		}}, client)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := CommitAndSaveService(ctx, root, changedService, SaveOptions{ExpectedGeneration: &first}); !errors.Is(err, ErrImmutableConfigMismatch) {
			t.Fatalf("CommitAndSaveService() error = %v, want ErrImmutableConfigMismatch", err)
		}
	})

	t.Run("model backed to raw", func(t *testing.T) {
		root := t.TempDir()
		first, err := SaveService(ctx, root, semanticService, SaveOptions{ExpectedGeneration: &Generation{}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Save(ctx, root, serviceIndexCopy(t, ctx, semanticService), SaveOptions{ExpectedGeneration: &first}); !errors.Is(err, ErrPersistenceModeMismatch) {
			t.Fatalf("Save() error = %v, want ErrPersistenceModeMismatch", err)
		}
	})

	t.Run("raw to model backed", func(t *testing.T) {
		root := t.TempDir()
		first, err := Save(ctx, root, serviceIndexCopy(t, ctx, semanticService), SaveOptions{ExpectedGeneration: &Generation{}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := CommitAndSaveService(ctx, root, semanticService, SaveOptions{ExpectedGeneration: &first}); !errors.Is(err, ErrPersistenceModeMismatch) {
			t.Fatalf("CommitAndSaveService() error = %v, want ErrPersistenceModeMismatch", err)
		}
	})
}

func serviceIndexCopy(t *testing.T, ctx context.Context, semanticService *service.Service) *semantic.Index {
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

type lineageTestLoader struct{ state service.LoadedState }

func (l lineageTestLoader) LoadServiceState(
	ctx context.Context,
	preflight service.RestorePreflight,
) (service.LoadedState, error) {
	if err := ctx.Err(); err != nil {
		return service.LoadedState{}, err
	}
	if err := preflight(service.RestoreMetadata{
		IndexID:      l.state.Index.ID(),
		GenerationID: l.state.GenerationID,
		IndexConfig:  l.state.Index.Config(),
		Pipeline:     l.state.Pipeline,
	}); err != nil {
		return service.LoadedState{}, err
	}
	return l.state, nil
}

func directoryEntryNames(t *testing.T, directory string) []string {
	t.Helper()

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}

func newLineageTestClient(t *testing.T) embedding.Client {
	t.Helper()

	client, err := embedding.NewClient(
		embedding.Model{ID: "lineage-test", Revision: "v1", Dimensions: 2, ContextTokens: 128},
		func(_ context.Context, texts []string) ([][]float32, error) {
			vectors := make([][]float32, len(texts))
			for i := range vectors {
				vectors[i] = []float32{1, 0}
			}
			return vectors, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
