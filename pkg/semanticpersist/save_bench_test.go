package semanticpersist

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

const (
	persistenceBenchmarkDimensions = 16
	persistenceBenchmarkDocuments  = 256
)

func BenchmarkSave(b *testing.B) {
	ctx := context.Background()
	index := newPersistenceBenchmarkIndex(b)
	parent := b.TempDir()

	b.ReportAllocs()
	for n := range b.N {
		b.StopTimer()
		root := filepath.Join(parent, fmt.Sprintf("save-%06d", n))
		if err := os.Mkdir(root, 0o700); err != nil {
			b.Fatal(err)
		}
		expected := Generation{}
		b.StartTimer()
		if _, err := Save(ctx, root, index, SaveConfig{
			ExpectedGeneration: &expected,
		}); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := os.RemoveAll(root); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLoadService(b *testing.B) {
	ctx := context.Background()
	root := filepath.Join(b.TempDir(), "service")
	if err := os.Mkdir(root, 0o700); err != nil {
		b.Fatal(err)
	}
	client := persistenceBenchmarkClient(b)
	service, err := semantic.New(client, persistenceBenchmarkConfig(b, client))
	if err != nil {
		b.Fatal(err)
	}
	for n := range persistenceBenchmarkDocuments {
		docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
		if err := service.AddDocument(ctx, fts.Document{
			ID: docID,
			Fields: map[string]fts.FieldData{
				fts.DefaultField: {Text: string(docID)},
			},
		}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := CommitAndSave(ctx, root, service.Index(), SaveConfig{
		ExpectedGeneration: &Generation{},
	}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		loaded, _, err := LoadService(
			ctx,
			root,
			client,
			LoadConfig{},
		)
		if err != nil {
			b.Fatal(err)
		}
		if loaded == nil {
			b.Fatal("LoadService() returned nil service")
		}
	}
}

func BenchmarkCommitAndSave(b *testing.B) {
	ctx := context.Background()
	root := filepath.Join(b.TempDir(), "service")
	client := persistenceBenchmarkClient(b)
	service, err := semantic.New(client, persistenceBenchmarkConfig(b, client))
	if err != nil {
		b.Fatal(err)
	}
	for n := range persistenceBenchmarkDocuments {
		docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
		if err := service.AddDocument(ctx, fts.Document{
			ID: docID,
			Fields: map[string]fts.FieldData{
				fts.DefaultField: {Text: string(docID)},
			},
		}); err != nil {
			b.Fatal(err)
		}
	}
	generation, err := CommitAndSave(ctx, root, service.Index(), SaveConfig{
		ExpectedGeneration: &Generation{},
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		next, err := CommitAndSave(ctx, root, service.Index(), SaveConfig{
			ExpectedGeneration: &generation,
		})
		if err != nil {
			b.Fatal(err)
		}
		generation = next
	}
}

func newPersistenceBenchmarkIndex(tb testing.TB) *semantic.Index {
	tb.Helper()
	index, err := semantic.NewIndex(semantic.SemanticConfig{
		Model: semantic.ModelConfig{
			ID: "deterministic", Revision: "v1", Dimensions: persistenceBenchmarkDimensions, ContextTokens: 16,
		},
		Index: semantic.IndexConfig{
			Metric: vector.MetricL2Squared,
			Limits: semantic.LimitsConfig{MaxLiveVectors: persistenceBenchmarkDocuments, MaxChunksPerDocument: 1},
			Search: semantic.SearchConfig{MaxResults: 10, MaxCandidateChunks: 64, MaxChunksPerResult: 1},
		},
		Chunking: semantic.ChunkingConfig{Kind: semantic.ChunkingKindExternal},
	})
	if err != nil {
		tb.Fatal(err)
	}
	for n := range persistenceBenchmarkDocuments {
		docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
		values := make([]float32, persistenceBenchmarkDimensions)
		for dimension := range values {
			values[dimension] = float32(math.Sin(float64((n + 1) * (dimension + 1))))
		}
		if err := index.AddDocument(context.Background(), docID, []semantic.ChunkEmbedding{{
			Ref: semantic.Ref{
				ID: semantic.ChunkID(fmt.Sprintf("chunk-%06d", n)), DocID: docID,
				Field: fts.DefaultField, EndByte: 1,
			},
			Embedding: values,
		}}); err != nil {
			tb.Fatal(err)
		}
	}
	if err := index.Commit(context.Background()); err != nil {
		tb.Fatal(err)
	}
	return index
}

func persistenceBenchmarkClient(tb testing.TB) embedding.Client {
	tb.Helper()
	client, err := embedding.NewClient(embedding.Model{
		ID: "persistence-benchmark", Revision: "v1",
		Dimensions: persistenceBenchmarkDimensions, ContextTokens: 512,
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for n, text := range texts {
			seed := 0
			for _, value := range []byte(text) {
				seed = (seed*31 + int(value)) % 1009
			}
			values := make([]float32, persistenceBenchmarkDimensions)
			for dimension := range values {
				values[dimension] = float32((seed+1)*(dimension+3)%101+1) / 101
			}
			vectors[n] = values
		}
		return vectors, nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return client
}

func persistenceBenchmarkConfig(tb testing.TB, client embedding.Client) semantic.SemanticConfig {
	tb.Helper()
	config, err := semantic.DefaultSemanticConfig(client.Model())
	if err != nil {
		tb.Fatal(err)
	}
	return config
}
