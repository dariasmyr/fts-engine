package semanticpersist

import (
	"context"
	"errors"
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
	serviceConfig := persistenceBenchmarkConfig(b, client)
	service, err := semantic.New(client, serviceConfig)
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
	if _, err := CommitAndSaveService(ctx, root, service, SaveConfig{
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
	generation, err := CommitAndSaveService(ctx, root, service, SaveConfig{
		ExpectedGeneration: &Generation{},
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		next, err := CommitAndSaveService(ctx, root, service, SaveConfig{
			ExpectedGeneration: &generation,
		})
		if err != nil {
			b.Fatal(err)
		}
		generation = next
	}
}

func BenchmarkIncrementalSaveReusedSegments(b *testing.B) {
	for _, segmentCount := range []int{1, 8, 16} {
		b.Run(fmt.Sprintf("segments=%d", segmentCount), func(b *testing.B) {
			ctx := context.Background()
			root := b.TempDir()
			client := persistenceBenchmarkClient(b)
			service := newPersistenceBenchmarkService(b, client, segmentCount)
			generation, err := CommitAndSaveService(ctx, root, service, SaveConfig{
				ExpectedGeneration: &Generation{},
			})
			if err != nil {
				b.Fatal(err)
			}
			before := benchmarkDirectoryBytes(b, root)
			verifiedBytes := benchmarkDirectoryBytes(b, newLayout(root).segments)
			operations := 0
			b.ReportAllocs()
			for b.Loop() {
				next, err := CommitAndSaveService(ctx, root, service, SaveConfig{
					ExpectedGeneration: &generation,
				})
				if err != nil {
					b.Fatal(err)
				}
				generation = next
				operations++
			}
			after := benchmarkDirectoryBytes(b, root)
			b.ReportMetric(float64(segmentCount), "segments")
			b.ReportMetric(float64(after-before)/float64(operations), "retained-bytes/op")
			b.ReportMetric(float64(verifiedBytes), "verified-bytes/op")
		})
	}
}

func BenchmarkLoadServiceSegments(b *testing.B) {
	for _, segmentCount := range []int{1, 8, 16} {
		b.Run(fmt.Sprintf("segments=%d", segmentCount), func(b *testing.B) {
			ctx := context.Background()
			root := b.TempDir()
			client := persistenceBenchmarkClient(b)
			service := newPersistenceBenchmarkService(b, client, segmentCount)
			if _, err := CommitAndSaveService(ctx, root, service, SaveConfig{
				ExpectedGeneration: &Generation{},
			}); err != nil {
				b.Fatal(err)
			}

			b.Run("matching-model", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					loaded, _, err := LoadService(ctx, root, client, LoadConfig{})
					if err != nil {
						b.Fatal(err)
					}
					if loaded == nil {
						b.Fatal("LoadService() returned nil service")
					}
				}
				b.ReportMetric(float64(segmentCount), "segments")
			})

			mismatch := persistenceBenchmarkMismatchClient(b)
			b.Run("mismatched-model", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					_, _, err := LoadService(ctx, root, mismatch, LoadConfig{})
					if !errors.Is(err, semantic.ErrModelServiceUnavailable) {
						b.Fatalf("LoadService() error = %v, want ErrModelServiceUnavailable", err)
					}
				}
				b.ReportMetric(float64(segmentCount), "segments")
			})
		})
	}
}

func newPersistenceBenchmarkIndex(tb testing.TB) *semantic.Index {
	tb.Helper()
	index, err := semantic.NewIndex(semantic.IndexConfig{
		Dimensions: persistenceBenchmarkDimensions,
		Metric:     vector.MetricL2Squared,
		Operational: semantic.OperationalLimits{
			Capacity:  semantic.CapacityLimits{MaxLiveVectors: persistenceBenchmarkDocuments},
			Ingestion: semantic.IngestionLimits{MaxChunksPerDocument: 1},
			Search:    semantic.SearchLimits{MaxResults: 10, MaxCandidateChunks: 64, MaxChunksPerResult: 1},
		},
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

func persistenceBenchmarkMismatchClient(tb testing.TB) embedding.Client {
	tb.Helper()
	client, err := embedding.NewClient(embedding.Model{
		ID: "persistence-benchmark", Revision: "other-revision",
		Dimensions: persistenceBenchmarkDimensions, ContextTokens: 512,
	}, func(context.Context, []string) ([][]float32, error) {
		return nil, errors.New("mismatched benchmark client must not embed")
	})
	if err != nil {
		tb.Fatal(err)
	}
	return client
}

func newPersistenceBenchmarkService(
	tb testing.TB,
	client embedding.Client,
	segmentCount int,
) *semantic.Service {
	tb.Helper()
	config := persistenceBenchmarkConfig(tb, client)
	config.Index.Operational.Capacity.MaxSegments = segmentCount + 1
	service, err := semantic.New(client, config)
	if err != nil {
		tb.Fatal(err)
	}
	documentsPerSegment := persistenceBenchmarkDocuments / segmentCount
	for segment := range segmentCount {
		for offset := range documentsPerSegment {
			n := segment*documentsPerSegment + offset
			docID := fts.DocID(fmt.Sprintf("doc-%06d", n))
			if err := service.AddDocument(context.Background(), fts.Document{
				ID: docID,
				Fields: map[string]fts.FieldData{
					fts.DefaultField: {Text: string(docID)},
				},
			}); err != nil {
				tb.Fatal(err)
			}
		}
		if err := service.Commit(context.Background()); err != nil {
			tb.Fatal(err)
		}
	}
	return service
}

func benchmarkDirectoryBytes(tb testing.TB, root string) int64 {
	tb.Helper()
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return total
}

func persistenceBenchmarkConfig(tb testing.TB, client embedding.Client) semantic.Config {
	tb.Helper()
	config, err := semantic.DefaultConfig(client.Model())
	if err != nil {
		tb.Fatal(err)
	}
	return config
}
