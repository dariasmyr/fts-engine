package vectorsearch

import (
	"context"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/benchmarks/internal/vectorsearch/exact"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

func BenchmarkBuildPreparedSource(b *testing.B) {
	for _, rows := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			const dimensions = 32
			values := make([][]float32, rows)
			for row := range values {
				values[row] = make([]float32, dimensions)
				for dimension := range dimensions {
					values[row][dimension] = float32((row+1)*(dimension+3)%101) / 101
				}
			}
			searcher := benchmarkExactSearcher(b, values, vector.MetricL2Squared)
			config := hnsw.Config{
				Build:  benchmarkBuildConfig(dimensions, rows, vector.MetricL2Squared),
				Search: benchmarkSearchConfig(rows),
			}
			b.ReportAllocs()
			b.SetBytes(int64(rows * dimensions * 4))
			b.ResetTimer()
			for b.Loop() {
				if _, err := hnsw.Build(context.Background(), searcher.Store(), config); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkANNAndExactReference(b *testing.B) {
	const rows, dimensions = 10_000, 32
	values := make([][]float32, rows)
	for row := range values {
		values[row] = make([]float32, dimensions)
		for dimension := range dimensions {
			values[row][dimension] = float32((row+7)*(dimension+1)%97) / 97
		}
	}
	searcher := benchmarkExactSearcher(b, values, vector.MetricL2Squared)
	reader := benchmarkBuild(b, searcher.Store(), dimensions, rows, vector.MetricL2Squared)
	query := make([]float32, dimensions)
	b.Run("ann", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := reader.Search(context.Background(), query, 10, hnsw.QueryConfig{EfSearch: 64}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("exact_reference", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := searcher.Search(context.Background(), query, 10, exact.Options{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkBuildConfig(dimensions, count int, metric vector.Metric) hnsw.BuildConfig {
	return hnsw.BuildConfig{
		MaxNeighbors: 4, EfConstruction: 32, Seed: 17,
	}
}

func benchmarkSearchConfig(count int) hnsw.SearchConfig {
	limit := max(1, count)
	return hnsw.SearchConfig{
		EfSearch:   min(8, limit),
		VisitLimit: limit,
	}
}

func benchmarkExactSearcher(t testing.TB, values [][]float32, metric vector.Metric) *exact.Searcher {
	t.Helper()
	searcher, err := exact.New(values, len(values[0]), metric, len(values))
	if err != nil {
		t.Fatal(err)
	}
	return searcher
}

func benchmarkBuild(t testing.TB, source vector.PreparedVectorStore, dimensions, count int, metric vector.Metric) *hnsw.Index {
	t.Helper()
	reader, err := hnsw.Build(
		context.Background(),
		source,
		hnsw.Config{
			Build:  benchmarkBuildConfig(dimensions, count, metric),
			Search: benchmarkSearchConfig(count),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}
