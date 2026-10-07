package vectorsearch

import (
	"context"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/vectorsearch/exact"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
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
			oracle := benchmarkExactOracle(b, values, vector.MetricL2Squared)
			build := benchmarkBuildConfig(dimensions, rows, vector.MetricL2Squared)
			search := benchmarkSearchConfig(rows)
			b.ReportAllocs()
			b.SetBytes(int64(rows * dimensions * 4))
			b.ResetTimer()
			for b.Loop() {
				if _, err := hnsw.Build(context.Background(), oracle.Store(), build, search); err != nil {
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
	oracle := benchmarkExactOracle(b, values, vector.MetricL2Squared)
	reader := benchmarkBuild(b, oracle.Store(), dimensions, rows, vector.MetricL2Squared)
	query := make([]float32, dimensions)
	b.Run("ann", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := reader.Search(context.Background(), query, 10, hnsw.SearchOptions{EfSearch: 64}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("exact_reference", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := oracle.Search(context.Background(), query, 10, exact.Options{}); err != nil {
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

func benchmarkExactOracle(t testing.TB, values [][]float32, metric vector.Metric) *exact.Oracle {
	t.Helper()
	oracle, err := exact.New(values, len(values[0]), metric, len(values))
	if err != nil {
		t.Fatal(err)
	}
	return oracle
}

func benchmarkBuild(t testing.TB, source vector.PreparedVectorStore, dimensions, count int, metric vector.Metric) *hnsw.Index {
	t.Helper()
	reader, err := hnsw.Build(
		context.Background(),
		source,
		benchmarkBuildConfig(dimensions, count, metric),
		benchmarkSearchConfig(count),
	)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func readPreparedVector(source vector.PreparedVectorStore, ordinal vector.Ordinal) ([]float32, bool) {
	if source == nil || uint64(ordinal) >= uint64(source.Len()) {
		return nil, false
	}
	value := make([]float32, source.Dimensions())
	if err := source.ReadVectorInto(context.Background(), ordinal, value); err != nil {
		return nil, false
	}
	return value, true
}
