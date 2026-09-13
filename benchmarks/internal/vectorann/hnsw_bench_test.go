package vectorann

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/flat"
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
			source := benchmarkFlatReader(b, values, vector.MetricL2Squared)
			options := hnsw.BuildOptions{BuildConfig: benchmarkBuildConfig(dimensions, rows, vector.MetricL2Squared), SearchConfig: benchmarkSearchConfig(rows)}
			b.ReportAllocs()
			b.SetBytes(int64(rows * dimensions * 4))
			b.ResetTimer()
			for b.Loop() {
				if _, err := hnsw.Build(context.Background(), source, options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkOpenGraph(b *testing.B) {
	const rows, dimensions = 10_000, 32
	values := make([][]float32, rows)
	for row := range values {
		values[row] = make([]float32, dimensions)
		for dimension := range dimensions {
			values[row][dimension] = float32((row+11)*(dimension+5)%103) / 103
		}
	}
	source := benchmarkFlatReader(b, values, vector.MetricL2Squared)
	reader := benchmarkBuild(b, source, dimensions, rows, vector.MetricL2Squared)
	_, vectorMetadata, err := flat.Marshal(source)
	if err != nil {
		b.Fatal(err)
	}
	reference := hnsw.VectorFileReference{Size: vectorMetadata.Size, SHA256: vectorMetadata.SHA256}
	graphData, _, err := hnsw.MarshalGraph(reader, reference)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(graphData)))
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := hnsw.OpenGraphContext(context.Background(), bytes.NewReader(graphData), source, reference, hnsw.DefaultGraphLimits()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkANNAndExactFallback(b *testing.B) {
	const rows, dimensions = 10_000, 32
	values := make([][]float32, rows)
	for row := range values {
		values[row] = make([]float32, dimensions)
		for dimension := range dimensions {
			values[row][dimension] = float32((row+7)*(dimension+1)%97) / 97
		}
	}
	source := benchmarkFlatReader(b, values, vector.MetricL2Squared)
	reader := benchmarkBuild(b, source, dimensions, rows, vector.MetricL2Squared)
	exact, err := reader.WithExactFallback(hnsw.ExactFallbackPolicy{MaxPhysicalRows: rows})
	if err != nil {
		b.Fatal(err)
	}
	query := make([]float32, dimensions)
	for name, searcher := range map[string]vector.Searcher{"ann": reader, "exact": exact} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := searcher.Search(context.Background(), query, 10, vector.SearchOptions{EfSearch: 64}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkBuildConfig(dimensions, count int, metric vector.Metric) hnsw.BuildConfig {
	return hnsw.BuildConfig{
		Dimensions: dimensions, Metric: metric, MaxVectors: max(1, count), MaxVectorBytes: uint64(max(1, dimensions*count*4)),
		MaxNeighbors: 4, EfConstruction: 32, Seed: 17,
	}
}

func benchmarkSearchConfig(count int) hnsw.SearchConfig {
	limit := max(1, count)
	return hnsw.SearchConfig{
		DefaultEfSearch: min(8, limit), MaxEfSearch: limit,
		DefaultVisitLimit: limit, MaxVisitLimit: limit, MaxK: limit,
	}
}

func benchmarkFlatReader(t testing.TB, values [][]float32, metric vector.Metric) *flat.Reader {
	t.Helper()
	index, err := flat.New(flat.Config{Dimensions: len(values[0]), Metric: metric, MaxVectors: len(values), MaxK: len(values)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.AppendBatch(values); err != nil {
		t.Fatal(err)
	}
	return index.Freeze()
}

func benchmarkBuild(t testing.TB, source hnsw.PreparedVectorSource, dimensions, count int, metric vector.Metric) *hnsw.Reader {
	t.Helper()
	reader, err := hnsw.Build(context.Background(), source, hnsw.BuildOptions{
		BuildConfig: benchmarkBuildConfig(dimensions, count, metric), SearchConfig: benchmarkSearchConfig(count),
	})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}
