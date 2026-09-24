package vectorsearch

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	flat "github.com/dariasmyr/fts-engine/benchmarks/internal/vectorsearch/flat"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist"
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
			source := benchmarkFlatReader(b, values, vector.MetricL2Squared)
			options := hnsw.BuildOptions{BuildConfig: benchmarkBuildConfig(dimensions, rows, vector.MetricL2Squared), SearchConfig: benchmarkSearchConfig(rows)}
			b.ReportAllocs()
			b.SetBytes(int64(rows * dimensions * 4))
			b.ResetTimer()
			for b.Loop() {
				if _, err := hnsw.BuildSearcher(context.Background(), source.VectorSource(), options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkOpenSearcher(b *testing.B) {
	const rows, dimensions = 10_000, 32
	values := make([][]float32, rows)
	for row := range values {
		values[row] = make([]float32, dimensions)
		for dimension := range dimensions {
			values[row][dimension] = float32((row+11)*(dimension+5)%103) / 103
		}
	}
	source := benchmarkFlatReader(b, values, vector.MetricL2Squared)
	reader := benchmarkBuild(b, source.VectorSource(), dimensions, rows, vector.MetricL2Squared)
	_, vectorMetadata, err := semanticpersist.MarshalSource(source.VectorSource(), source.MaxK())
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
		if _, _, err := hnsw.OpenSearcherContext(context.Background(), bytes.NewReader(graphData), source.VectorSource(), reference, hnsw.DefaultGraphLimits()); err != nil {
			b.Fatal(err)
		}
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
	source := benchmarkFlatReader(b, values, vector.MetricL2Squared)
	reader := benchmarkBuild(b, source.VectorSource(), dimensions, rows, vector.MetricL2Squared)
	query := make([]float32, dimensions)
	for name, searcher := range map[string]vector.Searcher{"ann": reader, "exact_reference": source} {
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

func benchmarkFlatReader(t testing.TB, values [][]float32, metric vector.Metric) *flat.Searcher {
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

func benchmarkBuild(t testing.TB, source vector.PreparedVectorSource, dimensions, count int, metric vector.Metric) *hnsw.Searcher {
	t.Helper()
	reader, err := hnsw.BuildSearcher(context.Background(), source, hnsw.BuildOptions{
		BuildConfig: benchmarkBuildConfig(dimensions, count, metric), SearchConfig: benchmarkSearchConfig(count),
	})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func readPreparedVector(source vector.PreparedVectorSource, ordinal vector.Ordinal) ([]float32, bool) {
	if source == nil || uint64(ordinal) >= uint64(source.Len()) {
		return nil, false
	}
	value := make([]float32, source.Dimensions())
	if err := source.ReadVectorInto(context.Background(), ordinal, value); err != nil {
		return nil, false
	}
	return value, true
}
