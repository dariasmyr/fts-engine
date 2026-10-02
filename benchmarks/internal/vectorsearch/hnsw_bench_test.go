package vectorsearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	flat "github.com/dariasmyr/fts-engine/benchmarks/internal/vectorsearch/flat"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
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
			flatIndex := benchmarkFlatIndex(b, values, vector.MetricL2Squared)
			options := hnsw.BuildOptions{Build: benchmarkBuildConfig(dimensions, rows, vector.MetricL2Squared), Search: benchmarkSearchConfig(rows)}
			b.ReportAllocs()
			b.SetBytes(int64(rows * dimensions * 4))
			b.ResetTimer()
			for b.Loop() {
				if _, err := hnsw.Build(context.Background(), flatIndex.Vectors(), options); err != nil {
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
	flatIndex := benchmarkFlatIndex(b, values, vector.MetricL2Squared)
	reader := benchmarkBuild(b, flatIndex.Vectors(), dimensions, rows, vector.MetricL2Squared)
	reference, err := benchmarkVectorReference(context.Background(), flatIndex.Vectors())
	if err != nil {
		b.Fatal(err)
	}
	var graphData bytes.Buffer
	_, err = hnsw.WriteGraph(context.Background(), &graphData, reader, reference)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(graphData.Len()))
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := hnsw.OpenGraph(context.Background(), bytes.NewReader(graphData.Bytes()), flatIndex.Vectors(), reference, hnsw.DefaultGraphLimits()); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkVectorReference(ctx context.Context, source vectorstore.PreparedVectorStore) (hnsw.VectorFileReference, error) {
	hash := sha256.New()
	values := make([]float32, source.Dimensions())
	var encoded [4]byte
	for ordinal := range source.Len() {
		if err := source.ReadVectorInto(ctx, vector.Ordinal(ordinal), values); err != nil {
			return hnsw.VectorFileReference{}, err
		}
		for _, value := range values {
			binary.LittleEndian.PutUint32(encoded[:], math.Float32bits(value))
			_, _ = hash.Write(encoded[:])
		}
	}
	result := hnsw.VectorFileReference{Size: uint64(source.Len()) * uint64(source.Dimensions()) * 4}
	copy(result.SHA256[:], hash.Sum(nil))
	return result, nil
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
	flatIndex := benchmarkFlatIndex(b, values, vector.MetricL2Squared)
	reader := benchmarkBuild(b, flatIndex.Vectors(), dimensions, rows, vector.MetricL2Squared)
	query := make([]float32, dimensions)
	for name, index := range map[string]vector.Index{"ann": reader, "exact_reference": flatIndex} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := index.Search(context.Background(), query, 10, vector.SearchOptions{EfSearch: 64}); err != nil {
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

func benchmarkFlatIndex(t testing.TB, values [][]float32, metric vector.Metric) *flat.FlatIndex {
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

func benchmarkBuild(t testing.TB, source vectorstore.PreparedVectorStore, dimensions, count int, metric vector.Metric) *hnsw.Index {
	t.Helper()
	reader, err := hnsw.Build(context.Background(), source, hnsw.BuildOptions{
		Build: benchmarkBuildConfig(dimensions, count, metric), Search: benchmarkSearchConfig(count),
	})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func readPreparedVector(source vectorstore.PreparedVectorStore, ordinal vector.Ordinal) ([]float32, bool) {
	if source == nil || uint64(ordinal) >= uint64(source.Len()) {
		return nil, false
	}
	value := make([]float32, source.Dimensions())
	if err := source.ReadVectorInto(context.Background(), ordinal, value); err != nil {
		return nil, false
	}
	return value, true
}
