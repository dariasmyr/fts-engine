package hnsw

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

var buildBenchmarkIndex *Index

// BenchmarkBuildMatrix measures the temporary HNSW build allocation separately
// from the already-materialized immutable source matrix.
func BenchmarkBuildMatrix(b *testing.B) {
	tests := []struct {
		rows       int
		dimensions int
	}{
		{rows: 1_000, dimensions: 384},
		{rows: 1_000, dimensions: 768},
		{rows: 4_000, dimensions: 384},
	}
	for _, test := range tests {
		name := fmt.Sprintf("rows=%d/dimensions=%d", test.rows, test.dimensions)
		b.Run(name, func(b *testing.B) {
			source := newBuildBenchmarkStore(b, test.rows, test.dimensions)
			config := Config{
				Build:  BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
				Search: SearchConfig{EfSearch: 32, VisitLimit: test.rows},
			}
			b.ReportAllocs()
			for b.Loop() {
				index, err := Build(context.Background(), source, config)
				if err != nil {
					b.Fatal(err)
				}
				buildBenchmarkIndex = index
			}
			b.ReportMetric(float64(test.rows*test.dimensions*4), "source-bytes")
		})
	}
}

func BenchmarkBuildMatrixCosine(b *testing.B) {
	const (
		rows       = 1_000
		dimensions = 768
	)
	source := newBuildBenchmarkStoreForMetric(b, rows, dimensions, vector.MetricCosine)
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
		Search: SearchConfig{EfSearch: 32, VisitLimit: rows},
	}
	b.ReportAllocs()
	for b.Loop() {
		index, err := Build(context.Background(), source, config)
		if err != nil {
			b.Fatal(err)
		}
		buildBenchmarkIndex = index
	}
	b.ReportMetric(float64(rows*dimensions*4), "source-bytes")
}

func newBuildBenchmarkStore(tb testing.TB, rows, dimensions int) *vector.Matrix {
	return newBuildBenchmarkStoreForMetric(tb, rows, dimensions, vector.MetricL2Squared)
}

func newBuildBenchmarkStoreForMetric(
	tb testing.TB,
	rows, dimensions int,
	metric vector.Metric,
) *vector.Matrix {
	tb.Helper()
	spec, err := vector.NewSpec(dimensions, metric)
	if err != nil {
		tb.Fatal(err)
	}
	values := make([]float32, rows*dimensions)
	for row := range rows {
		for dimension := range dimensions {
			values[row*dimensions+dimension] = float32(math.Sin(float64((row + 1) * (dimension + 1))))
		}
		if err := spec.PrepareInto(
			values[row*dimensions:(row+1)*dimensions],
			values[row*dimensions:(row+1)*dimensions],
		); err != nil {
			tb.Fatal(err)
		}
	}
	store, err := newCanonicalTestMatrix(spec.Dimensions(), spec.Metric(), values)
	if err != nil {
		tb.Fatal(err)
	}
	return store
}
