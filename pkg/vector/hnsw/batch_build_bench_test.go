package hnsw

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func BenchmarkBuildMatrixCosineBatchLarge(b *testing.B) {
	const (
		rows       = 10_000
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
}
