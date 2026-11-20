package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// This example uses HNSW directly with its in-memory vector matrix.
func main() {
	ctx := context.Background()
	// Matrix is the single in-memory vector representation. It validates,
	// prepares, and copies rows into contiguous float32 storage.
	matrix, err := vector.NewMatrix(2, vector.MetricL2Squared, [][]float32{
		{0, 0},
		{1, 0},
		{0, 2},
	})
	must(err)

	index, err := hnsw.Build(ctx, matrix, hnsw.Config{
		Build:  hnsw.BuildConfig{MaxNeighbors: 2, EfConstruction: 4, Seed: 1},
		Search: hnsw.SearchConfig{EfSearch: 3, VisitLimit: 3},
	})
	must(err)

	result, err := index.Search(ctx, []float32{0.9, 0}, 2, hnsw.QueryConfig{})
	must(err)
	for _, hit := range result.Hits {
		fmt.Printf("ordinal=%d distance=%.2f\n", hit.Ordinal, hit.Distance)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
