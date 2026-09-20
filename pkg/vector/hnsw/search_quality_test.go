package hnsw_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func recallAtK(got, want []vector.Hit) float64 {
	if len(want) == 0 {
		return 1
	}
	ordinals := make(map[vector.Ordinal]struct{}, len(got))
	for _, hit := range got {
		ordinals[hit.Ordinal] = struct{}{}
	}
	matched := 0
	for _, hit := range want {
		if _, ok := ordinals[hit.Ordinal]; ok {
			matched++
		}
	}
	return float64(matched) / float64(len(want))
}

func TestDifferentialRecallAgainstFlatUsingPublicAPIs(t *testing.T) {
	const rows, dimensions, k = 256, 8, 10
	rng := rand.New(rand.NewPCG(11, 29))
	values := make([][]float32, rows)
	for row := range values {
		values[row] = make([]float32, dimensions)
		for dimension := range dimensions {
			values[row][dimension] = rng.Float32()*2 - 1
		}
	}
	flatReader := testFlatReader(t, values, vector.MetricL2Squared)
	reader := testBuild(t, flatReader.VectorSource(), dimensions, rows, vector.MetricL2Squared)
	var totalRecall float64
	for range 20 {
		query := make([]float32, dimensions)
		for dimension := range dimensions {
			query[dimension] = rng.Float32()*2 - 1
		}
		exact, err := flatReader.Search(context.Background(), query, k, vector.SearchOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ann, err := reader.Search(context.Background(), query, k, vector.SearchOptions{EfSearch: 64, VisitLimit: rows})
		if err != nil {
			t.Fatal(err)
		}
		totalRecall += recallAtK(ann.Hits, exact.Hits)
	}
	if meanRecall := totalRecall / 20; meanRecall < 0.9 {
		t.Fatalf("mean recall@%d = %.3f, want >= 0.9", k, meanRecall)
	}
}
