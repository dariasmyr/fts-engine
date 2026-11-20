package hnsw

import (
	"context"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

var searchBenchmarkResult SearchResult

func BenchmarkSearchPreparedAccess(b *testing.B) {
	const (
		rows       = 4_000
		dimensions = 768
	)

	store := newBuildBenchmarkStoreForMetric(b, rows, dimensions, vector.MetricCosine)
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
		Search: SearchConfig{EfSearch: 200, VisitLimit: rows},
	}
	base, err := Build(context.Background(), store, config)
	if err != nil {
		b.Fatal(err)
	}
	queryValues := make([]float32, dimensions)
	for dimension := range dimensions {
		queryValues[dimension] = float32((dimension*17)%101) / 101
	}
	query, err := base.PrepareQuery(queryValues)
	if err != nil {
		b.Fatal(err)
	}

	for _, efSearch := range []int{16, 32, 64, 200} {
		b.Run(fmt.Sprintf("efSearch=%d", efSearch), func(b *testing.B) {
			b.StopTimer()
			request := QueryConfig{EfSearch: efSearch, VisitLimit: rows}
			sample, err := base.SearchPrepared(context.Background(), query, 10, request)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ReportMetric(float64(sample.Stats.DistanceComputations), "distances/op")
			b.ReportMetric(float64(sample.Stats.VisitedNodes), "visited/op")
			b.ReportMetric(0, "copied-bytes/op")
			b.ReportMetric(0, "vector-copies/op")
			b.StartTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					result, err := base.SearchPrepared(context.Background(), query, 10, request)
					if err != nil {
						b.Fatal(err)
					}
					_ = result
				}
			})
		})
	}
}

func BenchmarkSearchPreparedDistanceProfile(b *testing.B) {
	const (
		rows       = 4_000
		dimensions = 768
	)

	store := newBuildBenchmarkStoreForMetric(b, rows, dimensions, vector.MetricCosine)
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 16, EfConstruction: 64, Seed: 1},
		Search: SearchConfig{EfSearch: 200, VisitLimit: rows},
	}
	index, err := Build(context.Background(), store, config)
	if err != nil {
		b.Fatal(err)
	}
	query, err := index.PrepareQuery(make([]float32, dimensions))
	if err != nil {
		b.Fatal(err)
	}
	request := QueryConfig{EfSearch: 200, VisitLimit: rows}
	b.ReportAllocs()
	for b.Loop() {
		result, err := index.SearchPrepared(context.Background(), query, 10, request)
		if err != nil {
			b.Fatal(err)
		}
		searchBenchmarkResult = result
	}
}
