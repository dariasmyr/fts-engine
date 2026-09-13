package hnsw_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func TestExactFallbackBoundariesAndExactEquality(t *testing.T) {
	values := [][]float32{{-3, 1}, {-1, 0}, {0, 2}, {1, 0}, {3, -1}, {5, 2}}
	flatReader := testFlatReader(t, values, vector.MetricL2Squared)
	reader := testBuild(t, flatReader, 2, len(values), vector.MetricL2Squared)
	query := []float32{0.5, 0.25}

	ann, err := reader.Search(context.Background(), query, 3, vector.SearchOptions{EfSearch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if ann.Stats.UsedExactFallback {
		t.Fatal("Reader.Search unexpectedly selected exact fallback")
	}

	disabled, err := reader.WithExactFallback(hnsw.ExactFallbackPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := disabled.Search(context.Background(), query, 3, vector.SearchOptions{EfSearch: 1})
	if err != nil || result.Stats.UsedExactFallback {
		t.Fatalf("zero policy result = %+v, %v", result, err)
	}

	below, _ := reader.WithExactFallback(hnsw.ExactFallbackPolicy{MaxPhysicalRows: len(values) - 1})
	result, err = below.Search(context.Background(), query, 3, vector.SearchOptions{})
	if err != nil || result.Stats.UsedExactFallback {
		t.Fatalf("below-boundary result = %+v, %v", result, err)
	}

	atBoundary, _ := reader.WithExactFallback(hnsw.ExactFallbackPolicy{MaxPhysicalRows: len(values)})
	result, err = atBoundary.Search(context.Background(), query, 3, vector.SearchOptions{VisitLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.UsedExactFallback || !result.Incomplete || result.Stats.DistanceComputations != 1 {
		t.Fatalf("visit-limited ANN result = %+v", result)
	}
	result, err = atBoundary.Search(context.Background(), query, 3, vector.SearchOptions{VisitLimit: len(values)})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Stats.UsedExactFallback || result.Incomplete || result.Stats.DistanceComputations != len(values) {
		t.Fatalf("row-boundary exact fallback = %+v", result)
	}
	want, err := flatReader.Search(context.Background(), query, 3, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(result.Hits, want.Hits) {
		t.Fatalf("exact fallback hits = %+v, flat = %+v", result.Hits, want.Hits)
	}

	filter, err := vector.NewBitSet(uint32(len(values)), 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	distanceBoundary, _ := reader.WithExactFallback(hnsw.ExactFallbackPolicy{MaxDistanceComputations: 2})
	filtered, err := distanceBoundary.Search(context.Background(), query, 3, vector.SearchOptions{ResultFilter: filter})
	if err != nil {
		t.Fatal(err)
	}
	filteredWant, _ := flatReader.Search(context.Background(), query, 3, vector.SearchOptions{ResultFilter: filter})
	if !filtered.Stats.UsedExactFallback || filtered.Stats.DistanceComputations != 2 || !slices.Equal(filtered.Hits, filteredWant.Hits) {
		t.Fatalf("distance-boundary fallback = %+v, flat = %+v", filtered, filteredWant)
	}
	distanceBelow, _ := reader.WithExactFallback(hnsw.ExactFallbackPolicy{MaxDistanceComputations: 1})
	if got, err := distanceBelow.Search(context.Background(), query, 3, vector.SearchOptions{ResultFilter: filter}); err != nil || got.Stats.UsedExactFallback {
		t.Fatalf("distance-below result = %+v, %v", got, err)
	}
	both, _ := reader.WithExactFallback(hnsw.ExactFallbackPolicy{MaxPhysicalRows: len(values) - 1, MaxDistanceComputations: 2})
	if got, err := both.Search(context.Background(), query, 3, vector.SearchOptions{ResultFilter: filter}); err != nil || got.Stats.UsedExactFallback {
		t.Fatalf("conjunctive policy result = %+v, %v", got, err)
	}
}

func TestExactFallbackValidation(t *testing.T) {
	flatReader := testFlatReader(t, [][]float32{{0, 0}, {1, 0}, {2, 0}}, vector.MetricL2Squared)
	reader := testBuild(t, flatReader, 2, 3, vector.MetricL2Squared)
	if _, err := reader.WithExactFallback(hnsw.ExactFallbackPolicy{MaxPhysicalRows: -1}); !errors.Is(err, hnsw.ErrInvalidExactFallbackPolicy) {
		t.Fatalf("policy error = %v", err)
	}
	searcher, _ := reader.WithExactFallback(hnsw.ExactFallbackPolicy{MaxPhysicalRows: 3})
	if _, err := searcher.Search(context.Background(), []float32{0, 0}, 1, vector.SearchOptions{EfSearch: reader.SearchConfig().MaxEfSearch + 1}); !errors.Is(err, vector.ErrInvalidSearchOptions) {
		t.Fatalf("option error = %v", err)
	}
	wrongFilter := vector.NewFullBitSet(2)
	if _, err := searcher.Search(context.Background(), []float32{0, 0}, 1, vector.SearchOptions{ResultFilter: wrongFilter}); !errors.Is(err, vector.ErrResultFilterSizeMismatch) {
		t.Fatalf("filter error = %v", err)
	}
}
