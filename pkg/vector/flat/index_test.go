package flat

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func newTestIndex(t *testing.T, maxVectors, maxK int) *Index {
	t.Helper()
	idx, err := New(Config{Dimensions: 2, Metric: vector.MetricL2Squared, MaxVectors: maxVectors, MaxK: maxK})
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func TestNewReservesOnlyConfiguredInitialCapacity(t *testing.T) {
	idx, err := New(Config{
		Dimensions:            3,
		Metric:                vector.MetricL2Squared,
		MaxVectors:            100,
		MaxK:                  10,
		InitialVectorCapacity: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(idx.values) != 0 || cap(idx.values) != 21 {
		t.Fatalf("values len/cap = %d/%d, want 0/21", len(idx.values), cap(idx.values))
	}
	if _, err := New(Config{Dimensions: 3, Metric: vector.MetricL2Squared, MaxVectors: 5, MaxK: 1, InitialVectorCapacity: 6}); !errors.Is(err, ErrInvalidCapacity) {
		t.Fatalf("invalid initial capacity error = %v", err)
	}
}

func TestAppendBatchIsAtomicAndCopiesInput(t *testing.T) {
	idx := newTestIndex(t, 3, 3)
	input := []float32{1, 2}
	ordinals, err := idx.AppendBatch([][]float32{input, {3, 4}})
	if err != nil {
		t.Fatal(err)
	}
	if ordinals.Start != 0 || ordinals.Count != 2 {
		t.Fatalf("ordinals = %+v, want start 0 count 2", ordinals)
	}
	if second, ok := ordinals.At(1); !ok || second != 1 {
		t.Fatalf("ordinals.At(1) = %d, %t, want 1, true", second, ok)
	}
	input[0] = 100
	if idx.values[0] != 1 {
		t.Fatalf("stored input changed to %v", idx.values[0])
	}

	before := append([]float32(nil), idx.values...)
	if _, err := idx.AppendBatch([][]float32{{5, 6}, {7}}); !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Fatalf("invalid batch error = %v", err)
	}
	if !slices.Equal(idx.values, before) {
		t.Fatal("invalid batch changed the index")
	}
	if _, err := idx.AppendBatch([][]float32{{5, 6}, {7, 8}}); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("capacity error = %v", err)
	}
	if !slices.Equal(idx.values, before) {
		t.Fatal("capacity failure changed the index")
	}
}

func TestSearchFiltersOrdersAndCounts(t *testing.T) {
	idx := newTestIndex(t, 10, 10)
	_, err := idx.AppendBatch([][]float32{{2, 0}, {1, 0}, {-1, 0}, {4, 0}})
	if err != nil {
		t.Fatal(err)
	}
	eligible, err := vector.NewBitSet(4, 0, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := idx.Search(context.Background(), []float32{0, 0}, 3, vector.SearchOptions{ResultFilter: eligible})
	if err != nil {
		t.Fatal(err)
	}
	want := []vector.Hit{{Ordinal: 1, Distance: 1}, {Ordinal: 2, Distance: 1}, {Ordinal: 0, Distance: 4}}
	if !slices.Equal(result.Hits, want) {
		t.Fatalf("hits = %+v, want %+v", result.Hits, want)
	}
	if result.Stats.VisitedNodes != 4 || result.Stats.RejectedNodes != 1 || result.Stats.DistanceComputations != 3 {
		t.Fatalf("stats = %+v", result.Stats)
	}
}

func TestSearchVisitLimitCancellationAndAcceptSize(t *testing.T) {
	idx := newTestIndex(t, 10, 10)
	_, _ = idx.AppendBatch([][]float32{{0, 0}, {1, 0}, {2, 0}})
	result, err := idx.Search(context.Background(), []float32{0, 0}, 2, vector.SearchOptions{VisitLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Incomplete || result.Stats.Termination != vector.TerminationVisitLimit || result.Stats.VisitedNodes != 1 {
		t.Fatalf("limited result = %+v", result)
	}
	wrong := vector.NewFullBitSet(2)
	if _, err := idx.Search(context.Background(), []float32{0, 0}, 1, vector.SearchOptions{ResultFilter: wrong}); !errors.Is(err, vector.ErrResultFilterSizeMismatch) {
		t.Fatalf("eligible ordinal error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := idx.Search(ctx, []float32{0, 0}, 1, vector.SearchOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
}

func TestSearchMatchesFullSort(t *testing.T) {
	idx := newTestIndex(t, 500, 20)
	vectors := make([][]float32, 300)
	for i := range vectors {
		vectors[i] = []float32{rand.Float32()*20 - 10, rand.Float32()*20 - 10}
	}
	if _, err := idx.AppendBatch(vectors); err != nil {
		t.Fatal(err)
	}
	query := []float32{1.5, -2.5}
	result, err := idx.Search(context.Background(), query, 17, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	all := make([]vector.Hit, len(vectors))
	for i, vectorValue := range vectors {
		dx := float64(query[0] - vectorValue[0])
		dy := float64(query[1] - vectorValue[1])
		all[i] = vector.Hit{Ordinal: vector.Ordinal(i), Distance: dx*dx + dy*dy}
	}
	slices.SortFunc(all, func(a, b vector.Hit) int {
		if a.Distance < b.Distance {
			return -1
		}
		if a.Distance > b.Distance {
			return 1
		}
		return int(a.Ordinal) - int(b.Ordinal)
	})
	if !slices.Equal(result.Hits, all[:17]) {
		t.Fatalf("exact hits differ\ngot  %+v\nwant %+v", result.Hits, all[:17])
	}
}
