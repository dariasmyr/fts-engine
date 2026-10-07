package exact_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/vectorsearch/exact"
	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestNewCopiesCompleteVectorSet(t *testing.T) {
	values := [][]float32{{1, 2}, {3, 4}}
	oracle, err := exact.New(values, 2, vector.MetricL2Squared, 2)
	if err != nil {
		t.Fatal(err)
	}
	values[0][0] = 100
	stored := make([]float32, 2)
	if err := oracle.Store().ReadVectorInto(context.Background(), 0, stored); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(stored, []float32{1, 2}) {
		t.Fatalf("stored vector = %v, want [1 2]", stored)
	}
}

func TestNewFromPreparedStore(t *testing.T) {
	calculator, err := vector.NewCalculator(2, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.New(calculator, [][]float32{{1, 0}, {0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	oracle, err := exact.NewFromPreparedStore(store, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := oracle.Search(context.Background(), []float32{1, 0}, 2, exact.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.Hits[0].Ordinal != 0 || result.Hits[1].Ordinal != 1 {
		t.Fatalf("hits = %+v", result.Hits)
	}
}

func TestSearchFiltersOrdersAndCounts(t *testing.T) {
	oracle, err := exact.New([][]float32{{2, 0}, {1, 0}, {-1, 0}, {4, 0}}, 2, vector.MetricL2Squared, 4)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := vector.NewBitSet(4, 0, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := oracle.Search(context.Background(), []float32{0, 0}, 3, exact.Options{ResultFilter: filter})
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

func TestSearchValidationCancellationAndVisitLimit(t *testing.T) {
	oracle, err := exact.New([][]float32{{0, 0}, {1, 0}, {2, 0}}, 2, vector.MetricL2Squared, 3)
	if err != nil {
		t.Fatal(err)
	}
	result, err := oracle.Search(context.Background(), []float32{0, 0}, 2, exact.Options{VisitLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Incomplete || result.Stats.Termination != exact.TerminationVisitLimit || result.Stats.VisitedNodes != 1 {
		t.Fatalf("limited result = %+v", result)
	}
	if _, err := oracle.Search(context.Background(), []float32{0, 0}, 1, exact.Options{ResultFilter: vector.NewFullBitSet(2)}); !errors.Is(err, vector.ErrResultFilterSizeMismatch) {
		t.Fatalf("filter size error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := oracle.Search(ctx, []float32{0, 0}, 1, exact.Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
}

type invalidCountFilter struct{ count int }

func (f invalidCountFilter) Allows(vector.Ordinal) bool { return true }
func (f invalidCountFilter) AllowedOrdinalCount() int   { return f.count }
func (f invalidCountFilter) TotalOrdinalCount() uint32  { return 3 }

func TestSearchRejectsInvalidFilterCardinality(t *testing.T) {
	oracle, err := exact.New([][]float32{{0, 0}, {1, 0}, {2, 0}}, 2, vector.MetricL2Squared, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{-1, 4} {
		if _, err := oracle.Search(context.Background(), []float32{0, 0}, 1, exact.Options{ResultFilter: invalidCountFilter{count: count}}); !errors.Is(err, exact.ErrInvalidOptions) {
			t.Fatalf("allowed count %d error = %v", count, err)
		}
	}
}

func TestSearchMatchesFullSort(t *testing.T) {
	const rowCount = 300
	vectors := make([][]float32, rowCount)
	for i := range vectors {
		vectors[i] = []float32{rand.Float32()*20 - 10, rand.Float32()*20 - 10}
	}
	oracle, err := exact.New(vectors, 2, vector.MetricL2Squared, 20)
	if err != nil {
		t.Fatal(err)
	}
	query := []float32{1.5, -2.5}
	result, err := oracle.Search(context.Background(), query, 17, exact.Options{})
	if err != nil {
		t.Fatal(err)
	}
	all := make([]vector.Hit, len(vectors))
	for i, value := range vectors {
		dx := float64(query[0] - value[0])
		dy := float64(query[1] - value[1])
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
