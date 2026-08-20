package vector

import (
	"math"
	"math/rand"
	"slices"
	"testing"
)

func TestTopKKeepsBestHitsInDeterministicOrder(t *testing.T) {
	top := NewTopK(3)
	for _, hit := range []Hit{
		{Ordinal: 1, Distance: 0.3},
		{Ordinal: 5, Distance: 0.2},
		{Ordinal: 7, Distance: 0.1},
		{Ordinal: 2, Distance: 0.2},
		{Ordinal: 9, Distance: 0.9},
	} {
		top.Add(hit)
	}

	want := []Hit{
		{Ordinal: 7, Distance: 0.1},
		{Ordinal: 2, Distance: 0.2},
		{Ordinal: 5, Distance: 0.2},
	}
	if got := top.Results(); !slices.Equal(got, want) {
		t.Fatalf("Results() = %+v, want %+v", got, want)
	}
}

func TestTopKIsIndependentOfInsertionOrder(t *testing.T) {
	candidates := []Hit{
		{Ordinal: 8, Distance: 0.5},
		{Ordinal: 2, Distance: 0.1},
		{Ordinal: 7, Distance: 0.3},
		{Ordinal: 1, Distance: 0.1},
		{Ordinal: 5, Distance: 0.3},
		{Ordinal: 3, Distance: 0.2},
	}

	baseline := NewTopK(4)
	for _, hit := range candidates {
		baseline.Add(hit)
	}
	want := baseline.Results()

	rng := rand.New(rand.NewSource(42))
	for iteration := range 100 {
		shuffled := append([]Hit(nil), candidates...)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		top := NewTopK(4)
		for _, hit := range shuffled {
			top.Add(hit)
		}
		if got := top.Results(); !slices.Equal(got, want) {
			t.Fatalf("iteration %d Results() = %+v, want %+v", iteration, got, want)
		}
	}
}

func TestTopKRejectsInvalidDistancesAndZeroLimit(t *testing.T) {
	top := NewTopK(2)
	if top.Add(Hit{Ordinal: 1, Distance: math.NaN()}) {
		t.Fatal("Add(NaN) = true, want false")
	}
	if top.Add(Hit{Ordinal: 2, Distance: math.Inf(1)}) {
		t.Fatal("Add(+Inf) = true, want false")
	}
	if top.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", top.Len())
	}

	empty := NewTopK(-1)
	if empty.Add(Hit{Ordinal: 1, Distance: 0}) {
		t.Fatal("negative-limit TopK retained a hit")
	}
	if got := empty.Results(); len(got) != 0 {
		t.Fatalf("Results() = %+v, want empty", got)
	}
}

func TestTopKResultsReturnsCopy(t *testing.T) {
	top := NewTopK(1)
	top.Add(Hit{Ordinal: 3, Distance: 0.25})
	first := top.Results()
	first[0] = Hit{}
	second := top.Results()
	if second[0].Ordinal != 3 || second[0].Distance != 0.25 {
		t.Fatalf("Results() shared mutable storage: %+v", second)
	}
}

func TestTopKUpdatesExistingOrdinalWithoutDuplicates(t *testing.T) {
	top := NewTopK(2)
	top.Add(Hit{Ordinal: 1, Distance: 0.2})
	top.Add(Hit{Ordinal: 1, Distance: 0.1})
	top.Add(Hit{Ordinal: 2, Distance: 0.3})

	want := []Hit{
		{Ordinal: 1, Distance: 0.1},
		{Ordinal: 2, Distance: 0.3},
	}
	if got := top.Results(); !slices.Equal(got, want) {
		t.Fatalf("Results() = %+v, want %+v", got, want)
	}
	if top.Add(Hit{Ordinal: 1, Distance: 0.4}) {
		t.Fatal("Add(worse duplicate) = true, want false")
	}
}

func TestTopKMatchesFullSort(t *testing.T) {
	rng := rand.New(rand.NewSource(84))
	for iteration := range 100 {
		candidateCount := 1 + rng.Intn(200)
		limit := rng.Intn(candidateCount + 1)
		candidates := make([]Hit, candidateCount)
		for i := range candidates {
			candidates[i] = Hit{Ordinal: Ordinal(i), Distance: rng.Float64()}
		}

		top := NewTopK(limit)
		for _, hit := range candidates {
			top.Add(hit)
		}
		slices.SortFunc(candidates, compareHits)
		want := candidates[:limit]
		if got := top.Results(); !slices.Equal(got, want) {
			t.Fatalf("iteration %d Results() = %+v, want %+v", iteration, got, want)
		}
	}
}
