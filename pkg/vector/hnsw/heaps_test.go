package hnsw

import (
	"math"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestCandidateHeapOrdersByDistanceThenNode(t *testing.T) {
	input := []searchCandidate{
		{node: 8, distance: 2},
		{node: 7, distance: 1},
		{node: 3, distance: 1},
		{node: 1, distance: 4},
		{node: 5, distance: 2},
	}
	want := []searchCandidate{input[2], input[1], input[4], input[0], input[3]}

	var heap candidateHeap
	for _, candidate := range input {
		heap.Push(candidate)
	}
	if heap.Len() != len(input) {
		t.Fatalf("Len() = %d, want %d", heap.Len(), len(input))
	}
	for i, expected := range want {
		got, ok := heap.Pop()
		if !ok || got != expected {
			t.Fatalf("Pop() %d = (%+v, %t), want (%+v, true)", i, got, ok, expected)
		}
	}
	if _, ok := heap.Pop(); ok || heap.Len() != 0 {
		t.Fatal("empty heap Pop() succeeded")
	}
}

func TestResultHeapBoundsAndOrdersEqualDistances(t *testing.T) {
	heap := newResultHeap(3)
	for _, candidate := range []searchCandidate{
		{node: 7, vectorOrdinal: 1, distance: 1},
		{node: 4, vectorOrdinal: 4, distance: 2},
		{node: 2, vectorOrdinal: 1, distance: 1},
		{node: 9, vectorOrdinal: 0, distance: 1},
		{node: 1, vectorOrdinal: 1, distance: 1},
		{node: 1, vectorOrdinal: 8, distance: 4},
	} {
		heap.Add(candidate)
	}

	if heap.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", heap.Len())
	}
	worst, ok := heap.Worst()
	if !ok || worst.node != 2 || worst.vectorOrdinal != 1 || worst.distance != 1 {
		t.Fatalf("Worst() = (%+v, %t), want node 2, ordinal 1, distance 1", worst, ok)
	}
	want := []vector.Hit{
		{Ordinal: 0, Distance: 1},
		{Ordinal: 1, Distance: 1},
		{Ordinal: 1, Distance: 1},
	}
	if got := heap.Results(10); !slices.Equal(got, want) {
		t.Fatalf("Results() = %+v, want %+v", got, want)
	}
	if got := heap.Results(2); !slices.Equal(got, want[:2]) {
		t.Fatalf("Results(2) = %+v, want %+v", got, want[:2])
	}
}

func TestResultHeapIgnoresZeroCapacityAndInvalidDistances(t *testing.T) {
	empty := newResultHeap(0)
	empty.Add(searchCandidate{distance: 1})
	if empty.Len() != 0 {
		t.Fatalf("zero-capacity Len() = %d, want 0", empty.Len())
	}

	heap := newResultHeap(2)
	heap.Add(searchCandidate{node: 1, distance: 0.5})
	heap.Add(searchCandidate{node: 2, distance: math.NaN()})
	heap.Add(searchCandidate{node: 3, distance: math.Inf(1)})
	if heap.Len() != 1 {
		t.Fatalf("Len() after non-finite values = %d, want 1", heap.Len())
	}
}

func FuzzCandidateHeapOrdering(f *testing.F) {
	f.Add([]byte{3, 1, 3, 0, 2})
	f.Add([]byte{255, 0, 255, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			data = data[:64]
		}
		want := make([]searchCandidate, len(data))
		var heap candidateHeap
		for i, value := range data {
			candidate := searchCandidate{
				node:     NodeOrdinal((uint16(value) + uint16(i)*17) % 31),
				distance: float64(value % 11),
			}
			want[i] = candidate
			heap.Push(candidate)
		}
		slices.SortFunc(want, func(a, b searchCandidate) int {
			if navigationBetter(a, b) {
				return -1
			}
			if navigationBetter(b, a) {
				return 1
			}
			return 0
		})
		for i, expected := range want {
			got, ok := heap.Pop()
			if !ok || got != expected {
				t.Fatalf("Pop() %d = (%+v, %t), want (%+v, true)", i, got, ok, expected)
			}
		}
	})
}
