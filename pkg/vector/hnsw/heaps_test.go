package hnsw

import (
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestResultHeapRetainsLowestOrdinalsForEqualDistances(t *testing.T) {
	t.Parallel()

	heap := newResultHeapWithBuffer(3, nil)
	for _, node := range []nodeOrdinal{3, 1, 2, 0} {
		heap.Add(candidate{node: node, distance: 1, accepted: true})
	}
	hits := heap.Hits(3)
	for ordinal, hit := range hits {
		if hit != (vector.Hit{Ordinal: vector.Ordinal(ordinal), Distance: 1}) {
			t.Fatalf("hit %d = %+v, want ordinal %d at distance 1", ordinal, hit, ordinal)
		}
	}
}
