package flat

import (
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestExactTopKBoundsAndOrdersUniqueHits(t *testing.T) {
	top := newExactTopK(3)
	for _, hit := range []vector.Hit{
		{Ordinal: 5, Distance: 2},
		{Ordinal: 3, Distance: 1},
		{Ordinal: 2, Distance: 1},
		{Ordinal: 8, Distance: 4},
		{Ordinal: 1, Distance: 0.5},
	} {
		top.Add(hit)
	}
	want := []vector.Hit{
		{Ordinal: 1, Distance: 0.5},
		{Ordinal: 2, Distance: 1},
		{Ordinal: 3, Distance: 1},
	}
	if got := top.Results(); !slices.Equal(got, want) {
		t.Fatalf("Results() = %+v, want %+v", got, want)
	}
}
