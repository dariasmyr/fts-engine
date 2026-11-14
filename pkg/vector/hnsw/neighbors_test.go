package hnsw

import (
	"slices"
	"testing"
)

func TestNeighborSelectionUsesFallbackOnlyForDirectLinks(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(
		t,
		[]float32{0, 1, 2, 3, 4, 5},
		[][]nodeOrdinal{nil, nil, nil, nil, nil, nil},
		2,
	)
	candidates := make([]candidate, 0, 5)
	for node := nodeOrdinal(1); node <= 5; node++ {
		candidates = append(candidates, candidate{
			node:     node,
			distance: b.distanceNodes(0, node),
		})
	}

	direct, err := b.selectClosestNeighbors(t.Context(), candidates, 4)
	if err != nil {
		t.Fatal(err)
	}
	if want := []nodeOrdinal{1, 2, 3, 4}; !slices.Equal(direct, want) {
		t.Fatalf("direct links = %v, want fallback-filled %v", direct, want)
	}

	b.graph.nodes[0].links[0] = []nodeOrdinal{1, 2, 3, 4}
	if err := b.addReverseLink(t.Context(), 0, 5, 0); err != nil {
		t.Fatal(err)
	}
	if want := []nodeOrdinal{1}; !slices.Equal(b.graph.nodes[0].links[0], want) {
		t.Fatalf("reverse links = %v, want diversified-only %v", b.graph.nodes[0].links[0], want)
	}
}
