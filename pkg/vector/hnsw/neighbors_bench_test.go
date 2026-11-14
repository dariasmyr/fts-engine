package hnsw

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkSelectNeighbors(b *testing.B) {
	builder := newConnectivityTestBuilder(
		b,
		[]float32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
		make([][]nodeOrdinal, 18),
		8,
	)
	candidates := make([]candidate, 0, 17)
	for node := nodeOrdinal(1); node <= 17; node++ {
		candidates = append(candidates, candidate{
			node:     node,
			distance: builder.distanceNodes(0, node),
		})
	}

	for _, fillFromRejected := range []bool{true, false} {
		b.Run(fmt.Sprintf("fallback=%t", fillFromRejected), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := builder.selectNeighbors(
					context.Background(),
					candidates,
					16,
					fillFromRejected,
				); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
