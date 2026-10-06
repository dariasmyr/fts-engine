package semantic

import (
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

var benchmarkRankedHits rankedHitHeap

func BenchmarkRankedHitHeap(b *testing.B) {
	const (
		segments = 64
		k        = 100
	)
	candidates := make([]rankedHit, segments*k)
	for i := range candidates {
		candidates[i] = rankedHit{
			hit:       ChunkHit{Distance: float64((i * 7919) % len(candidates))},
			component: uint64(i/k + 1),
			ordinal:   vector.Ordinal(i % k),
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		top := make(rankedHitHeap, 0, k)
		for _, candidate := range candidates {
			top.add(candidate, k)
		}
		benchmarkRankedHits = top
	}
}
