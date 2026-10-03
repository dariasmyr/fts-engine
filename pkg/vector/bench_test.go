package vector

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkDistancePrepared(b *testing.B) {
	for _, metric := range []Metric{MetricCosine, MetricL2Squared} {
		for _, dimensions := range []int{128, 384, 768, 1536} {
			b.Run(fmt.Sprintf("%s/dims=%d", metric, dimensions), func(b *testing.B) {
				space, err := NewCalculator(dimensions, metric)
				if err != nil {
					b.Fatalf("NewCalculator() error = %v", err)
				}
				rng := rand.New(rand.NewSource(42))
				a, err := space.Prepare(randomVector(rng, dimensions))
				if err != nil {
					b.Fatalf("Prepare(a) error = %v", err)
				}
				other, err := space.Prepare(randomVector(rng, dimensions))
				if err != nil {
					b.Fatalf("Prepare(b) error = %v", err)
				}

				var distance float64
				for b.Loop() {
					distance = space.DistancePrepared(a, other)
				}
				_ = distance
			})
		}
	}
}

func BenchmarkBitSetWithChanges(b *testing.B) {
	set := NewFullBitSet(1_000_000)
	disallowed := make([]Ordinal, 32)
	for i := range disallowed {
		disallowed[i] = Ordinal(i * 30_000)
	}
	b.ReportAllocs()
	var next BitSet
	b.ResetTimer()
	for b.Loop() {
		var err error
		next, err = set.WithChanges(set.TotalOrdinalCount(), nil, disallowed)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = next
}
