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
				space, err := NewSpace(dimensions, metric)
				if err != nil {
					b.Fatalf("NewSpace() error = %v", err)
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

func BenchmarkTopK(b *testing.B) {
	for _, candidates := range []int{1_000, 10_000, 100_000} {
		for _, k := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("candidates=%d/k=%d", candidates, k), func(b *testing.B) {
				distances := make([]float64, candidates)
				rng := rand.New(rand.NewSource(42))
				for i := range distances {
					distances[i] = rng.Float64()
				}

				for b.Loop() {
					top := NewTopK(k)
					for i, distance := range distances {
						top.Add(Hit{Ordinal: Ordinal(i), Distance: distance})
					}
					_ = top.Results()
				}
			})
		}
	}
}
