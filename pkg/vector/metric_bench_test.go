package vector

import (
	"fmt"
	"math"
	"testing"
)

var (
	dotFloat64BenchmarkResult      float64
	dotFloat64BatchBenchmarkResult [4]float64
)

func BenchmarkDotFloat64(b *testing.B) {
	for _, dimensions := range []int{128, 384, 768, 1024} {
		a, c := newDotFloat64BenchmarkVectors(dimensions)
		b.Run(fmt.Sprintf("dimensions=%d", dimensions), func(b *testing.B) {
			b.ReportAllocs()
			var result float64
			for b.Loop() {
				result = dotFloat64(a, c)
			}
			dotFloat64BenchmarkResult = result
		})
	}
}

func BenchmarkDotFloat64Variants(b *testing.B) {
	variants := []struct {
		name string
		dot  func([]float32, []float32) float64
	}{
		{name: "accumulators=1", dot: dotFloat64SingleAccumulator},
		{name: "accumulators=2", dot: dotFloat64TwoAccumulators},
		{name: "accumulators=4", dot: dotFloat64FourAccumulators},
		{name: "accumulators=8", dot: dotFloat64EightAccumulators},
	}

	for _, dimensions := range []int{128, 384, 768, 1024} {
		a, c := newDotFloat64BenchmarkVectors(dimensions)
		for _, variant := range variants {
			b.Run(fmt.Sprintf("dimensions=%d/%s", dimensions, variant.name), func(b *testing.B) {
				b.ReportAllocs()
				var result float64
				for b.Loop() {
					result = variant.dot(a, c)
				}
				dotFloat64BenchmarkResult = result
			})
		}
	}
}

func BenchmarkDotFloat64BatchVariants(b *testing.B) {
	for _, dimensions := range []int{128, 384, 768, 1024} {
		target, v0 := newDotFloat64BenchmarkVectors(dimensions)
		_, v1 := newDotFloat64BenchmarkVectors(dimensions)
		_, v2 := newDotFloat64BenchmarkVectors(dimensions)
		_, v3 := newDotFloat64BenchmarkVectors(dimensions)
		for i := range dimensions {
			v1[i] += float32(i%3) * 0.001
			v2[i] -= float32(i%5) * 0.001
			v3[i] += float32(i%7) * 0.001
		}

		b.Run(fmt.Sprintf("dimensions=%d/scalar", dimensions), func(b *testing.B) {
			b.ReportAllocs()
			var result [4]float64
			for b.Loop() {
				result[0] = dotFloat64(target, v0)
				result[1] = dotFloat64(target, v1)
				result[2] = dotFloat64(target, v2)
				result[3] = dotFloat64(target, v3)
			}
			dotFloat64BatchBenchmarkResult = result
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*4), "ns/distance")
		})
		b.Run(fmt.Sprintf("dimensions=%d/batch2", dimensions), func(b *testing.B) {
			b.ReportAllocs()
			var result [4]float64
			for b.Loop() {
				result[0], result[1] = distanceBatch2(target, v0, v1)
				result[2], result[3] = distanceBatch2(target, v2, v3)
			}
			dotFloat64BatchBenchmarkResult = result
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*4), "ns/distance")
		})
		b.Run(fmt.Sprintf("dimensions=%d/batch4", dimensions), func(b *testing.B) {
			b.ReportAllocs()
			var result [4]float64
			for b.Loop() {
				result = distanceBatch4(target, v0, v1, v2, v3)
			}
			dotFloat64BatchBenchmarkResult = result
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*4), "ns/distance")
		})
	}
}

func newDotFloat64BenchmarkVectors(dimensions int) ([]float32, []float32) {
	a := make([]float32, dimensions)
	b := make([]float32, dimensions)
	for i := range dimensions {
		a[i] = float32(math.Sin(float64(i + 1)))
		b[i] = float32(math.Cos(float64(i + 1)))
	}
	return a, b
}

func dotFloat64SingleAccumulator(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

func dotFloat64TwoAccumulators(a, b []float32) float64 {
	var sum0, sum1 float64
	i := 0
	for ; i+1 < len(a); i += 2 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
	}
	sum := sum0 + sum1
	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

func dotFloat64FourAccumulators(a, b []float32) float64 {
	var sum0, sum1, sum2, sum3 float64
	i := 0
	for ; i+3 < len(a); i += 4 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
		sum2 += float64(a[i+2]) * float64(b[i+2])
		sum3 += float64(a[i+3]) * float64(b[i+3])
	}
	sum := (sum0 + sum1) + (sum2 + sum3)
	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

func dotFloat64EightAccumulators(a, b []float32) float64 {
	var sum0, sum1, sum2, sum3 float64
	var sum4, sum5, sum6, sum7 float64
	i := 0
	for ; i+7 < len(a); i += 8 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
		sum2 += float64(a[i+2]) * float64(b[i+2])
		sum3 += float64(a[i+3]) * float64(b[i+3])
		sum4 += float64(a[i+4]) * float64(b[i+4])
		sum5 += float64(a[i+5]) * float64(b[i+5])
		sum6 += float64(a[i+6]) * float64(b[i+6])
		sum7 += float64(a[i+7]) * float64(b[i+7])
	}
	sum01 := sum0 + sum1
	sum23 := sum2 + sum3
	sum45 := sum4 + sum5
	sum67 := sum6 + sum7
	sum := (sum01 + sum23) + (sum45 + sum67)
	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
