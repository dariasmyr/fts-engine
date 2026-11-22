//go:build arm64 && go1.27 && goexperiment.simd

package dot

import (
	"fmt"
	"math"
	"simd"
	"testing"
)

func BenchmarkDotFloat64SIMDVariants(b *testing.B) {
	variants := []struct {
		name string
		dot  func([]float32, []float32) float64
	}{
		{name: "scalar-8x-float64", dot: dotFloat64EightAccumulators},
		{name: "portable-float32", dot: dotFloat32PortableSIMD},
		{name: "arch-widen-float64", dot: Float64},
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

func TestDotFloat32PortableSIMDNumericalError(t *testing.T) {
	for _, dimensions := range []int{128, 384, 768, 1024} {
		a, b := newDotFloat64BenchmarkVectors(dimensions)
		want := dotFloat64SingleAccumulator(a, b)
		got := dotFloat32PortableSIMD(a, b)
		t.Logf(
			"dimensions=%d absolute-error=%.9g relative-error=%.9g",
			dimensions,
			math.Abs(got-want),
			math.Abs((got-want)/want),
		)
	}
}

func dotFloat32PortableSIMD(a, b []float32) float64 {
	var accumulator simd.Float32s
	lanes := accumulator.Len()
	i := 0
	for ; i+lanes <= len(a); i += lanes {
		av := simd.LoadFloat32s(a[i : i+lanes])
		bv := simd.LoadFloat32s(b[i : i+lanes])
		accumulator = av.MulAdd(bv, accumulator)
	}
	if i < len(a) {
		av, _ := simd.LoadFloat32sPart(a[i:])
		bv, _ := simd.LoadFloat32sPart(b[i:])
		accumulator = av.MulAdd(bv, accumulator)
	}
	var laneSums [16]float32 // Go 1.27 supports SIMD vectors up to 512 bits.
	accumulator.Store(laneSums[:lanes])
	var sum float64
	for _, laneSum := range laneSums[:lanes] {
		sum += float64(laneSum)
	}
	return sum
}
