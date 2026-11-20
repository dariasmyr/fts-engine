package vector

import (
	"fmt"
	"math"
	"testing"
)

func TestDotFloat64MatchesSequentialReference(t *testing.T) {
	tests := []struct {
		name string
		a    []float32
		b    []float32
	}{
		{name: "empty"},
		{name: "one remainder", a: []float32{2}, b: []float32{-3}},
		{name: "three remainders", a: []float32{1, -2, 3}, b: []float32{-4, 5, -6}},
		{
			name: "not divisible by eight",
			a:    []float32{1, -2, 3, -4, 5, -6, 7, -8, 9, -10, 11},
			b:    []float32{-11, 10, -9, 8, -7, 6, -5, 4, -3, 2, -1},
		},
	}
	for _, dimensions := range []int{7, 128, 384, 769, 1024} {
		a := make([]float32, dimensions)
		b := make([]float32, dimensions)
		for i := range dimensions {
			a[i] = float32(math.Sin(float64(i+1)) * float64((i%7)+1))
			b[i] = float32(math.Cos(float64(i+3)) * float64((i%11)+1))
		}
		tests = append(tests, struct {
			name string
			a    []float32
			b    []float32
		}{
			name: fmt.Sprintf("mixed signs dimensions=%d", dimensions),
			a:    a,
			b:    b,
		})
	}
	var maxAbsoluteError float64
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := dotFloat64SingleAccumulator(tt.a, tt.b)
			got := dotFloat64(tt.a, tt.b)
			var absoluteProductSum float64
			for i := range tt.a {
				absoluteProductSum += math.Abs(float64(tt.a[i]) * float64(tt.b[i]))
			}
			tolerance := 1e-12 * max(1, absoluteProductSum)
			absoluteError := math.Abs(got - want)
			maxAbsoluteError = max(maxAbsoluteError, absoluteError)
			if absoluteError > tolerance {
				t.Fatalf("dotFloat64() = %.17g, sequential = %.17g, tolerance = %g", got, want, tolerance)
			}
		})
	}
	t.Logf("maximum absolute error against sequential accumulation: %.3g", maxAbsoluteError)
}
