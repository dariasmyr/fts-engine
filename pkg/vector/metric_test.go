package vector

import (
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestSpaceDistances(t *testing.T) {
	tests := []struct {
		name   string
		metric Metric
		a, b   []float32
		want   float64
	}{
		{name: "squared L2", metric: MetricL2Squared, a: []float32{1, 2}, b: []float32{4, 6}, want: 25},
		{name: "cosine", metric: MetricCosine, a: []float32{1, 0}, b: []float32{0, 1}, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			space, err := NewSpace(2, tt.metric)
			if err != nil {
				t.Fatal(err)
			}
			got, err := space.Distance(tt.a, tt.b)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(got-tt.want) > 1e-6 {
				t.Fatalf("Distance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSpacePrepareCopiesAndValidates(t *testing.T) {
	space, err := NewSpace(2, MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	source := []float32{3, 4}
	prepared, err := space.Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 0
	values := make([]float32, 2)
	if err := prepared.CopyTo(values); err != nil {
		t.Fatal(err)
	}
	if math.Abs(float64(values[0])-0.6) > 1e-6 || math.Abs(float64(values[1])-0.8) > 1e-6 {
		t.Fatalf("Prepare() = %v, want normalized copy", values)
	}
	values[0] = 99
	if err := prepared.CopyTo(values); err != nil {
		t.Fatal(err)
	}
	if math.Abs(float64(values[0])-0.6) > 1e-6 {
		t.Fatalf("CopyTo() exposed mutable storage: %v", values)
	}
	if err := space.ValidatePreparedVector(prepared); err != nil {
		t.Fatalf("ValidatePreparedVector() error = %v", err)
	}
	otherSpace, err := NewSpace(2, MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	if err := otherSpace.ValidatePreparedVector(prepared); !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("ValidatePreparedVector() error = %v, want ErrDimensionMismatch", err)
	}
	if _, err := space.Prepare([]float32{0, 0}); !errors.Is(err, ErrZeroNorm) {
		t.Fatalf("Prepare(zero) error = %v, want ErrZeroNorm", err)
	}
	if _, err := space.Prepare([]float32{float32(math.NaN()), 1}); !errors.Is(err, ErrNonFiniteVector) {
		t.Fatalf("Prepare(NaN) error = %v, want ErrNonFiniteVector", err)
	}
}

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
