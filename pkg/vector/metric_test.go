package vector

import (
	"errors"
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
