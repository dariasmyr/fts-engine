package vector

import (
	"errors"
	"math"
	"testing"
)

func TestCalculatorDistances(t *testing.T) {
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
			calculator, err := NewCalculator(2, tt.metric)
			if err != nil {
				t.Fatal(err)
			}
			got, err := calculator.Distance(tt.a, tt.b)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(got-tt.want) > 1e-6 {
				t.Fatalf("Distance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculatorPrepareCopiesAndValidates(t *testing.T) {
	calculator, err := NewCalculator(2, MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	source := []float32{3, 4}
	prepared, err := calculator.Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 0
	if math.Abs(float64(prepared[0])-0.6) > 1e-6 || math.Abs(float64(prepared[1])-0.8) > 1e-6 {
		t.Fatalf("Prepare() = %v, want normalized copy", prepared)
	}
	if _, err := calculator.Prepare([]float32{0, 0}); !errors.Is(err, ErrZeroNorm) {
		t.Fatalf("Prepare(zero) error = %v, want ErrZeroNorm", err)
	}
	if _, err := calculator.Prepare([]float32{float32(math.NaN()), 1}); !errors.Is(err, ErrNonFiniteVector) {
		t.Fatalf("Prepare(NaN) error = %v, want ErrNonFiniteVector", err)
	}
}
