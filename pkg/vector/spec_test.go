package vector_test

import (
	"errors"
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestSpecDistances(t *testing.T) {
	tests := []struct {
		name   string
		metric vector.Metric
		a, b   []float32
		want   float64
	}{
		{name: "squared L2", metric: vector.MetricL2Squared, a: []float32{1, 2}, b: []float32{4, 6}, want: 25},
		{name: "cosine", metric: vector.MetricCosine, a: []float32{1, 0}, b: []float32{0, 1}, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec, err := vector.NewSpec(2, tt.metric)
			if err != nil {
				t.Fatal(err)
			}
			got, err := spec.Distance(tt.a, tt.b)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(got-tt.want) > 1e-6 {
				t.Fatalf("Distance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSpecPrepareCopiesAndValidates(t *testing.T) {
	spec, err := vector.NewSpec(2, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	source := []float32{3, 4}
	prepared, err := spec.Prepare(source)
	if err != nil {
		t.Fatal(err)
	}
	source[0] = 0
	distance := spec.DistancePrepared(prepared, []float32{0.6, 0.8})
	if math.Abs(distance) > 1e-6 {
		t.Fatalf("prepared distance = %v, want normalized defensive copy", distance)
	}
	if err := spec.ValidatePrepared(prepared); err != nil {
		t.Fatalf("ValidatePrepared() error = %v", err)
	}
	otherSpec, err := vector.NewSpec(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	if err := otherSpec.ValidatePrepared(prepared); !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Fatalf("ValidatePrepared() error = %v, want ErrDimensionMismatch", err)
	}
	if _, err := spec.Prepare([]float32{0, 0}); !errors.Is(err, vector.ErrZeroNorm) {
		t.Fatalf("Prepare(zero) error = %v, want ErrZeroNorm", err)
	}
	if _, err := spec.Prepare([]float32{float32(math.NaN()), 1}); !errors.Is(err, vector.ErrNonFinite) {
		t.Fatalf("Prepare(NaN) error = %v, want ErrNonFinite", err)
	}
}
