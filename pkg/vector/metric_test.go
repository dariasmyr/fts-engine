package vector

import (
	"errors"
	"math"
	"math/rand"
	"testing"
)

func TestNewSpaceValidation(t *testing.T) {
	if _, err := NewSpace(0, MetricCosine); !errors.Is(err, ErrInvalidDimensions) {
		t.Fatalf("NewSpace(0, cosine) error = %v, want ErrInvalidDimensions", err)
	}
	if _, err := NewSpace(3, Metric(255)); !errors.Is(err, ErrUnsupportedMetric) {
		t.Fatalf("NewSpace(3, unknown) error = %v, want ErrUnsupportedMetric", err)
	}
}

func TestSpaceMetadata(t *testing.T) {
	cosine := mustSpace(t, 3, MetricCosine)
	if cosine.Dimensions() != 3 {
		t.Fatalf("Dimensions() = %d, want 3", cosine.Dimensions())
	}
	if cosine.Metric() != MetricCosine {
		t.Fatalf("Metric() = %v, want cosine", cosine.Metric())
	}
	if cosine.Normalization() != NormalizationUnitLength {
		t.Fatalf("Normalization() = %v, want unit length", cosine.Normalization())
	}

	l2 := mustSpace(t, 3, MetricL2Squared)
	if l2.Normalization() != NormalizationNone {
		t.Fatalf("L2 Normalization() = %v, want none", l2.Normalization())
	}
}

func TestSpacePrepareCopiesNormalizesAndCanonicalizesZero(t *testing.T) {
	space := mustSpace(t, 2, MetricCosine)
	input := []float32{float32(math.Copysign(0, -1)), 2}
	prepared, err := space.Prepare(input)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if math.Signbit(float64(prepared[0])) {
		t.Fatal("Prepare() preserved negative zero")
	}
	if !closeFloat(float64(prepared[1]), 1, 1e-7) {
		t.Fatalf("prepared[1] = %v, want 1", prepared[1])
	}

	input[1] = 7
	if !closeFloat(float64(prepared[1]), 1, 1e-7) {
		t.Fatalf("prepared changed after caller mutation: %v", prepared)
	}
}

func TestSpacePrepareCanonicalizesNormalizationCreatedNegativeZero(t *testing.T) {
	space := mustSpace(t, 2, MetricCosine)
	prepared, err := space.Prepare([]float32{math.MaxFloat32, -math.SmallestNonzeroFloat32})
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if prepared[1] != 0 || math.Signbit(float64(prepared[1])) {
		t.Fatalf("prepared[1] = %v signbit=%t, want positive zero", prepared[1], math.Signbit(float64(prepared[1])))
	}
}

func TestSpacePrepareValidation(t *testing.T) {
	space := mustSpace(t, 2, MetricCosine)
	tests := []struct {
		name  string
		value []float32
		want  error
	}{
		{name: "dimension mismatch", value: []float32{1}, want: ErrDimensionMismatch},
		{name: "nan", value: []float32{float32(math.NaN()), 1}, want: ErrNonFiniteVector},
		{name: "positive infinity", value: []float32{float32(math.Inf(1)), 1}, want: ErrNonFiniteVector},
		{name: "negative infinity", value: []float32{float32(math.Inf(-1)), 1}, want: ErrNonFiniteVector},
		{name: "zero norm", value: []float32{0, 0}, want: ErrZeroNorm},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := space.Prepare(test.value); !errors.Is(err, test.want) {
				t.Fatalf("Prepare(%v) error = %v, want %v", test.value, err, test.want)
			}
		})
	}
}

func TestCosineDistanceKnownVectors(t *testing.T) {
	space := mustSpace(t, 2, MetricCosine)
	tests := []struct {
		name string
		a    []float32
		b    []float32
		want float64
	}{
		{name: "identical", a: []float32{2, 0}, b: []float32{4, 0}, want: 0},
		{name: "orthogonal", a: []float32{1, 0}, b: []float32{0, 1}, want: 1},
		{name: "opposite", a: []float32{1, 0}, b: []float32{-1, 0}, want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := space.Distance(test.a, test.b)
			if err != nil {
				t.Fatalf("Distance() error = %v", err)
			}
			if !closeFloat(got, test.want, 1e-6) {
				t.Fatalf("Distance() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestL2SquaredKnownVectors(t *testing.T) {
	space := mustSpace(t, 3, MetricL2Squared)
	got, err := space.Distance([]float32{1, 2, 3}, []float32{4, 6, 3})
	if err != nil {
		t.Fatalf("Distance() error = %v", err)
	}
	if got != 25 {
		t.Fatalf("Distance() = %v, want 25", got)
	}
}

func TestDistanceProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, metric := range []Metric{MetricCosine, MetricL2Squared} {
		space := mustSpace(t, 16, metric)
		for range 200 {
			a := randomVector(rng, space.Dimensions())
			b := randomVector(rng, space.Dimensions())
			preparedA, err := space.Prepare(a)
			if err != nil {
				t.Fatalf("Prepare(a) error = %v", err)
			}
			preparedB, err := space.Prepare(b)
			if err != nil {
				t.Fatalf("Prepare(b) error = %v", err)
			}

			ab := space.DistancePrepared(preparedA, preparedB)
			ba := space.DistancePrepared(preparedB, preparedA)
			if math.IsNaN(ab) || math.IsInf(ab, 0) || ab < 0 {
				t.Fatalf("metric=%v distance = %v, want finite non-negative", metric, ab)
			}
			if !closeFloat(ab, ba, 1e-12) {
				t.Fatalf("metric=%v asymmetric distance: ab=%v ba=%v", metric, ab, ba)
			}
			self := space.DistancePrepared(preparedA, preparedA)
			if !closeFloat(self, 0, 1e-6) {
				t.Fatalf("metric=%v self distance = %v, want 0", metric, self)
			}
		}
	}
}

func mustSpace(t *testing.T, dimensions int, metric Metric) Space {
	t.Helper()
	space, err := NewSpace(dimensions, metric)
	if err != nil {
		t.Fatalf("NewSpace() error = %v", err)
	}
	return space
}

func randomVector(rng *rand.Rand, dimensions int) []float32 {
	value := make([]float32, dimensions)
	for i := range value {
		value[i] = rng.Float32()*2 - 1
	}
	return value
}

func closeFloat(got, want, tolerance float64) bool {
	return math.Abs(got-want) <= tolerance
}
