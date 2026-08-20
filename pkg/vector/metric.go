package vector

import (
	"errors"
	"fmt"
	"math"
)

var (
	ErrInvalidDimensions = errors.New("vector: dimensions must be positive")
	ErrUnsupportedMetric = errors.New("vector: unsupported metric")
	ErrDimensionMismatch = errors.New("vector: dimension mismatch")
	ErrNonFiniteVector   = errors.New("vector: vector contains a non-finite value")
	ErrZeroNorm          = errors.New("vector: zero-norm vector is invalid for cosine distance")
)

// Space validates and prepares vectors that share dimensions and a metric.
// The zero value is invalid; construct a Space with NewSpace.
type Space struct {
	dimensions    int
	metric        Metric
	normalization Normalization
}

func NewSpace(dimensions int, metric Metric) (Space, error) {
	if dimensions <= 0 {
		return Space{}, ErrInvalidDimensions
	}
	if !metric.Valid() {
		return Space{}, fmt.Errorf("%w: %d", ErrUnsupportedMetric, metric)
	}

	normalization := NormalizationNone
	if metric == MetricCosine {
		normalization = NormalizationUnitLength
	}
	return Space{dimensions: dimensions, metric: metric, normalization: normalization}, nil
}

func (s Space) Dimensions() int { return s.dimensions }

func (s Space) Metric() Metric { return s.metric }

func (s Space) Normalization() Normalization { return s.normalization }

// Prepare validates value, copies it, canonicalizes signed zero, and applies
// the space's storage normalization.
func (s Space) Prepare(value []float32) ([]float32, error) {
	if err := s.validate(value); err != nil {
		return nil, err
	}

	prepared := make([]float32, len(value))
	copy(prepared, value)

	if s.normalization == NormalizationUnitLength {
		normSquared := dotFloat64(prepared, prepared)
		if normSquared == 0 {
			return nil, ErrZeroNorm
		}
		if math.IsNaN(normSquared) || math.IsInf(normSquared, 0) {
			return nil, ErrNonFiniteVector
		}
		inverseNorm := 1 / math.Sqrt(normSquared)
		for i := range prepared {
			prepared[i] = float32(float64(prepared[i]) * inverseNorm)
		}
	}
	for i := range prepared {
		if prepared[i] == 0 {
			prepared[i] = 0 // canonicalize input and normalization-created negative zero
		}
	}

	return prepared, nil
}

// Distance validates and prepares both values before calculating their
// distance. Index implementations should prepare stored vectors and queries
// once, then use DistancePrepared in their hot loops.
func (s Space) Distance(a, b []float32) (float64, error) {
	preparedA, err := s.Prepare(a)
	if err != nil {
		return 0, err
	}
	preparedB, err := s.Prepare(b)
	if err != nil {
		return 0, err
	}
	return s.DistancePrepared(preparedA, preparedB), nil
}

// DistancePrepared calculates distance without validation or allocation. Both
// vectors must have Space.Dimensions elements and must have been returned by
// Prepare for this space.
func (s Space) DistancePrepared(a, b []float32) float64 {
	switch s.metric {
	case MetricCosine:
		similarity := dotFloat64(a, b)
		// float32 normalization can leave a small rounding error outside [-1, 1].
		similarity = max(-1, min(1, similarity))
		return 1 - similarity
	case MetricL2Squared:
		var distance float64
		for i := range a {
			delta := float64(a[i]) - float64(b[i])
			distance += delta * delta
		}
		return distance
	default:
		panic("vector: DistancePrepared called with an invalid Space")
	}
}

func (s Space) validate(value []float32) error {
	if s.dimensions <= 0 || !s.metric.Valid() {
		return errors.New("vector: invalid space")
	}
	if len(value) != s.dimensions {
		return fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(value), s.dimensions)
	}
	for i, component := range value {
		v := float64(component)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("%w at dimension %d", ErrNonFiniteVector, i)
		}
	}
	return nil
}

func dotFloat64(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
