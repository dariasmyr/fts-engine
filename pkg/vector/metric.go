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

// Calculator validates and prepares vectors that share dimensions and a metric.
// The zero value is invalid; construct a Calculator with NewCalculator.
type Calculator struct {
	dimensions    int
	metric        Metric
	normalization Normalization
}

func NewCalculator(dimensions int, metric Metric) (Calculator, error) {
	if dimensions <= 0 {
		return Calculator{}, ErrInvalidDimensions
	}
	if !metric.Valid() {
		return Calculator{}, fmt.Errorf("%w: %d", ErrUnsupportedMetric, metric)
	}

	normalization := NormalizationNone
	if metric == MetricCosine {
		normalization = NormalizationUnitLength
	}
	return Calculator{dimensions: dimensions, metric: metric, normalization: normalization}, nil
}

func (s Calculator) Dimensions() int { return s.dimensions }

func (s Calculator) Metric() Metric { return s.metric }

func (s Calculator) Normalization() Normalization { return s.normalization }

// Prepare validates value, copies it, canonicalizes signed zero, and applies
// the calculator's storage normalization.
func (s Calculator) Prepare(value []float32) ([]float32, error) {
	normSquared, err := s.validate(value)
	if err != nil {
		return nil, err
	}

	prepared := make([]float32, len(value))
	s.prepareIntoValidated(prepared, value, normSquared)
	return prepared, nil
}

// PrepareInto validates value and writes its copied, canonicalized, normalized
// representation into dst without allocating. Batch writers can therefore
// prepare directly into rows of one preallocated contiguous matrix. dst must
// have Calculator.Dimensions elements and may alias value.
func (s Calculator) PrepareInto(dst, value []float32) error {
	if len(dst) != s.dimensions {
		return fmt.Errorf("%w: destination has %d, want %d", ErrDimensionMismatch, len(dst), s.dimensions)
	}
	normSquared, err := s.validate(value)
	if err != nil {
		return err
	}
	s.prepareIntoValidated(dst, value, normSquared)
	return nil
}

// Distance validates and prepares both values before calculating their
// distance. Index implementations should prepare stored vectors and queries
// once, then use DistancePrepared in their hot loops.
func (s Calculator) Distance(a, b []float32) (float64, error) {
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
// vectors must have Calculator.Dimensions elements and must have been produced by
// Prepare or PrepareInto for this calculator.
func (s Calculator) DistancePrepared(a, b []float32) float64 {
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
		panic("vector: DistancePrepared called with an invalid Calculator")
	}
}

// Validate checks whether value can be prepared for this calculator without
// allocating or retaining it.
func (s Calculator) Validate(value []float32) error {
	_, err := s.validate(value)
	return err
}

func (s Calculator) validate(value []float32) (float64, error) {
	if s.dimensions <= 0 || !s.metric.Valid() {
		return 0, errors.New("vector: invalid calculator")
	}
	if len(value) != s.dimensions {
		return 0, fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(value), s.dimensions)
	}
	for i, component := range value {
		v := float64(component)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("%w at dimension %d", ErrNonFiniteVector, i)
		}
	}
	var normSquared float64
	if s.normalization == NormalizationUnitLength {
		normSquared = dotFloat64(value, value)
		if normSquared == 0 {
			return 0, ErrZeroNorm
		}
		if math.IsNaN(normSquared) || math.IsInf(normSquared, 0) {
			return 0, ErrNonFiniteVector
		}
	}
	return normSquared, nil
}

func (s Calculator) prepareIntoValidated(dst, value []float32, normSquared float64) {
	copy(dst, value)
	if s.normalization == NormalizationUnitLength {
		inverseNorm := 1 / math.Sqrt(normSquared)
		for i := range dst {
			dst[i] = float32(float64(dst[i]) * inverseNorm)
		}
	}
	for i := range dst {
		if dst[i] == 0 {
			dst[i] = 0 // canonicalize input and normalization-created negative zero
		}
	}
}

func dotFloat64(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
