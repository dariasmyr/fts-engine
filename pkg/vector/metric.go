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

// PreparedVector is an immutable vector owned by a Space. Its components are
// validated, canonicalized, and normalized for that space. The zero value is
// invalid.
type PreparedVector struct {
	value         []float32
	metric        Metric
	normalization Normalization
}

// NewSpace creates vector validation, normalization, and distance operations
// for one dimensions/metric combination.
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

// Prepare validates value, creates an owned defensive copy,
// canonicalizes signed zero, and applies the space's storage normalization.
// Later caller mutations therefore cannot change the prepared vector.
func (s Space) Prepare(value []float32) (PreparedVector, error) {
	normSquared, err := s.validate(value)
	if err != nil {
		return PreparedVector{}, err
	}

	prepared := make([]float32, len(value))
	s.prepareIntoValidated(prepared, value, normSquared)
	return PreparedVector{
		value:         prepared,
		metric:        s.metric,
		normalization: s.normalization,
	}, nil
}

// CopyTo copies the prepared components into dst without exposing the owned
// backing array.
func (v PreparedVector) CopyTo(dst []float32) error {
	if len(v.value) == 0 || !v.metric.Valid() || len(dst) != len(v.value) {
		return fmt.Errorf("%w: destination has %d, want %d", ErrDimensionMismatch, len(dst), len(v.value))
	}
	copy(dst, v.value)
	return nil
}

// ValidatePreparedVector checks that prepared belongs to this space without
// rescanning its components.
func (s Space) ValidatePreparedVector(prepared PreparedVector) error {
	if !s.compatiblePrepared(prepared) {
		return ErrDimensionMismatch
	}
	return nil
}

// DistancePreparedVector calculates distance from a validated PreparedVector
// to one row read from a compatible PreparedVectorStore.
func (s Space) DistancePreparedVector(prepared PreparedVector, stored []float32) float64 {
	return s.DistancePrepared(prepared.value, stored)
}

// PrepareInto validates value and writes its copied, canonicalized, normalized
// representation into dst without allocating. Batch writers can therefore
// prepare directly into rows of one preallocated contiguous matrix. dst must
// have Space.Dimensions elements and may alias value.
func (s Space) PrepareInto(dst, value []float32) error {
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
func (s Space) Distance(a, b []float32) (float64, error) {
	preparedA, err := s.Prepare(a)
	if err != nil {
		return 0, err
	}
	preparedB, err := s.Prepare(b)
	if err != nil {
		return 0, err
	}
	return s.DistancePrepared(preparedA.value, preparedB.value), nil
}

func (s Space) compatiblePrepared(prepared PreparedVector) bool {
	return s.dimensions > 0 && s.metric.Valid() &&
		len(prepared.value) == s.dimensions &&
		prepared.metric == s.metric && prepared.normalization == s.normalization
}

// DistancePrepared calculates distance without validation or allocation. Both
// vectors must have Space.Dimensions elements and must have been produced by
// Prepare or PrepareInto for this space.
func (s Space) DistancePrepared(a, b []float32) float64 {
	switch s.metric {
	case MetricCosine:
		// Example:
		// a = [0.6, 0.8]
		// b = [0.8, 0.6]
		// dot(a, b) = 0.6*0.8 + 0.8*0.6 = 0.96
		// distance = 1 - dot(a, b) = 1 - 0.96 = 0.04
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

// Validate checks whether value can be prepared for this space without
// allocating or retaining it.
func (s Space) Validate(value []float32) error {
	_, err := s.validate(value)
	return err
}

func (s Space) validate(value []float32) (float64, error) {
	if s.dimensions <= 0 || !s.metric.Valid() {
		return 0, errors.New("vector: invalid space")
	}
	if len(value) != s.dimensions {
		return 0, fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(value), s.dimensions)
	}
	var normSquared float64
	for i, component := range value {
		v := float64(component)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("%w at dimension %d", ErrNonFiniteVector, i)
		}
		if s.normalization == NormalizationUnitLength {
			normSquared += v * v
		}
	}
	if s.normalization == NormalizationUnitLength {
		if normSquared == 0 {
			return 0, ErrZeroNorm
		}
		if math.IsNaN(normSquared) || math.IsInf(normSquared, 0) {
			return 0, ErrNonFiniteVector
		}
	}
	return normSquared, nil
}

// ValidatePrepared validates a canonical stored vector in one component pass.
func (s Space) ValidatePrepared(value []float32) error {
	if s.dimensions <= 0 || !s.metric.Valid() {
		return errors.New("vector: invalid space")
	}
	if len(value) != s.dimensions {
		return fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(value), s.dimensions)
	}
	// normSquared is only used for unit-length normalization, but we still compute it to check for non-finite values.
	var normSquared float64
	for i, component := range value {
		v := float64(component)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("%w at dimension %d", ErrNonFiniteVector, i)
		}
		if math.Float32bits(component) == 1<<31 {
			return errors.New("vector: prepared vector contains negative zero")
		}
		if s.normalization == NormalizationUnitLength {
			normSquared += v * v
		}
	}
	if s.normalization == NormalizationUnitLength && math.Abs(normSquared-1) > 1e-4 {
		return errors.New("vector: prepared vector is not unit length")
	}
	return nil
}

func (s Space) prepareIntoValidated(dst, value []float32, normSquared float64) {
	copy(dst, value)
	if s.normalization == NormalizationUnitLength {
		// Example:
		// values [3,4]
		// normSquared = 3*3 + 4*4 = 25
		// inverseNorm = 1 / sqrt(25) = 1/5 = 0.2
		// dst[0] = 3 * 0.2 = 0.6
		// dst[1] = 4 * 0.2 = 0.8
		inverseNorm := 1 / math.Sqrt(normSquared)
		for i := range dst {
			dst[i] = float32(float64(dst[i]) * inverseNorm)
		}
	}
	for i := range dst {
		if dst[i] == 0 {
			// Canonicalize negative zero to positive zero.
			dst[i] = 0
		}
	}
}

func dotFloat64(a, b []float32) float64 {
	var sum float64
	// We don't check dimensions here because the caller is expected to have validated them.
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}
