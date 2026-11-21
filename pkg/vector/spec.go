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
	ErrNonFinite         = errors.New("vector: vector contains a non-finite value")
	ErrZeroNorm          = errors.New("vector: zero-norm vector is invalid for cosine distance")
)

// Spec contains vector validation and distance operations for one dimension
// count and metric combination. It contains no vectors, bytes, or persistent state.
// Applications building an index should normally construct a Matrix instead.
// The zero value is invalid; construct a Spec with NewSpec.
type Spec struct {
	dimensions    int
	metric        Metric
	normalization Normalization
}

// NewSpec creates vector validation, normalization, and distance operations
// for one dimension-count/metric combination.
func NewSpec(dimensions int, metric Metric) (Spec, error) {
	if dimensions <= 0 {
		return Spec{}, ErrInvalidDimensions
	}
	if !metric.Valid() {
		return Spec{}, fmt.Errorf("%w: %d", ErrUnsupportedMetric, metric)
	}

	normalization := NormalizationNone
	if metric == MetricCosine {
		normalization = NormalizationUnitLength
	}
	return Spec{dimensions: dimensions, metric: metric, normalization: normalization}, nil
}

func (s Spec) Dimensions() int { return s.dimensions }

func (s Spec) Metric() Metric { return s.metric }

func (s Spec) Normalization() Normalization { return s.normalization }

// Prepare validates value, creates an owned defensive copy,
// canonicalizes signed zero, and applies the spec's storage normalization.
// Later caller mutations therefore cannot change the prepared vector.
func (s Spec) Prepare(value []float32) (Prepared, error) {
	normSquared, err := s.validate(value)
	if err != nil {
		return Prepared{}, err
	}

	prepared := make([]float32, len(value))
	s.prepareIntoValidated(prepared, value, normSquared)
	return Prepared{
		value:         prepared,
		metric:        s.metric,
		normalization: s.normalization,
	}, nil
}

// ValidatePrepared checks that prepared belongs to this spec without
// rescanning its components.
func (s Spec) ValidatePrepared(prepared Prepared) error {
	if !s.compatiblePrepared(prepared) {
		return ErrDimensionMismatch
	}
	return nil
}

// DistancePrepared calculates distance from a validated Prepared
// to one compatible prepared row.
func (s Spec) DistancePrepared(prepared Prepared, stored []float32) float64 {
	return s.DistancePreparedValues(prepared.value, stored)
}

// PrepareInto validates value and writes its copied, canonicalized, normalized
// representation into dst without allocating. It returns a Prepared view over
// dst, which must not be mutated while that Prepared value is in use. dst must
// have Spec.Dimensions elements and may alias value.
func (s Spec) PrepareInto(dst, value []float32) (Prepared, error) {
	if len(dst) != s.dimensions {
		return Prepared{}, fmt.Errorf("%w: destination has %d, want %d", ErrDimensionMismatch, len(dst), s.dimensions)
	}
	normSquared, err := s.validate(value)
	if err != nil {
		return Prepared{}, err
	}
	s.prepareIntoValidated(dst, value, normSquared)
	return Prepared{
		value:         dst,
		metric:        s.metric,
		normalization: s.normalization,
	}, nil
}

// Distance validates and prepares both values before calculating their
// distance. Index implementations should prepare stored vectors and queries
// once, then use DistancePrepared in their hot loops.
func (s Spec) Distance(a, b []float32) (float64, error) {
	preparedA, err := s.Prepare(a)
	if err != nil {
		return 0, err
	}
	preparedB, err := s.Prepare(b)
	if err != nil {
		return 0, err
	}
	return s.DistancePreparedValues(preparedA.value, preparedB.value), nil
}

func (s Spec) compatiblePrepared(prepared Prepared) bool {
	return s.dimensions > 0 && s.metric.Valid() &&
		len(prepared.value) == s.dimensions &&
		prepared.metric == s.metric && prepared.normalization == s.normalization
}

// DistancePrepared calculates distance without validation or allocation. Both
// vectors must have Spec.Dimensions elements and must have been produced by
// Prepare or PrepareInto for this spec.
func (s Spec) DistancePreparedValues(a, b []float32) float64 {
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
		panic("vector: DistancePreparedValues called with an invalid Spec")
	}
}

// Validate checks whether value can be prepared for this spec without
// allocating or retaining it.
func (s Spec) Validate(value []float32) error {
	_, err := s.validate(value)
	return err
}

func (s Spec) validate(value []float32) (float64, error) {
	if s.dimensions <= 0 || !s.metric.Valid() {
		return 0, errors.New("vector: invalid spec")
	}
	if len(value) != s.dimensions {
		return 0, fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(value), s.dimensions)
	}
	var normSquared float64
	for i, component := range value {
		v := float64(component)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("%w at dimension %d", ErrNonFinite, i)
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
			return 0, ErrNonFinite
		}
	}
	return normSquared, nil
}

// copyCanonical validates and copies a canonical stored vector in one component
// pass. Callers must provide a destination with the spec dimensions.
func (s Spec) copyCanonical(dst, value []float32) error {
	if s.dimensions <= 0 || !s.metric.Valid() {
		return errors.New("vector: invalid spec")
	}
	if len(dst) != s.dimensions || len(value) != s.dimensions {
		return fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(value), s.dimensions)
	}
	// normSquared is only used for unit-length normalization, but we still compute it to check for non-finite values.
	var normSquared float64
	for i, component := range value {
		v := float64(component)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("%w at dimension %d", ErrNonFinite, i)
		}
		if math.Float32bits(component) == 1<<31 {
			return errors.New("vector: prepared vector contains negative zero")
		}
		dst[i] = component
		if s.normalization == NormalizationUnitLength {
			normSquared += v * v
		}
	}
	if s.normalization == NormalizationUnitLength && math.Abs(normSquared-1) > 1e-4 {
		return errors.New("vector: prepared vector is not unit length")
	}
	return nil
}

func (s Spec) prepareIntoValidated(dst, value []float32, normSquared float64) {
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
