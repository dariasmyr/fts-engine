package vector

import (
	"context"
	"fmt"
)

// Matrix is an immutable contiguous in-memory matrix of prepared vectors.
// Rows are addressed by Ordinal. Matrix owns its storage and is safe for
// concurrent reads.
type Matrix struct {
	spec   Spec
	values []float32
}

// NewMatrix validates, prepares, and copies vectors into contiguous memory.
func NewMatrix(dimensions int, metric Metric, vectors [][]float32) (*Matrix, error) {
	builder, err := NewMatrixBuilder(dimensions, metric, len(vectors))
	if err != nil {
		return nil, err
	}
	for row, value := range vectors {
		if err := builder.Add(value); err != nil {
			return nil, fmt.Errorf("vector: prepare row %d: %w", row, err)
		}
	}
	return builder.Build()
}

func newMatrix(spec Spec, values []float32) *Matrix {
	return &Matrix{spec: spec, values: values}
}

// Len returns the number of rows.
func (m *Matrix) Len() int {
	if m == nil || m.spec.Dimensions() <= 0 {
		return 0
	}
	return len(m.values) / m.spec.Dimensions()
}

// Dimensions returns the number of components in each row.
func (m *Matrix) Dimensions() int {
	if m == nil {
		return 0
	}
	return m.spec.Dimensions()
}

// Metric returns the distance metric used by the matrix.
func (m *Matrix) Metric() Metric {
	if m == nil {
		return 0
	}
	return m.spec.Metric()
}

// Normalization returns the canonical representation used by stored rows.
func (m *Matrix) Normalization() Normalization {
	if m == nil {
		return 0
	}
	return m.spec.Normalization()
}

// Spec returns the immutable definition shared by all matrix rows.
func (m *Matrix) Spec() Spec {
	if m == nil {
		return Spec{}
	}
	return m.spec
}

// Prepare validates and prepares a query for repeated comparisons with rows.
func (m *Matrix) Prepare(value []float32) (Prepared, error) {
	if m == nil {
		return Prepared{}, ErrInvalidDimensions
	}
	return m.spec.Prepare(value)
}

// ReadInto copies one row into dst without exposing matrix storage.
func (m *Matrix) ReadInto(ctx context.Context, ordinal Ordinal, dst []float32) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || m.Dimensions() <= 0 {
		return ErrInvalidDimensions
	}
	if len(dst) != m.Dimensions() {
		return fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(dst), m.Dimensions())
	}
	row, err := m.row(ordinal)
	if err != nil {
		return err
	}
	copy(dst, row)
	return nil
}

// DistancePrepared returns the distance from a prepared query to one row.
func (m *Matrix) DistancePrepared(query Prepared, ordinal Ordinal) (float64, error) {
	if m == nil || !m.spec.compatiblePrepared(query) {
		return 0, ErrDimensionMismatch
	}
	row, err := m.row(ordinal)
	if err != nil {
		return 0, err
	}
	return m.spec.DistancePrepared(query, row), nil
}

// DistancePreparedValues returns the distance from a prepared component slice
// to one row. The query must have been prepared for this matrix.
func (m *Matrix) DistancePreparedValues(query []float32, ordinal Ordinal) (float64, error) {
	if m == nil || len(query) != m.Dimensions() {
		return 0, ErrDimensionMismatch
	}
	row, err := m.row(ordinal)
	if err != nil {
		return 0, err
	}
	return m.spec.DistancePreparedValues(query, row), nil
}

// DistanceRows returns the distance between two rows.
func (m *Matrix) DistanceRows(a, b Ordinal) (float64, error) {
	if m == nil {
		return 0, ErrInvalidDimensions
	}
	left, err := m.row(a)
	if err != nil {
		return 0, err
	}
	right, err := m.row(b)
	if err != nil {
		return 0, err
	}
	return m.spec.DistancePreparedValues(left, right), nil
}

func (m *Matrix) row(ordinal Ordinal) ([]float32, error) {
	if uint64(ordinal) >= uint64(m.Len()) {
		return nil, fmt.Errorf("%w: %d", ErrOrdinalOutOfRange, ordinal)
	}
	start := int(ordinal) * m.Dimensions()
	return m.values[start : start+m.Dimensions()], nil
}
