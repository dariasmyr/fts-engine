package vector

import (
	"errors"
	"math"
	"slices"
)

var ErrMatrixBuilderFinalized = errors.New("vector: matrix builder is finalized")

// MatrixBuilder incrementally constructs one immutable Matrix. It owns its
// backing storage and validates raw or canonical input at the boundary where it
// enters the builder. Build transfers that storage without rescanning it.
type MatrixBuilder struct {
	spec      Spec
	values    []float32
	finalized bool
}

// NewMatrixBuilder creates an empty builder with capacity for rowCapacity rows.
func NewMatrixBuilder(dimensions int, metric Metric, rowCapacity int) (*MatrixBuilder, error) {
	spec, err := NewSpec(dimensions, metric)
	if err != nil {
		return nil, err
	}
	if rowCapacity < 0 || rowCapacity > math.MaxInt/dimensions {
		return nil, ErrDimensionMismatch
	}
	return &MatrixBuilder{
		spec:   spec,
		values: make([]float32, 0, rowCapacity*dimensions),
	}, nil
}

// Add validates, prepares, and appends one raw vector.
func (b *MatrixBuilder) Add(value []float32) error {
	if err := b.ready(); err != nil {
		return err
	}
	row := b.appendRow()
	if err := b.spec.PrepareInto(row, value); err != nil {
		b.values = b.values[:len(b.values)-b.spec.Dimensions()]
		return err
	}
	return nil
}

// AddCanonical validates and appends one canonical stored vector. It is for
// untrusted imported representations that claim to have already been prepared.
func (b *MatrixBuilder) AddCanonical(value []float32) error {
	if err := b.ready(); err != nil {
		return err
	}
	row := b.appendRow()
	if err := b.spec.copyCanonical(row, value); err != nil {
		b.values = b.values[:len(b.values)-b.spec.Dimensions()]
		return err
	}
	return nil
}

// AddPrepared appends a vector previously produced for the same vector spec.
// Compatibility is checked from opaque provenance without rescanning components.
func (b *MatrixBuilder) AddPrepared(value Prepared) error {
	if err := b.ready(); err != nil {
		return err
	}
	if err := b.spec.ValidatePrepared(value); err != nil {
		return err
	}
	copy(b.appendRow(), value.value)
	return nil
}

// AddMatrixRow copies one trusted row from an immutable compatible Matrix
// without rescanning its components.
func (b *MatrixBuilder) AddMatrixRow(matrix *Matrix, ordinal Ordinal) error {
	if err := b.ready(); err != nil {
		return err
	}
	if matrix == nil || matrix.spec != b.spec {
		return ErrDimensionMismatch
	}
	row, err := matrix.row(ordinal)
	if err != nil {
		return err
	}
	copy(b.appendRow(), row)
	return nil
}

// Len returns the number of appended rows.
func (b *MatrixBuilder) Len() int {
	if b == nil || b.spec.Dimensions() <= 0 {
		return 0
	}
	return len(b.values) / b.spec.Dimensions()
}

// Build finalizes the builder and transfers its backing storage to a Matrix.
// Calls after a successful Build return ErrMatrixBuilderFinalized.
func (b *MatrixBuilder) Build() (*Matrix, error) {
	if err := b.ready(); err != nil {
		return nil, err
	}
	values := b.values[:len(b.values):len(b.values)]
	matrix := newMatrix(b.spec, values)
	b.values = nil
	b.finalized = true
	return matrix, nil
}

func (b *MatrixBuilder) ready() error {
	if b == nil || b.spec.Dimensions() <= 0 {
		return ErrInvalidDimensions
	}
	if b.finalized {
		return ErrMatrixBuilderFinalized
	}
	return nil
}

func (b *MatrixBuilder) appendRow() []float32 {
	dimensions := b.spec.Dimensions()
	start := len(b.values)
	b.values = slices.Grow(b.values, dimensions)
	b.values = b.values[:start+dimensions]
	clear(b.values[start:])
	return b.values[start:]
}
