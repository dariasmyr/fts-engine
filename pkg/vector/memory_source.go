package vector

import (
	"context"
	"fmt"
)

// MemorySource is immutable prepared vector storage backed by a contiguous
// in-memory matrix. It implements the vector source contract, not search.
type MemorySource struct {
	calculator Calculator
	values     []float32
}

// NewMemorySource prepares and stores a set of vector rows in memory.
func NewMemorySource(calculator Calculator, values [][]float32) (*MemorySource, error) {
	prepared := make([]float32, len(values)*calculator.Dimensions())
	for row, value := range values {
		start := row * calculator.Dimensions()
		if err := calculator.PrepareInto(prepared[start:start+calculator.Dimensions()], value); err != nil {
			return nil, err
		}
	}
	return NewPreparedMemorySource(calculator, prepared)
}

// NewPreparedMemorySource stores a copy of an already prepared contiguous
// vector matrix. The input slice is not retained.
func NewPreparedMemorySource(calculator Calculator, prepared []float32) (*MemorySource, error) {
	if calculator.Dimensions() <= 0 {
		return nil, ErrInvalidDimensions
	}
	if len(prepared)%calculator.Dimensions() != 0 {
		return nil, fmt.Errorf("%w: got %d values for dimension %d", ErrDimensionMismatch, len(prepared), calculator.Dimensions())
	}
	return &MemorySource{calculator: calculator, values: append([]float32(nil), prepared...)}, nil
}

func (s *MemorySource) Len() int { return len(s.values) / s.calculator.Dimensions() }

func (s *MemorySource) Dimensions() int { return s.calculator.Dimensions() }

func (s *MemorySource) Metric() Metric { return s.calculator.Metric() }

func (s *MemorySource) Normalization() Normalization { return s.calculator.Normalization() }

// ReadVectorInto copies one prepared row into dst without exposing storage.
func (s *MemorySource) ReadVectorInto(ctx context.Context, ordinal Ordinal, dst []float32) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(dst) != s.Dimensions() {
		return fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(dst), s.Dimensions())
	}
	if uint64(ordinal) >= uint64(s.Len()) {
		return fmt.Errorf("%w: %d", ErrOrdinalOutOfRange, ordinal)
	}
	start := int(ordinal) * s.Dimensions()
	copy(dst, s.values[start:start+s.Dimensions()])
	return nil
}

var _ PreparedVectorSource = (*MemorySource)(nil)
