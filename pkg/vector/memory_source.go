package vector

import (
	"context"
	"fmt"
)

// MemorySource is immutable prepared vector storage backed by a contiguous
// in-memory matrix. It implements the vector source contract, not search.
type MemorySource struct {
	space  Space
	values []float32
}

// NewMemorySource prepares and stores a set of vector rows in memory.
func NewMemorySource(space Space, values [][]float32) (*MemorySource, error) {
	prepared := make([]float32, len(values)*space.Dimensions())
	for row, value := range values {
		start := row * space.Dimensions()
		if err := space.PrepareInto(prepared[start:start+space.Dimensions()], value); err != nil {
			return nil, err
		}
	}
	return NewPreparedMemorySource(space, prepared)
}

// NewPreparedMemorySource stores a copy of an already prepared contiguous
// vector matrix. The input slice is not retained.
func NewPreparedMemorySource(space Space, prepared []float32) (*MemorySource, error) {
	if space.Dimensions() <= 0 {
		return nil, ErrInvalidDimensions
	}
	if len(prepared)%space.Dimensions() != 0 {
		return nil, fmt.Errorf("%w: got %d values for dimension %d", ErrDimensionMismatch, len(prepared), space.Dimensions())
	}
	return &MemorySource{space: space, values: append([]float32(nil), prepared...)}, nil
}

func (s *MemorySource) Len() int { return len(s.values) / s.space.Dimensions() }

func (s *MemorySource) Dimensions() int { return s.space.Dimensions() }

func (s *MemorySource) Metric() Metric { return s.space.Metric() }

func (s *MemorySource) Normalization() Normalization { return s.space.Normalization() }

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
