package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type ordinalRange struct {
	Start vector.Ordinal
	Count int
}

type mutableSource struct {
	space      vector.Space
	maxVectors int
	values     []float32
}

func newMutableSource(space vector.Space, maxVectors, initialCapacity int) *mutableSource {
	return &mutableSource{space: space, maxVectors: maxVectors, values: make([]float32, 0, initialCapacity*space.Dimensions())}
}

func (s *mutableSource) Len() int { return len(s.values) / s.space.Dimensions() }

func (s *mutableSource) Dimensions() int { return s.space.Dimensions() }

func (s *mutableSource) Metric() vector.Metric { return s.space.Metric() }

func (s *mutableSource) Normalization() vector.Normalization { return s.space.Normalization() }

func (s *mutableSource) AppendBatch(values [][]float32) (ordinalRange, error) {
	if len(values) > s.maxVectors-s.Len() {
		return ordinalRange{}, ErrCapacityExceeded
	}
	prepared := make([]float32, len(values)*s.Dimensions())
	for row, value := range values {
		start := row * s.Dimensions()
		if err := s.space.PrepareInto(prepared[start:start+s.Dimensions()], value); err != nil {
			return ordinalRange{}, err
		}
	}
	start := s.Len()
	s.values = append(s.values, prepared...)
	return ordinalRange{Start: vector.Ordinal(start), Count: len(values)}, nil
}

func (s *mutableSource) FreezeCompact(ctx context.Context, filter vector.ResultFilter) (*memorySource, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if filter != nil && filter.TotalOrdinalCount() != uint32(s.Len()) {
		return nil, vector.ErrResultFilterSizeMismatch
	}
	values := make([]float32, 0, s.Len()*s.Dimensions())
	for row := 0; row < s.Len(); row++ {
		if row%64 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if filter != nil && !filter.Allows(vector.Ordinal(row)) {
			continue
		}
		start := row * s.Dimensions()
		values = append(values, s.values[start:start+s.Dimensions()]...)
	}
	return &memorySource{space: s.space, values: values}, nil
}

func (s *mutableSource) Compact(ctx context.Context, filter vector.ResultFilter) (*mutableSource, error) {
	compacted, err := s.FreezeCompact(ctx, filter)
	if err != nil {
		return nil, err
	}
	return &mutableSource{
		space: compacted.space, maxVectors: s.maxVectors,
		values: append([]float32(nil), compacted.values...),
	}, nil
}

func (s *mutableSource) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
	return (&memorySource{space: s.space, values: s.values}).ReadVectorInto(ctx, ordinal, dst)
}

// memorySource is immutable prepared vector storage owned by one semantic
// segment. It deliberately implements only the vector source contract; it is
// not an exact search index.
type memorySource struct {
	space  vector.Space
	values []float32
}

func newMemorySource(space vector.Space, values [][]float32) (*memorySource, error) {
	prepared := make([]float32, len(values)*space.Dimensions())
	for row, value := range values {
		start := row * space.Dimensions()
		if err := space.PrepareInto(prepared[start:start+space.Dimensions()], value); err != nil {
			return nil, err
		}
	}
	return &memorySource{space: space, values: prepared}, nil
}

func (s *memorySource) Len() int { return len(s.values) / s.space.Dimensions() }

func (s *memorySource) Dimensions() int { return s.space.Dimensions() }

func (s *memorySource) Metric() vector.Metric { return s.space.Metric() }

func (s *memorySource) Normalization() vector.Normalization { return s.space.Normalization() }

func (s *memorySource) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(dst) != s.Dimensions() {
		return fmt.Errorf("%w: got %d, want %d", vector.ErrDimensionMismatch, len(dst), s.Dimensions())
	}
	if uint64(ordinal) >= uint64(s.Len()) {
		return fmt.Errorf("%w: %d", vector.ErrOrdinalOutOfRange, ordinal)
	}
	start := int(ordinal) * s.Dimensions()
	copy(dst, s.values[start:start+s.Dimensions()])
	return nil
}
