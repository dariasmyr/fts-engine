package memorystore

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// MemoryVectorStore is immutable prepared vector storage backed by a contiguous
// in-memory matrix. It implements the vector store contract, not search.
type MemoryVectorStore struct {
	space   vector.Space
	vectors []float32
}

// NewMemoryVectorStore prepares and stores a set of vector rows in memory.
func New(space vector.Space, vectors [][]float32) (*MemoryVectorStore, error) {
	flatVectors := make([]float32, len(vectors)*space.Dimensions())
	for row, vector := range vectors {
		start := row * space.Dimensions()
		if err := space.PrepareInto(flatVectors[start:start+space.Dimensions()], vector); err != nil {
			return nil, err
		}
	}
	return NewPrepared(space, flatVectors)
}

// NewPreparedMemoryVectorStore stores an already prepared contiguous
// vector matrix without copying it. The caller transfers exclusive ownership
// of flatVectors and must not access or mutate it after this call.
func NewPrepared(space vector.Space, flatVectors []float32) (*MemoryVectorStore, error) {
	if space.Dimensions() <= 0 {
		return nil, vector.ErrInvalidDimensions
	}
	if len(flatVectors)%space.Dimensions() != 0 {
		return nil, fmt.Errorf("%w: got %d values for dimension %d", vector.ErrDimensionMismatch, len(flatVectors), space.Dimensions())
	}
	return &MemoryVectorStore{space: space, vectors: flatVectors}, nil
}

func (s *MemoryVectorStore) Len() int { return len(s.vectors) / s.space.Dimensions() }

func (s *MemoryVectorStore) Dimensions() int { return s.space.Dimensions() }

func (s *MemoryVectorStore) Metric() vector.Metric { return s.space.Metric() }

func (s *MemoryVectorStore) Normalization() vector.Normalization { return s.space.Normalization() }

// ReadVectorInto copies one prepared row into dst without exposing storage.
func (s *MemoryVectorStore) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
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
	copy(dst, s.vectors[start:start+s.Dimensions()])
	return nil
}

var _ vector.PreparedVectorStore = (*MemoryVectorStore)(nil)
