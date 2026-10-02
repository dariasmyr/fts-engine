package vectorstore

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// MemoryVectorStore is immutable prepared vector storage backed by a contiguous
// in-memory matrix. It implements the vector store contract, not search.
type MemoryVectorStore struct {
	calculator vector.Calculator
	vectors    []float32
}

// NewMemoryVectorStore prepares and stores a set of vector rows in memory.
func NewMemoryVectorStore(calculator vector.Calculator, vectors [][]float32) (*MemoryVectorStore, error) {
	flatVectors := make([]float32, len(vectors)*calculator.Dimensions())
	for row, vector := range vectors {
		start := row * calculator.Dimensions()
		if err := calculator.PrepareInto(flatVectors[start:start+calculator.Dimensions()], vector); err != nil {
			return nil, err
		}
	}
	return NewPreparedMemoryVectorStore(calculator, flatVectors)
}

// NewPreparedMemoryVectorStore stores a copy of an already prepared contiguous
// vector matrix. The input slice is not retained.
func NewPreparedMemoryVectorStore(calculator vector.Calculator, flatVectors []float32) (*MemoryVectorStore, error) {
	if calculator.Dimensions() <= 0 {
		return nil, vector.ErrInvalidDimensions
	}
	if len(flatVectors)%calculator.Dimensions() != 0 {
		return nil, fmt.Errorf("%w: got %d values for dimension %d", vector.ErrDimensionMismatch, len(flatVectors), calculator.Dimensions())
	}
	return &MemoryVectorStore{calculator: calculator, vectors: append([]float32(nil), flatVectors...)}, nil
}

func (s *MemoryVectorStore) Len() int { return len(s.vectors) / s.calculator.Dimensions() }

func (s *MemoryVectorStore) Dimensions() int { return s.calculator.Dimensions() }

func (s *MemoryVectorStore) Metric() vector.Metric { return s.calculator.Metric() }

func (s *MemoryVectorStore) Normalization() vector.Normalization { return s.calculator.Normalization() }

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

var _ PreparedVectorStore = (*MemoryVectorStore)(nil)
