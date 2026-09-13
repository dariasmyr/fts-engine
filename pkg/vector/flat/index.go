// Package flat provides mutable exact dense-vector search.
package flat

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/vector/internal/exactsearch"
)

var (
	ErrInvalidMaxVectors = errors.New("vector/flat: max vectors must be positive and below the uint32 ordinal limit")
	ErrInvalidMaxK       = errors.New("vector/flat: max k must be positive")
	ErrInvalidCapacity   = errors.New("vector/flat: initial vector capacity must be between zero and max vectors")
	ErrCapacityExceeded  = errors.New("vector/flat: capacity exceeded")
)

type Config struct {
	Dimensions            int
	Metric                vector.Metric
	MaxVectors            int
	MaxK                  int
	InitialVectorCapacity int
}

// OrdinalRange describes contiguous ordinals assigned to one appended batch.
type OrdinalRange struct {
	Start vector.Ordinal
	Count uint32
}

func (r OrdinalRange) At(index int) (vector.Ordinal, bool) {
	if index < 0 || uint32(index) >= r.Count {
		return 0, false
	}
	return r.Start + vector.Ordinal(index), true
}

// Index stores prepared vectors as one contiguous row-major matrix.
type Index struct {
	mu         sync.RWMutex
	space      vector.Space
	maxVectors int
	maxK       int
	values     []float32
}

func New(config Config) (*Index, error) {
	space, err := vector.NewSpace(config.Dimensions, config.Metric)
	if err != nil {
		return nil, err
	}
	if config.MaxVectors <= 0 || uint64(config.MaxVectors) >= math.MaxUint32 ||
		config.MaxVectors > math.MaxInt/config.Dimensions {
		return nil, ErrInvalidMaxVectors
	}
	if config.MaxK <= 0 {
		return nil, ErrInvalidMaxK
	}
	if config.InitialVectorCapacity < 0 || config.InitialVectorCapacity > config.MaxVectors {
		return nil, ErrInvalidCapacity
	}
	return &Index{
		space:      space,
		maxVectors: config.MaxVectors,
		maxK:       config.MaxK,
		values:     make([]float32, 0, config.InitialVectorCapacity*config.Dimensions),
	}, nil
}

func (idx *Index) Add(value []float32) (vector.Ordinal, error) {
	ordinals, err := idx.AppendBatch([][]float32{value})
	if err != nil {
		return 0, err
	}
	return ordinals.Start, nil
}

// AppendBatch prepares and appends every vector or leaves the index unchanged.
// The returned range maps input vector i to Start+i without allocating one
// ordinal per vector.
func (idx *Index) AppendBatch(vectors [][]float32) (OrdinalRange, error) {
	if len(vectors) == 0 {
		return OrdinalRange{}, nil
	}
	if len(vectors) > idx.maxVectors {
		return OrdinalRange{}, fmt.Errorf("%w: batch=%d max=%d", ErrCapacityExceeded, len(vectors), idx.maxVectors)
	}
	if len(vectors) > int(^uint(0)>>1)/idx.space.Dimensions() {
		return OrdinalRange{}, ErrCapacityExceeded
	}

	dimensions := idx.space.Dimensions()
	// Prepare the complete batch in one allocation. Besides avoiding one
	// temporary allocation per vector, this keeps validation and normalization
	// outside the write lock and preserves all-or-nothing append semantics.
	prepared := make([]float32, len(vectors)*dimensions)
	for i, vectorValue := range vectors {
		rowStart := i * dimensions
		rowEnd := rowStart + dimensions
		// row is a view into prepared; slicing does not copy vector components.
		row := prepared[rowStart:rowEnd]
		if err := idx.space.PrepareInto(row, vectorValue); err != nil {
			return OrdinalRange{}, err
		}
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()
	start := idx.lenLocked()
	if len(vectors) > idx.maxVectors-start {
		return OrdinalRange{}, fmt.Errorf("%w: have=%d append=%d max=%d", ErrCapacityExceeded, start, len(vectors), idx.maxVectors)
	}

	// Copy the fully prepared temporary matrix into persistent index storage.
	// append always copies the new components; it allocates a larger backing
	// array only when idx.values has insufficient spare capacity.
	idx.values = append(idx.values, prepared...)
	return OrdinalRange{Start: vector.Ordinal(start), Count: uint32(len(vectors))}, nil
}

func (idx *Index) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return exactsearch.Search(ctx, idx.space, idx.values, idx.maxK, query, k, options)
}

// Compact returns a new mutable index containing only rows allowed by filter.
// Prepared vector components are copied exactly and are not normalized again.
func (idx *Index) Compact(ctx context.Context, filter vector.ResultFilter) (*Index, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	values, err := compactPrepared(ctx, idx.space.Dimensions(), idx.values, filter)
	if err != nil {
		return nil, err
	}
	return &Index{space: idx.space, maxVectors: idx.maxVectors, maxK: idx.maxK, values: values}, nil
}

func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.lenLocked()
}

// Dimensions returns the number of dimensions in the index.
func (idx *Index) Dimensions() int { return idx.space.Dimensions() }

func (idx *Index) Metric() vector.Metric { return idx.space.Metric() }

// lenLocked returns the number of vectors in the index. The caller must hold a read or write lock.
func (idx *Index) lenLocked() int { return len(idx.values) / idx.space.Dimensions() }

// Freeze copies the current matrix into an immutable concurrent reader.
func (idx *Index) Freeze() *Reader {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return newReader(idx.space, idx.maxK, append([]float32(nil), idx.values...))
}

// FreezeCompact returns an immutable reader containing only rows allowed by
// filter. Prepared components are copied once and are not normalized again.
func (idx *Index) FreezeCompact(ctx context.Context, filter vector.ResultFilter) (*Reader, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	values, err := compactPrepared(ctx, idx.space.Dimensions(), idx.values, filter)
	if err != nil {
		return nil, err
	}
	return newReader(idx.space, idx.maxK, values), nil
}

func compactPrepared(ctx context.Context, dimensions int, matrix []float32, filter vector.ResultFilter) ([]float32, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rowCount := len(matrix) / dimensions
	allowedCount := rowCount
	if filter != nil {
		if filter.TotalOrdinalCount() != uint32(rowCount) {
			return nil, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, filter.TotalOrdinalCount(), rowCount)
		}
		allowedCount = filter.AllowedOrdinalCount()
		if allowedCount < 0 || allowedCount > rowCount {
			return nil, vector.ErrInvalidSearchOptions
		}
	}

	compacted := make([]float32, 0, allowedCount*dimensions)
	for row := range rowCount {
		if err := contextcheck.PeriodicError(ctx, row); err != nil {
			return nil, err
		}
		if filter != nil && !filter.Allows(vector.Ordinal(row)) {
			continue
		}
		start := row * dimensions
		compacted = append(compacted, matrix[start:start+dimensions]...)
	}
	if len(compacted)/dimensions != allowedCount {
		return nil, vector.ErrInvalidSearchOptions
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return compacted, nil
}
