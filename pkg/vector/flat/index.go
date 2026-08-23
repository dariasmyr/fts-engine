// Package flat provides mutable exact dense-vector search.
package flat

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/vector"
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
	if ctx == nil {
		return vector.SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	if k <= 0 {
		return vector.SearchResult{}, vector.ErrInvalidK
	}
	if k > idx.maxK {
		return vector.SearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, idx.maxK)
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return vector.SearchResult{}, vector.ErrInvalidSearchOptions
	}
	preparedQuery, err := idx.space.Prepare(query)
	if err != nil {
		return vector.SearchResult{}, err
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()
	count := idx.lenLocked()
	if options.Accept != nil && uint64(options.Accept.Size()) != uint64(count) {
		return vector.SearchResult{}, fmt.Errorf("%w: got %d, want %d", vector.ErrAcceptSetSizeMismatch, options.Accept.Size(), count)
	}

	resultLimit := min(k, count)
	if options.Accept != nil {
		resultLimit = min(resultLimit, options.Accept.Cardinality())
	}
	top := newExactTopK(resultLimit)
	stats := vector.SearchStats{Termination: vector.TerminationComplete}
	incomplete := false
	for row := 0; row < count; row++ {
		if options.VisitLimit > 0 && stats.VisitedNodes >= options.VisitLimit {
			stats.Termination = vector.TerminationVisitLimit
			incomplete = true
			break
		}
		if row%256 == 0 {
			if err := ctx.Err(); err != nil {
				return vector.SearchResult{}, err
			}
		}

		stats.VisitedNodes++
		ord := vector.Ordinal(row)
		if options.Accept != nil && !options.Accept.Contains(ord) {
			stats.RejectedNodes++
			continue
		}
		start := row * idx.space.Dimensions()
		distance := idx.space.DistancePrepared(preparedQuery, idx.values[start:start+idx.space.Dimensions()])
		stats.DistanceComputations++
		top.Add(vector.Hit{Ordinal: ord, Distance: distance})
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	return vector.SearchResult{Hits: top.Results(), Stats: stats, Incomplete: incomplete}, nil
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

var _ vector.Searcher = (*Index)(nil)
