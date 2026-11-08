// Package exact provides exhaustive nearest-neighbor search for vector
// benchmarks.
package exact

import (
	"context"
	"errors"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

var ErrInvalidOptions = errors.New("vectorsearch/exact: invalid search options")

const (
	TerminationComplete   = "complete"
	TerminationVisitLimit = "visit_limit"
)

type Options struct {
	VisitLimit   int
	ResultFilter vector.ResultFilter
}

type Stats struct {
	VisitedNodes         int
	DistanceComputations int
	RejectedNodes        int
	Termination          string
}

type Result struct {
	Hits       []vector.Hit
	Stats      Stats
	Incomplete bool
}

// Searcher finds nearest vectors by scanning every eligible row in an immutable
// prepared vector store. It is a correctness baseline for approximate search.
type Searcher struct {
	store vector.PreparedVectorStore
	space vector.Space
	maxK  int
}

// New prepares and copies the complete vector set into an immutable Searcher.
func New(vectors [][]float32, dimensions int, metric vector.Metric, maxK int) (*Searcher, error) {
	space, err := vector.NewSpace(dimensions, metric)
	if err != nil {
		return nil, err
	}
	store, err := newPreparedStore(space, vectors)
	if err != nil {
		return nil, err
	}
	return NewFromPreparedStore(store, maxK)
}

// NewFromPreparedStore constructs a Searcher over a complete immutable store.
func NewFromPreparedStore(store vector.PreparedVectorStore, maxK int) (*Searcher, error) {
	if store == nil {
		return nil, ErrInvalidOptions
	}
	if maxK <= 0 {
		return nil, fmt.Errorf("%w: got %d", vector.ErrInvalidK, maxK)
	}
	space, err := vector.NewSpace(store.Dimensions(), store.Metric())
	if err != nil {
		return nil, err
	}
	if space.Normalization() != store.Normalization() {
		return nil, ErrInvalidOptions
	}
	return &Searcher{store: store, space: space, maxK: maxK}, nil
}

// Search returns the exact nearest vectors in distance and ordinal order.
func (s *Searcher) Search(ctx context.Context, query []float32, k int, options Options) (Result, error) {
	if ctx == nil {
		return Result{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if k <= 0 || k > s.maxK {
		return Result{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, s.maxK)
	}
	if options.VisitLimit < 0 {
		return Result{}, ErrInvalidOptions
	}
	preparedQuery, err := s.space.Prepare(query)
	if err != nil {
		return Result{}, err
	}
	allowedCount, err := allowedCount(s.store.Len(), options.ResultFilter)
	if err != nil {
		return Result{}, err
	}

	topK := newTopK(min(k, allowedCount))
	stats := Stats{Termination: TerminationComplete}
	value := make([]float32, s.store.Dimensions())
	incomplete := false
	for row := range s.store.Len() {
		if options.VisitLimit > 0 && stats.VisitedNodes >= options.VisitLimit {
			stats.Termination = TerminationVisitLimit
			incomplete = true
			break
		}
		if err := periodicContextError(ctx, row); err != nil {
			return Result{}, err
		}
		stats.VisitedNodes++
		ordinal := vector.Ordinal(row)
		if options.ResultFilter != nil && !options.ResultFilter.Allows(ordinal) {
			stats.RejectedNodes++
			continue
		}
		if err := s.store.ReadVectorInto(ctx, ordinal, value); err != nil {
			return Result{}, err
		}
		distance := s.space.DistancePreparedVector(preparedQuery, value)
		stats.DistanceComputations++
		topK.add(vector.Hit{Ordinal: ordinal, Distance: distance})
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return Result{Hits: topK.results(), Stats: stats, Incomplete: incomplete}, nil
}

// Len returns the number of vectors searched.
func (s *Searcher) Len() int { return s.store.Len() }

// Store returns the immutable prepared store used by exhaustive search.
func (s *Searcher) Store() vector.PreparedVectorStore { return s.store }

type preparedStore struct {
	space  vector.Space
	values []float32
}

func newPreparedStore(space vector.Space, values [][]float32) (*preparedStore, error) {
	flat := make([]float32, len(values)*space.Dimensions())
	for row, value := range values {
		start := row * space.Dimensions()
		if err := space.PrepareInto(flat[start:start+space.Dimensions()], value); err != nil {
			return nil, err
		}
	}
	return &preparedStore{space: space, values: flat}, nil
}

func (s *preparedStore) Len() int { return len(s.values) / s.space.Dimensions() }

func (s *preparedStore) Dimensions() int { return s.space.Dimensions() }

func (s *preparedStore) Metric() vector.Metric { return s.space.Metric() }

func (s *preparedStore) Normalization() vector.Normalization { return s.space.Normalization() }

func (s *preparedStore) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(dst) != s.Dimensions() || uint64(ordinal) >= uint64(s.Len()) {
		return ErrInvalidOptions
	}
	start := int(ordinal) * s.Dimensions()
	copy(dst, s.values[start:start+s.Dimensions()])
	return nil
}

func allowedCount(rowCount int, filter vector.ResultFilter) (int, error) {
	if filter == nil {
		return rowCount, nil
	}
	if uint64(filter.TotalOrdinalCount()) != uint64(rowCount) {
		return 0, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, filter.TotalOrdinalCount(), rowCount)
	}
	allowed := filter.AllowedOrdinalCount()
	if allowed < 0 || allowed > rowCount {
		return 0, ErrInvalidOptions
	}
	return allowed, nil
}

const contextCheckInterval = 256

func periodicContextError(ctx context.Context, iteration int) error {
	if (iteration+1)%contextCheckInterval == 0 {
		return ctx.Err()
	}
	return nil
}
