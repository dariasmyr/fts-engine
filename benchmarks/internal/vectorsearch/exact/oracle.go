// Package exact provides an immutable exact-search oracle for vector benchmarks.
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

// Oracle performs an exact scan over an immutable prepared vector store.
type Oracle struct {
	store      vector.PreparedVectorStore
	calculator vector.Calculator
	maxK       int
}

// New prepares and copies the complete vector set into an immutable oracle.
func New(vectors [][]float32, dimensions int, metric vector.Metric, maxK int) (*Oracle, error) {
	calculator, err := vector.NewCalculator(dimensions, metric)
	if err != nil {
		return nil, err
	}
	store, err := newPreparedStore(calculator, vectors)
	if err != nil {
		return nil, err
	}
	return NewFromPreparedStore(store, maxK)
}

// NewFromPreparedStore constructs an oracle over a complete immutable store.
func NewFromPreparedStore(store vector.PreparedVectorStore, maxK int) (*Oracle, error) {
	if store == nil {
		return nil, ErrInvalidOptions
	}
	if maxK <= 0 {
		return nil, fmt.Errorf("%w: got %d", vector.ErrInvalidK, maxK)
	}
	calculator, err := vector.NewCalculator(store.Dimensions(), store.Metric())
	if err != nil {
		return nil, err
	}
	if calculator.Normalization() != store.Normalization() {
		return nil, ErrInvalidOptions
	}
	return &Oracle{store: store, calculator: calculator, maxK: maxK}, nil
}

// Search returns the exact nearest vectors in distance and ordinal order.
func (o *Oracle) Search(ctx context.Context, query []float32, k int, options Options) (Result, error) {
	if ctx == nil {
		return Result{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if k <= 0 || k > o.maxK {
		return Result{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, o.maxK)
	}
	if options.VisitLimit < 0 {
		return Result{}, ErrInvalidOptions
	}
	preparedQuery, err := o.calculator.Prepare(query)
	if err != nil {
		return Result{}, err
	}
	allowedCount, err := allowedCount(o.store.Len(), options.ResultFilter)
	if err != nil {
		return Result{}, err
	}

	topK := newTopK(min(k, allowedCount))
	stats := Stats{Termination: TerminationComplete}
	value := make([]float32, o.store.Dimensions())
	incomplete := false
	for row := range o.store.Len() {
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
		if err := o.store.ReadVectorInto(ctx, ordinal, value); err != nil {
			return Result{}, err
		}
		distance := o.calculator.DistancePrepared(preparedQuery, value)
		stats.DistanceComputations++
		topK.add(vector.Hit{Ordinal: ordinal, Distance: distance})
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return Result{Hits: topK.results(), Stats: stats, Incomplete: incomplete}, nil
}

// Len returns the number of vectors searched by the oracle.
func (o *Oracle) Len() int { return o.store.Len() }

// Store returns the immutable prepared store used by the oracle.
func (o *Oracle) Store() vector.PreparedVectorStore { return o.store }

type preparedStore struct {
	calculator vector.Calculator
	values     []float32
}

func newPreparedStore(calculator vector.Calculator, values [][]float32) (*preparedStore, error) {
	flat := make([]float32, len(values)*calculator.Dimensions())
	for row, value := range values {
		start := row * calculator.Dimensions()
		if err := calculator.PrepareInto(flat[start:start+calculator.Dimensions()], value); err != nil {
			return nil, err
		}
	}
	return &preparedStore{calculator: calculator, values: flat}, nil
}

func (s *preparedStore) Len() int { return len(s.values) / s.calculator.Dimensions() }

func (s *preparedStore) Dimensions() int { return s.calculator.Dimensions() }

func (s *preparedStore) Metric() vector.Metric { return s.calculator.Metric() }

func (s *preparedStore) Normalization() vector.Normalization { return s.calculator.Normalization() }

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
