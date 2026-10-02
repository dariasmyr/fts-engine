// Package exact provides an immutable exact-search oracle for vector benchmarks.
package exact

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

// Oracle performs an exact scan over an immutable prepared vector store.
type Oracle struct {
	store      vectorstore.PreparedVectorStore
	calculator vector.Calculator
	maxK       int
}

// New prepares and copies the complete vector set into an immutable oracle.
func New(vectors [][]float32, dimensions int, metric vector.Metric, maxK int) (*Oracle, error) {
	calculator, err := vector.NewCalculator(dimensions, metric)
	if err != nil {
		return nil, err
	}
	store, err := vectorstore.NewMemoryVectorStore(calculator, vectors)
	if err != nil {
		return nil, err
	}
	return NewFromPreparedStore(store, maxK)
}

// NewFromPreparedStore constructs an oracle over a complete immutable store.
func NewFromPreparedStore(store vectorstore.PreparedVectorStore, maxK int) (*Oracle, error) {
	if store == nil {
		return nil, vector.ErrInvalidSearchOptions
	}
	if maxK <= 0 {
		return nil, fmt.Errorf("%w: got %d", vector.ErrInvalidK, maxK)
	}
	calculator, err := vector.NewCalculator(store.Dimensions(), store.Metric())
	if err != nil {
		return nil, err
	}
	if calculator.Normalization() != store.Normalization() {
		return nil, vector.ErrInvalidSearchOptions
	}
	return &Oracle{store: store, calculator: calculator, maxK: maxK}, nil
}

// Search returns the exact nearest vectors in distance and ordinal order.
func (o *Oracle) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if ctx == nil {
		return vector.SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	if k <= 0 || k > o.maxK {
		return vector.SearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, o.maxK)
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return vector.SearchResult{}, vector.ErrInvalidSearchOptions
	}
	preparedQuery, err := o.calculator.Prepare(query)
	if err != nil {
		return vector.SearchResult{}, err
	}
	allowedCount, err := allowedCount(o.store.Len(), options.ResultFilter)
	if err != nil {
		return vector.SearchResult{}, err
	}

	topK := newTopK(min(k, allowedCount))
	stats := vector.SearchStats{Termination: vector.TerminationComplete}
	value := make([]float32, o.store.Dimensions())
	incomplete := false
	for row := range o.store.Len() {
		if options.VisitLimit > 0 && stats.VisitedNodes >= options.VisitLimit {
			stats.Termination = vector.TerminationVisitLimit
			incomplete = true
			break
		}
		if err := periodicContextError(ctx, row); err != nil {
			return vector.SearchResult{}, err
		}
		stats.VisitedNodes++
		ordinal := vector.Ordinal(row)
		if options.ResultFilter != nil && !options.ResultFilter.Allows(ordinal) {
			stats.RejectedNodes++
			continue
		}
		if err := o.store.ReadVectorInto(ctx, ordinal, value); err != nil {
			return vector.SearchResult{}, err
		}
		distance := o.calculator.DistancePrepared(preparedQuery, value)
		stats.DistanceComputations++
		topK.add(vector.Hit{Ordinal: ordinal, Distance: distance})
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	return vector.SearchResult{Hits: topK.results(), Stats: stats, Incomplete: incomplete}, nil
}

// Len returns the number of vectors searched by the oracle.
func (o *Oracle) Len() int { return o.store.Len() }

// Store returns the immutable prepared store used by the oracle.
func (o *Oracle) Store() vectorstore.PreparedVectorStore { return o.store }

func allowedCount(rowCount int, filter vector.ResultFilter) (int, error) {
	if filter == nil {
		return rowCount, nil
	}
	if uint64(filter.TotalOrdinalCount()) != uint64(rowCount) {
		return 0, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, filter.TotalOrdinalCount(), rowCount)
	}
	allowed := filter.AllowedOrdinalCount()
	if allowed < 0 || allowed > rowCount {
		return 0, vector.ErrInvalidSearchOptions
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

var _ vector.Index = (*Oracle)(nil)
