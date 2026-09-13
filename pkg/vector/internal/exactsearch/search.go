// Package exactsearch implements exact search over a prepared row-major matrix.
package exactsearch

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/internal/contextcheck"
)

func Search(ctx context.Context, space vector.Space, matrix []float32, maxK int, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if ctx == nil {
		return vector.SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	if k <= 0 || k > maxK {
		return vector.SearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxK)
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return vector.SearchResult{}, vector.ErrInvalidSearchOptions
	}
	preparedQuery, err := space.Prepare(query)
	if err != nil {
		return vector.SearchResult{}, err
	}

	rowCount := len(matrix) / space.Dimensions()
	allowedCount, err := AllowedCount(rowCount, options.ResultFilter)
	if err != nil {
		return vector.SearchResult{}, err
	}
	topK := NewTopK(min(k, allowedCount))
	stats := vector.SearchStats{Termination: vector.TerminationComplete}
	incomplete := false

	for row := range rowCount {
		if options.VisitLimit > 0 && stats.VisitedNodes >= options.VisitLimit {
			stats.Termination = vector.TerminationVisitLimit
			incomplete = true
			break
		}
		if err := contextcheck.PeriodicError(ctx, row); err != nil {
			return vector.SearchResult{}, err
		}
		stats.VisitedNodes++
		ordinal := vector.Ordinal(row)
		if options.ResultFilter != nil && !options.ResultFilter.Allows(ordinal) {
			stats.RejectedNodes++
			continue
		}
		rowStart := row * space.Dimensions()
		distance := space.DistancePrepared(preparedQuery, matrix[rowStart:rowStart+space.Dimensions()])
		stats.DistanceComputations++
		topK.Add(vector.Hit{Ordinal: ordinal, Distance: distance})
	}

	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	return vector.SearchResult{Hits: topK.Results(), Stats: stats, Incomplete: incomplete}, nil
}

// SearchSource performs the same exact scan over an immutable prepared source.
func SearchSource(ctx context.Context, source vector.PreparedVectorSource, maxK int, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if source == nil {
		return vector.SearchResult{}, vector.ErrInvalidSearchOptions
	}
	if ctx == nil {
		return vector.SearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	if k <= 0 || k > maxK {
		return vector.SearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxK)
	}
	if options.EfSearch < 0 || options.VisitLimit < 0 {
		return vector.SearchResult{}, vector.ErrInvalidSearchOptions
	}
	space, err := vector.NewSpace(source.Dimensions(), source.Metric())
	if err != nil || space.Normalization() != source.Normalization() {
		return vector.SearchResult{}, vector.ErrInvalidSearchOptions
	}
	preparedQuery, err := space.Prepare(query)
	if err != nil {
		return vector.SearchResult{}, err
	}
	allowedCount, err := AllowedCount(source.Len(), options.ResultFilter)
	if err != nil {
		return vector.SearchResult{}, err
	}
	topK := NewTopK(min(k, allowedCount))
	stats := vector.SearchStats{Termination: vector.TerminationComplete}
	value := make([]float32, source.Dimensions())
	incomplete := false
	for row := range source.Len() {
		if options.VisitLimit > 0 && stats.VisitedNodes >= options.VisitLimit {
			stats.Termination = vector.TerminationVisitLimit
			incomplete = true
			break
		}
		if err := contextcheck.PeriodicError(ctx, row); err != nil {
			return vector.SearchResult{}, err
		}
		stats.VisitedNodes++
		ordinal := vector.Ordinal(row)
		if options.ResultFilter != nil && !options.ResultFilter.Allows(ordinal) {
			stats.RejectedNodes++
			continue
		}
		if err := source.ReadVectorInto(ctx, ordinal, value); err != nil {
			return vector.SearchResult{}, err
		}
		distance := space.DistancePrepared(preparedQuery, value)
		stats.DistanceComputations++
		topK.Add(vector.Hit{Ordinal: ordinal, Distance: distance})
	}
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	return vector.SearchResult{Hits: topK.Results(), Stats: stats, Incomplete: incomplete}, nil
}

// AllowedCount validates a filter and returns its declared eligible row count.
func AllowedCount(rowCount int, filter vector.ResultFilter) (int, error) {
	if filter == nil {
		return rowCount, nil
	}
	if uint64(filter.TotalOrdinalCount()) != uint64(rowCount) {
		return 0, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, filter.TotalOrdinalCount(), rowCount)
	}
	allowedCount := filter.AllowedOrdinalCount()
	if allowedCount < 0 || allowedCount > rowCount {
		return 0, vector.ErrInvalidSearchOptions
	}
	return allowedCount, nil
}
