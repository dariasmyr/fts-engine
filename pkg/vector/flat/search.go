package flat

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const contextCheckInterval = 256

func searchExact(ctx context.Context, space vector.Space, matrix []float32, maxK int, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	// Validate the request and prepare the query for the configured metric.
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

	// Validate the result filter and bound the result collector.
	rowCount := len(matrix) / space.Dimensions()
	resultFilter := options.ResultFilter
	if resultFilter != nil && uint64(resultFilter.TotalOrdinalCount()) != uint64(rowCount) {
		return vector.SearchResult{}, fmt.Errorf("%w: got %d, want %d", vector.ErrResultFilterSizeMismatch, resultFilter.TotalOrdinalCount(), rowCount)
	}
	hitLimit := min(k, rowCount)
	if resultFilter != nil {
		hitLimit = min(hitLimit, resultFilter.AllowedOrdinalCount())
	}
	topK := newExactTopK(hitLimit)
	stats := vector.SearchStats{Termination: vector.TerminationComplete}
	incomplete := false

	// Scan each row once and calculate distance only for allowed ordinals.
	for row := range rowCount {
		if options.VisitLimit > 0 && stats.VisitedNodes >= options.VisitLimit {
			stats.Termination = vector.TerminationVisitLimit
			incomplete = true
			break
		}
		// Check periodically to avoid a context call for every matrix row.
		if row%contextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return vector.SearchResult{}, err
			}
		}
		stats.VisitedNodes++
		ordinal := vector.Ordinal(row)
		if resultFilter != nil && !resultFilter.Allows(ordinal) {
			stats.RejectedNodes++
			continue
		}
		rowStart := row * space.Dimensions()
		distance := space.DistancePrepared(preparedQuery, matrix[rowStart:rowStart+space.Dimensions()])
		stats.DistanceComputations++
		topK.Add(vector.Hit{Ordinal: ordinal, Distance: distance})
	}

	// Cancellation returns an error rather than a partial result.
	if err := ctx.Err(); err != nil {
		return vector.SearchResult{}, err
	}
	return vector.SearchResult{Hits: topK.Results(), Stats: stats, Incomplete: incomplete}, nil
}
