package posting

import (
	"context"
	"sync/atomic"
)

// TraversalMetrics counts physical posting work performed by storage cursors.
// It is safe to share between cursors and concurrent field searches.
type TraversalMetrics struct {
	decodedBlocks   atomic.Uint64
	decodedPostings atomic.Uint64
}

// TraversalMetricsSnapshot is a point-in-time copy of physical posting work.
type TraversalMetricsSnapshot struct {
	DecodedBlocks   uint64
	DecodedPostings uint64
}

type traversalMetricsContextKey struct{}

// WithTraversalMetrics enables physical posting traversal metrics in ctx.
func WithTraversalMetrics(ctx context.Context) (context.Context, *TraversalMetrics) {
	if ctx == nil {
		ctx = context.Background()
	}
	metrics := &TraversalMetrics{}
	return context.WithValue(ctx, traversalMetricsContextKey{}, metrics), metrics
}

// TraversalMetricsFromContext returns the metrics attached to ctx, if any.
func TraversalMetricsFromContext(ctx context.Context) *TraversalMetrics {
	if ctx == nil {
		return nil
	}
	metrics, _ := ctx.Value(traversalMetricsContextKey{}).(*TraversalMetrics)
	return metrics
}

// RecordDecodedBlock records one successfully decoded posting block.
// Storage implementations should call it once the complete block is validated.
func (m *TraversalMetrics) RecordDecodedBlock(postings int) {
	if m == nil {
		return
	}
	m.decodedBlocks.Add(1)
	m.decodedPostings.Add(uint64(postings))
}

// Snapshot returns the current counters.
func (m *TraversalMetrics) Snapshot() TraversalMetricsSnapshot {
	if m == nil {
		return TraversalMetricsSnapshot{}
	}
	return TraversalMetricsSnapshot{
		DecodedBlocks:   m.decodedBlocks.Load(),
		DecodedPostings: m.decodedPostings.Load(),
	}
}
