package posting

import (
	"context"
	"testing"
)

func TestTraversalMetrics(t *testing.T) {
	ctx, metrics := WithTraversalMetrics(context.Background())
	metrics.RecordDecodedBlock(128)
	metrics.RecordDecodedBlock(16)

	got := metrics.Snapshot()
	want := TraversalMetricsSnapshot{DecodedBlocks: 2, DecodedPostings: 144}
	if got != want {
		t.Fatalf("Snapshot() = %+v, want %+v", got, want)
	}
	if fromContext := TraversalMetricsFromContext(ctx); fromContext != metrics {
		t.Fatal("TraversalMetricsFromContext() did not return attached metrics")
	}
	if got := (*TraversalMetrics)(nil).Snapshot(); got != (TraversalMetricsSnapshot{}) {
		t.Fatalf("nil Snapshot() = %+v", got)
	}
}
