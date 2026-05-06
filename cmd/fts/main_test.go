package main

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
)

func TestServiceAdapterObservesSearchDiagnostics(t *testing.T) {
	svc := fts.New(radix.New(), fts.WordKeys)
	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-1", "alpha beta gamma"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-2", "alpha delta"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	adapter := &serviceAdapter{service: svc, searchStats: ftsstats.NewSearchStats(8)}
	if _, err := adapter.SearchDocuments(ctx, "alpha", 10); err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	snap, ok := adapter.SearchStatsSnapshot()
	if !ok {
		t.Fatal("expected SearchStatsSnapshot to be available")
	}
	if snap.TotalSearches != 1 {
		t.Fatalf("TotalSearches = %d, want 1", snap.TotalSearches)
	}
	if len(snap.ByStrategy) == 0 {
		t.Fatalf("expected strategy aggregation, got %+v", snap.ByStrategy)
	}
	if len(snap.Recent) != 1 || snap.Recent[0].ExecutionStrategy == "" || snap.Recent[0].TotalDuration <= 0 {
		t.Fatalf("unexpected recent event: %+v", snap.Recent)
	}
}
