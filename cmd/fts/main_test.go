package main

import (
	"context"
	"strings"
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
	res, err := adapter.SearchDocuments(ctx, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if res.Diagnostics == nil {
		t.Fatal("expected projected diagnostics in models.SearchResult")
	}
	if res.Diagnostics.ExecutionStrategy == "" {
		t.Fatalf("expected execution strategy, got %+v", res.Diagnostics)
	}
	if res.Diagnostics.Timings["total"] == "" {
		t.Fatalf("expected formatted total timing, got %+v", res.Diagnostics.Timings)
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

func TestServiceAdapterHighlightTextUsesFTSHighlighter(t *testing.T) {
	svc := fts.New(radix.New(), fts.WordKeys)
	adapter := &serviceAdapter{service: svc}

	got := adapter.HighlightText("obam*", "obama obamacare orbit")
	if strings.Count(got, "\033[31m") != 2 {
		t.Fatalf("expected 2 highlighted matches, got %q", got)
	}
	if strings.Contains(got, "\033[31morbit\033[0m") {
		t.Fatalf("unexpected highlight for non-matching word: %q", got)
	}
}
