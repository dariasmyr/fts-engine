package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/internal/domain/models"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
)

func indexTestDocument(ctx context.Context, svc *fts.Service, docID, content string) error {
	return svc.Index(ctx, fts.Document{ID: fts.DocID(docID), Fields: map[string]fts.Field{fts.DefaultField: {Value: content}}})
}

func TestServiceAdapterObservesSearchDiagnostics(t *testing.T) {
	svc := fts.New(slicedradix.New(), fts.WordKeys)
	ctx := context.Background()
	if err := indexTestDocument(ctx, svc, "doc-1", "alpha beta gamma"); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}
	if err := indexTestDocument(ctx, svc, "doc-2", "alpha delta"); err != nil {
		t.Fatalf("Index(doc-2) error = %v", err)
	}

	adapter := &serviceAdapter{service: svc, searchStats: ftsstats.NewSearchStats(8)}
	res, err := adapter.SearchPlainText(ctx, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchPlainText() error = %v", err)
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

func TestServiceAdapterHighlightPlainTextUsesFTSHighlighter(t *testing.T) {
	svc := fts.New(slicedradix.New(), fts.WordKeys)
	adapter := &serviceAdapter{service: svc}

	got := adapter.HighlightPlainText("obam*", "obama obamacare orbit")
	if strings.Count(got, "\033[31m") != 0 {
		t.Fatalf("plain-text highlight should not treat '*' as prefix syntax, got %q", got)
	}
	if !strings.Contains(got, "obama obamacare orbit") {
		t.Fatalf("plain-text highlight should fall back to raw text, got %q", got)
	}

	got = adapter.HighlightPlainText("obama obamacare", "obama obamacare orbit")
	if strings.Count(got, "\033[31m") != 2 {
		t.Fatalf("expected 2 highlighted plain-text matches, got %q", got)
	}
}

func TestServiceAdapterHighlightQueryStringUsesSyntax(t *testing.T) {
	svc := fts.New(slicedradix.New(), fts.WordKeys)
	adapter := &serviceAdapter{service: svc}

	got := adapter.HighlightQueryString("obam*", "obama obamacare orbit")
	if strings.Count(got, "\033[31m") != 2 {
		t.Fatalf("expected 2 highlighted syntax matches, got %q", got)
	}
}

func TestServiceAdapterSearchQueryStringUsesParserSemantics(t *testing.T) {
	svc := fts.New(slicedradix.New(), fts.WordKeys)
	ctx := context.Background()
	if err := indexTestDocument(ctx, svc, "doc-a", "barack obama"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	if err := indexTestDocument(ctx, svc, "doc-b", "obama only"); err != nil {
		t.Fatalf("Index(doc-b) error = %v", err)
	}

	adapter := &serviceAdapter{service: svc}
	res, err := adapter.SearchQueryString(ctx, `"barack obama"`, 10)
	if err != nil {
		t.Fatalf("SearchQueryString() error = %v", err)
	}
	if len(res.ResultData) != 1 || res.ResultData[0].ID != "doc-a" {
		t.Fatalf("unexpected syntax-mode results: %+v", res.ResultData)
	}
}

func TestPopulateDocumentsSkipsReindexWhenPersistenceLoaded(t *testing.T) {
	documents := []models.Document{
		{DocumentBase: models.DocumentBase{Abstract: "alpha"}, ID: "doc-1"},
		{DocumentBase: models.DocumentBase{Abstract: "beta"}, ID: "doc-2"},
	}
	documentsByID := make(map[string]models.Document, len(documents))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	interrupted := populateDocuments(context.Background(), context.Background(), log, nil, documents, documentsByID, true)
	if interrupted {
		t.Fatal("populateDocuments() interrupted = true, want false")
	}
	if len(documentsByID) != len(documents) {
		t.Fatalf("documentsByID size = %d, want %d", len(documentsByID), len(documents))
	}
	if _, ok := documentsByID["doc-1"]; !ok {
		t.Fatal("documentsByID missing doc-1")
	}
	if _, ok := documentsByID["doc-2"]; !ok {
		t.Fatal("documentsByID missing doc-2")
	}
}
