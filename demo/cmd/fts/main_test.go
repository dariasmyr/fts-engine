package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/demo/internal/config"
	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
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

	got := adapter.HighlightPlainText("jane*", "doe doecare orbit")
	if strings.Count(got, "\033[31m") != 0 {
		t.Fatalf("plain-text highlight should not treat '*' as prefix syntax, got %q", got)
	}
	if !strings.Contains(got, "doe doecare orbit") {
		t.Fatalf("plain-text highlight should fall back to raw text, got %q", got)
	}

	got = adapter.HighlightPlainText("doe doecare", "doe doecare orbit")
	if strings.Count(got, "\033[31m") != 2 {
		t.Fatalf("expected 2 highlighted plain-text matches, got %q", got)
	}
}

func TestServiceAdapterHighlightQueryStringUsesSyntax(t *testing.T) {
	svc := fts.New(slicedradix.New(), fts.WordKeys)
	adapter := &serviceAdapter{service: svc}

	got := adapter.HighlightQueryString("doe*", "doe doecare orbit")
	if strings.Count(got, "\033[31m") != 2 {
		t.Fatalf("expected 2 highlighted syntax matches, got %q", got)
	}
}

func TestServiceAdapterSearchQueryStringUsesParserSemantics(t *testing.T) {
	svc := fts.New(slicedradix.New(), fts.WordKeys)
	ctx := context.Background()
	if err := indexTestDocument(ctx, svc, "doc-a", "james doe"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	if err := indexTestDocument(ctx, svc, "doc-b", "doe only"); err != nil {
		t.Fatalf("Index(doc-b) error = %v", err)
	}

	adapter := &serviceAdapter{service: svc}
	res, err := adapter.SearchQueryString(ctx, `"james doe"`, 10)
	if err != nil {
		t.Fatalf("SearchQueryString() error = %v", err)
	}
	if len(res.ResultData) != 1 || res.ResultData[0].ID != "doc-a" {
		t.Fatalf("unexpected syntax-mode results: %+v", res.ResultData)
	}
}

func TestServiceAdapterSearchQueryStringProjectsScoreExplanation(t *testing.T) {
	svc := fts.NewMultiField(func(string) (fts.Index, error) {
		return slicedradix.New(), nil
	}, fts.WordKeys, fts.WithRankProfile(fts.RankProfile{
		Base:         fts.BM25(),
		FieldWeights: fts.FieldWeights{"title": 3},
	}))
	ctx := context.Background()
	if err := svc.Index(ctx, fts.Document{ID: "doc-a", Fields: map[string]fts.Field{
		"title": {Value: "alpha"},
	}}); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}

	adapter := &serviceAdapter{service: svc}
	res, err := adapter.SearchQueryString(ctx, "title:alpha", 10)
	if err != nil {
		t.Fatalf("SearchQueryString() error = %v", err)
	}
	if len(res.ResultData) != 1 {
		t.Fatalf("ResultData len = %d, want 1", len(res.ResultData))
	}
	got := res.ResultData[0]
	if got.Score == 0 {
		t.Fatalf("expected projected score, got %+v", got)
	}
	if got.Explanation == nil || len(got.Explanation.Contributions) != 1 {
		t.Fatalf("expected projected explanation, got %+v", got.Explanation)
	}
	if c := got.Explanation.Contributions[0]; c.Field != "title" || c.FieldWeight != 3 || c.Score == 0 {
		t.Fatalf("unexpected explanation contribution: %+v", c)
	}
}

func TestBuildServiceUsesConfiguredRankProfile(t *testing.T) {
	opt, err := selectScoringOption(config.FTSConfig{
		Scorer: "bm25",
		RankProfile: config.RankProfileConfig{
			Name:             "title-2",
			FieldWeights:     map[string]float64{"title": 2},
			QueryTypeWeights: config.QueryTypeWeightsConfig{Phrase: 3},
		},
	})
	if err != nil {
		t.Fatalf("selectScoringOption() error = %v", err)
	}
	if opt == nil {
		t.Fatal("selectScoringOption() = nil, want rank profile option")
	}
	svc := fts.NewMultiField(func(string) (fts.Index, error) {
		return slicedradix.New(), nil
	}, fts.WordKeys, opt)
	ctx := context.Background()
	if err := svc.Index(ctx, fts.Document{ID: "doc-a", Fields: map[string]fts.Field{
		"title": {Value: "alpha beta"},
	}}); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	explanation, err := svc.Explain(ctx, `title:"alpha beta"`, "doc-a")
	if err != nil {
		t.Fatalf("Explain() error = %v", err)
	}
	if len(explanation.Contributions) != 1 || explanation.Contributions[0].FieldWeight != 2 || explanation.Contributions[0].QueryTypeWeight != 3 {
		t.Fatalf("unexpected explanation: %+v", explanation)
	}
}

func TestIndexDocumentUsesNamedFields(t *testing.T) {
	svc := fts.NewMultiField(func(string) (fts.Index, error) {
		return slicedradix.New(), nil
	}, fts.WordKeys)
	doc := models.Document{
		DocumentBase: models.DocumentBase{
			Title:    "James Doe",
			Abstract: "Australian hotel",
		},
		ID:      "doc-a",
		Extract: "Long biography extract",
	}

	if err := indexDocument(context.Background(), svc, doc); err != nil {
		t.Fatalf("indexDocument() error = %v", err)
	}

	fields := svc.Fields()
	if len(fields) != 3 || fields[0] != "abstract" || fields[1] != "extract" || fields[2] != "title" {
		t.Fatalf("Fields() = %v, want [abstract extract title]", fields)
	}

	res, err := svc.SearchDocuments(context.Background(), "title:james", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(title:james) error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("unexpected title field results: %+v", res.Results)
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
