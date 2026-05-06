package bench

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/internal/domain/models"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func mkDoc(id, title, abstract string) models.Document {
	return models.Document{
		DocumentBase: models.DocumentBase{Title: title, Abstract: abstract},
		ID:           id,
	}
}

func TestCorpusTitleIndex(t *testing.T) {
	corpus := Corpus{
		mkDoc("1", "  Rosa Barge  ", "french hotel barge"),
		mkDoc("2", "Obama Speech", "presidential address"),
		mkDoc("3", "", "no title"),
	}

	idx := corpus.TitleIndex()
	if got, want := idx["rosa barge"], "1"; got != want {
		t.Fatalf("idx[rosa barge] = %q, want %q", got, want)
	}
	if _, ok := idx[""]; ok {
		t.Fatal("empty title should not be indexed")
	}
}

func TestResolveRelevant(t *testing.T) {
	titleIdx := map[string]string{"rosa barge": "1", "obama speech": "2"}
	q := Query{
		RelevantIDs:    []string{"raw-id-3"},
		RelevantTitles: []string{"Rosa Barge", "Unknown Title"},
	}

	got := ResolveRelevant(q, titleIdx)
	want := []string{"raw-id-3", "1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ResolveRelevant() = %v, want %v", got, want)
	}
}

func TestCountMissingTitles(t *testing.T) {
	titleIdx := map[string]string{"rosa barge": "1"}
	gt := &GroundTruth{Queries: []Query{
		{RelevantTitles: []string{"Rosa Barge", "Unknown"}},
		{RelevantTitles: []string{"Another Unknown"}},
	}}

	if got := CountMissingTitles(gt, titleIdx); got != 2 {
		t.Fatalf("CountMissingTitles() = %d, want 2", got)
	}
}

func TestIndexAndRunQueriesEndToEnd(t *testing.T) {
	ctx := context.Background()
	corpus := Corpus{
		mkDoc("1", "Rosa Barge", "Rosa is a French hotel barge on the Canal du Midi"),
		mkDoc("2", "Barack Obama", "Obama delivered a speech on climate change"),
		mkDoc("3", "Empty", "unrelated content about cats and dogs"),
	}

	svc := fts.New(radix.New(), keygen.Word)
	idxReport, err := IndexCorpus(ctx, svc, corpus, SelectAbstract)
	if err != nil {
		t.Fatalf("IndexCorpus() error = %v", err)
	}
	if idxReport.DocumentCount != 3 {
		t.Fatalf("DocumentCount = %d, want 3", idxReport.DocumentCount)
	}

	gt := &GroundTruth{Queries: []Query{
		{Query: "french hotel barge", RelevantTitles: []string{"Rosa Barge"}},
		{Query: "obama speech", RelevantTitles: []string{"Barack Obama"}},
	}}

	titleIdx := corpus.TitleIndex()
	queryReports, err := RunQueries(ctx, svc, gt, titleIdx, 10)
	if err != nil {
		t.Fatalf("RunQueries() error = %v", err)
	}
	if len(queryReports) != 2 {
		t.Fatalf("len(queryReports) = %d, want 2", len(queryReports))
	}
	for _, qr := range queryReports {
		if qr.ExecutionStrategy == "" {
			t.Fatalf("query %q missing ExecutionStrategy", qr.Query)
		}
		if qr.DiagnosticsTotal <= 0 {
			t.Fatalf("query %q missing DiagnosticsTotal, got %v", qr.Query, qr.DiagnosticsTotal)
		}
	}

	report := Aggregate(10, idxReport, queryReports)
	if report.MeanNDCG <= 0 {
		t.Fatalf("MeanNDCG = %v, want > 0", report.MeanNDCG)
	}
	if report.MeanMRR <= 0 {
		t.Fatalf("MeanMRR = %v, want > 0", report.MeanMRR)
	}
	if report.DiagnosticsTotalP50 <= 0 {
		t.Fatalf("DiagnosticsTotalP50 = %v, want > 0", report.DiagnosticsTotalP50)
	}
	strategyCount := 0
	for _, st := range report.Strategies {
		strategyCount += st.Count
	}
	if strategyCount != len(queryReports) {
		t.Fatalf("strategy counts sum = %d, want %d (strategies=%+v)", strategyCount, len(queryReports), report.Strategies)
	}
}

func TestAggregateIncludesDiagnosticsAndStrategies(t *testing.T) {
	queries := []QueryReport{
		{
			Query:                   "alpha",
			NDCG:                    1,
			MRR:                     1,
			Recall:                  1,
			Latency:                 10 * time.Millisecond,
			ExecutionStrategy:       "term",
			IndexSearches:           2,
			PostingEntriesRead:      5,
			DiagnosticsTotal:        8 * time.Millisecond,
			DiagnosticsSearchTokens: 6 * time.Millisecond,
		},
		{
			Query:                   "alpha beta",
			NDCG:                    0.5,
			MRR:                     0.5,
			Recall:                  0.5,
			Latency:                 20 * time.Millisecond,
			ExecutionStrategy:       "bool_or_wand",
			IndexSearches:           4,
			PostingEntriesRead:      15,
			DiagnosticsTotal:        16 * time.Millisecond,
			DiagnosticsSearchTokens: 12 * time.Millisecond,
		},
	}

	report := Aggregate(10, IndexReport{}, queries)
	if report.DiagnosticsTotalP50 != 12*time.Millisecond {
		t.Fatalf("DiagnosticsTotalP50 = %v, want 12ms", report.DiagnosticsTotalP50)
	}
	if report.DiagnosticsSearchP50 != 9*time.Millisecond {
		t.Fatalf("DiagnosticsSearchP50 = %v, want 9ms", report.DiagnosticsSearchP50)
	}
	if report.MeanPostingEntriesRead != 10 {
		t.Fatalf("MeanPostingEntriesRead = %v, want 10", report.MeanPostingEntriesRead)
	}
	if report.MeanIndexSearches != 3 {
		t.Fatalf("MeanIndexSearches = %v, want 3", report.MeanIndexSearches)
	}
	if report.Strategies["term"].Count != 1 {
		t.Fatalf("term strategy count = %d, want 1", report.Strategies["term"].Count)
	}
	if report.Strategies["bool_or_wand"].TotalPostingsRead != 15 {
		t.Fatalf("bool_or_wand postings = %d, want 15", report.Strategies["bool_or_wand"].TotalPostingsRead)
	}
}
