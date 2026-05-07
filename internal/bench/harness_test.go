package bench

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/internal/domain/models"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
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
	queryReports, err := RunQueries(ctx, svc, gt, titleIdx, 10, RunQueryOptions{Diagnostics: true})
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

	report := Aggregate(10, idxReport, queryReports, true)
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

func TestRunQueriesWithoutDiagnosticsLeavesInstrumentationFieldsEmpty(t *testing.T) {
	ctx := context.Background()
	corpus := Corpus{mkDoc("1", "Rosa Barge", "Rosa is a French hotel barge on the Canal du Midi")}

	svc := fts.New(radix.New(), keygen.Word)
	if _, err := IndexCorpus(ctx, svc, corpus, SelectAbstract); err != nil {
		t.Fatalf("IndexCorpus() error = %v", err)
	}

	gt := &GroundTruth{Queries: []Query{{Query: "french hotel barge", RelevantTitles: []string{"Rosa Barge"}}}}
	queryReports, err := RunQueries(ctx, svc, gt, corpus.TitleIndex(), 10, RunQueryOptions{})
	if err != nil {
		t.Fatalf("RunQueries() error = %v", err)
	}
	if len(queryReports) != 1 {
		t.Fatalf("len(queryReports) = %d, want 1", len(queryReports))
	}
	qr := queryReports[0]
	if qr.ExecutionStrategy != "" || qr.DiagnosticsTotal != 0 || qr.DiagnosticsSearchTokens != 0 || qr.IndexSearches != 0 || qr.PostingEntriesRead != 0 {
		t.Fatalf("expected empty diagnostics fields, got %+v", qr)
	}

	report := Aggregate(10, IndexReport{}, queryReports, false)
	if report.DiagnosticsEnabled {
		t.Fatal("expected DiagnosticsEnabled to be false")
	}
	if len(report.Strategies) != 0 {
		t.Fatalf("expected no strategy breakdown without diagnostics, got %+v", report.Strategies)
	}
}

func TestRunQueriesObserverTracksQueriesWithoutDiagnostics(t *testing.T) {
	ctx := context.Background()
	corpus := Corpus{mkDoc("1", "Rosa Barge", "Rosa is a French hotel barge on the Canal du Midi")}

	svc := fts.New(radix.New(), keygen.Word)
	if _, err := IndexCorpus(ctx, svc, corpus, SelectAbstract); err != nil {
		t.Fatalf("IndexCorpus() error = %v", err)
	}

	gt := &GroundTruth{Queries: []Query{{Query: "french hotel barge", RelevantTitles: []string{"Rosa Barge"}}}}
	stats := ftsstats.NewSearchStats(8)
	if _, err := RunQueries(ctx, svc, gt, corpus.TitleIndex(), 10, RunQueryOptions{Observer: stats}); err != nil {
		t.Fatalf("RunQueries() error = %v", err)
	}

	snap := stats.Snapshot()
	if snap.TotalSearches != 1 {
		t.Fatalf("TotalSearches = %d, want 1", snap.TotalSearches)
	}
	if len(snap.ByStrategy) != 0 {
		t.Fatalf("expected no strategy stats without diagnostics, got %+v", snap.ByStrategy)
	}
	if len(snap.Recent) != 1 {
		t.Fatalf("Recent size = %d, want 1", len(snap.Recent))
	}
}

func TestRunQueriesRepeatMultipliesMeasuredRuns(t *testing.T) {
	ctx := context.Background()
	corpus := Corpus{mkDoc("1", "Rosa Barge", "Rosa is a French hotel barge on the Canal du Midi")}

	svc := fts.New(radix.New(), keygen.Word)
	if _, err := IndexCorpus(ctx, svc, corpus, SelectAbstract); err != nil {
		t.Fatalf("IndexCorpus() error = %v", err)
	}

	gt := &GroundTruth{Queries: []Query{{Query: "french hotel barge", RelevantTitles: []string{"Rosa Barge"}}}}
	stats := ftsstats.NewSearchStats(8)
	queryReports, err := RunQueries(ctx, svc, gt, corpus.TitleIndex(), 10, RunQueryOptions{Observer: stats, Repeat: 3})
	if err != nil {
		t.Fatalf("RunQueries() error = %v", err)
	}
	if len(queryReports) != 3 {
		t.Fatalf("len(queryReports) = %d, want 3", len(queryReports))
	}
	snap := stats.Snapshot()
	if snap.TotalSearches != 3 {
		t.Fatalf("TotalSearches = %d, want 3", snap.TotalSearches)
	}
}

func TestRunQueriesWarmupDoesNotAffectMeasuredReportsOrObserver(t *testing.T) {
	ctx := context.Background()
	corpus := Corpus{mkDoc("1", "Rosa Barge", "Rosa is a French hotel barge on the Canal du Midi")}

	svc := fts.New(radix.New(), keygen.Word)
	if _, err := IndexCorpus(ctx, svc, corpus, SelectAbstract); err != nil {
		t.Fatalf("IndexCorpus() error = %v", err)
	}

	gt := &GroundTruth{Queries: []Query{{Query: "french hotel barge", RelevantTitles: []string{"Rosa Barge"}}}}
	stats := ftsstats.NewSearchStats(8)
	queryReports, err := RunQueries(ctx, svc, gt, corpus.TitleIndex(), 10, RunQueryOptions{Observer: stats, Repeat: 2, Warmup: 5})
	if err != nil {
		t.Fatalf("RunQueries() error = %v", err)
	}
	if len(queryReports) != 2 {
		t.Fatalf("len(queryReports) = %d, want 2", len(queryReports))
	}
	snap := stats.Snapshot()
	if snap.TotalSearches != 2 {
		t.Fatalf("TotalSearches = %d, want 2", snap.TotalSearches)
	}
	if len(snap.Recent) != 2 {
		t.Fatalf("Recent size = %d, want 2", len(snap.Recent))
	}
}

func TestOrderedQueriesShufflePreservesMembersAndCanChangeOrder(t *testing.T) {
	in := []Query{{Query: "one"}, {Query: "two"}, {Query: "three"}, {Query: "four"}, {Query: "five"}}
	out := orderedQueries(in, true, rand.New(rand.NewSource(1)))
	if len(out) != len(in) {
		t.Fatalf("len(out) = %d, want %d", len(out), len(in))
	}
	seen := make(map[string]int, len(out))
	for _, q := range out {
		seen[q.Query]++
	}
	for _, q := range in {
		if seen[q.Query] != 1 {
			t.Fatalf("query %q count = %d, want 1", q.Query, seen[q.Query])
		}
	}
	sameOrder := true
	for i := range in {
		if in[i].Query != out[i].Query {
			sameOrder = false
			break
		}
	}
	if sameOrder {
		t.Fatal("expected shuffled order to differ from input order")
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

	report := Aggregate(10, IndexReport{}, queries, true)
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
