package fts

import (
	"context"
	"testing"
	"time"
)

func requireDiagnostics(t *testing.T, res *SearchResult) *QueryDiagnostics {
	t.Helper()
	if res == nil {
		t.Fatal("expected non-nil SearchResult")
	}
	if res.Diagnostics == nil {
		t.Fatal("expected non-nil Diagnostics")
	}
	if res.Diagnostics.Timings["total"] <= 0 {
		t.Fatalf("expected Diagnostics.Timings[total] > 0, got %v", res.Diagnostics.Timings["total"])
	}
	return res.Diagnostics
}

func TestSearchDiagnosticsTermStrategy(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.Search(context.Background(), TermQuery{Term: "barack"}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType != "term" {
		t.Fatalf("LogicalQueryType = %q, want term", d.LogicalQueryType)
	}
	if d.ExecutionStrategy != "term" {
		t.Fatalf("ExecutionStrategy = %q, want term", d.ExecutionStrategy)
	}
	if d.ReturnedDocs != len(res.Results) {
		t.Fatalf("ReturnedDocs = %d, want %d", d.ReturnedDocs, len(res.Results))
	}
	if d.MatchedDocs != res.TotalResultsCount {
		t.Fatalf("MatchedDocs = %d, want %d", d.MatchedDocs, res.TotalResultsCount)
	}
	if d.ProcessedTokens <= 0 || d.IndexSearches <= 0 || d.PostingEntriesRead <= 0 {
		t.Fatalf("expected positive token/lookups/postings metrics, got %+v", *d)
	}
	if d.Timings["search_tokens"] <= 0 || d.Timings["total"] < d.Timings["search_tokens"] {
		t.Fatalf("unexpected timings: %+v", d.Timings)
	}
	if d.Timings["total"] > 10*time.Second {
		t.Fatalf("unexpectedly large timing: %+v", d.Timings)
	}
}

func TestSearchDiagnosticsPrefixStrategy(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.Search(context.Background(), PrefixQuery{Field: "title", Prefix: "ba"}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType != "prefix" {
		t.Fatalf("LogicalQueryType = %q, want prefix", d.LogicalQueryType)
	}
	if d.ExecutionStrategy != "prefix" {
		t.Fatalf("ExecutionStrategy = %q, want prefix", d.ExecutionStrategy)
	}
	if d.FieldsVisited <= 0 || d.IndexSearches <= 0 || d.PostingEntriesRead <= 0 {
		t.Fatalf("expected positive fields/lookups/postings metrics, got %+v", *d)
	}
}

func TestSearchDiagnosticsBooleanOrWandStrategy(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 2, Seq: 1}, {ID: "doc-b", Count: 1, Seq: 2}, {ID: "doc-c", Count: 1, Seq: 3}, {ID: "doc-e", Count: 1, Seq: 5}}
	idx.entries["beta"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}, {ID: "doc-c", Count: 2, Seq: 3}, {ID: "doc-d", Count: 1, Seq: 4}}
	idx.entries["gamma"] = []DocRef{{ID: "doc-c", Count: 1, Seq: 3}, {ID: "doc-d", Count: 3, Seq: 4}, {ID: "doc-e", Count: 1, Seq: 5}}

	svc := New(idx, WordKeys, WithScorer(TFIDF()))
	observeDefaultFieldLengths(svc, map[DocID]uint32{
		"doc-a": 3,
		"doc-b": 1,
		"doc-c": 4,
		"doc-d": 4,
		"doc-e": 2,
	})

	q := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "alpha"}),
		ShouldClause(TermQuery{Term: "beta"}),
		ShouldClause(TermQuery{Term: "gamma"}),
	}}

	res, err := svc.Search(context.Background(), q, 2)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType != "boolean" {
		t.Fatalf("LogicalQueryType = %q, want boolean", d.LogicalQueryType)
	}
	if d.ExecutionStrategy != "bool_or_wand" {
		t.Fatalf("ExecutionStrategy = %q, want bool_or_wand", d.ExecutionStrategy)
	}
	if d.ReturnedDocs != 2 {
		t.Fatalf("ReturnedDocs = %d, want 2", d.ReturnedDocs)
	}
}

func TestSearchDiagnosticsPhraseStrategy(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.Search(context.Background(), PhraseQuery{Phrase: "barack obama"}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType != "phrase" {
		t.Fatalf("LogicalQueryType = %q, want phrase", d.LogicalQueryType)
	}
	if d.ExecutionStrategy != "phrase_exact" {
		t.Fatalf("ExecutionStrategy = %q, want phrase_exact", d.ExecutionStrategy)
	}
	if d.ProcessedTokens <= 0 || d.PostingEntriesRead <= 0 {
		t.Fatalf("expected positive phrase diagnostics, got %+v", *d)
	}
}

func TestSearchDiagnosticsBooleanFallbackStrategy(t *testing.T) {
	svc := buildExecService(t)
	svc.scorer = TFIDF()
	q := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(PhraseQuery{Phrase: "barack obama"}),
		ShouldClause(TermQuery{Term: "banana"}),
	}}

	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType != "boolean" {
		t.Fatalf("LogicalQueryType = %q, want boolean", d.LogicalQueryType)
	}
	if d.ExecutionStrategy != "bool_fallback" {
		t.Fatalf("ExecutionStrategy = %q, want bool_fallback", d.ExecutionStrategy)
	}
	if d.StrategyReason != "wand_not_or_terms_only" {
		t.Fatalf("StrategyReason = %q, want wand_not_or_terms_only", d.StrategyReason)
	}
}

func TestSearchDiagnosticsBooleanAndFastStrategy(t *testing.T) {
	svc := buildExecService(t)
	q := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Field: "body", Term: "barack"}),
		MustClause(TermQuery{Field: "body", Term: "mars"}),
	}}

	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType != "boolean" {
		t.Fatalf("LogicalQueryType = %q, want boolean", d.LogicalQueryType)
	}
	if d.ExecutionStrategy != "bool_and_fast_sort_merge" {
		t.Fatalf("ExecutionStrategy = %q, want bool_and_fast_sort_merge", d.ExecutionStrategy)
	}
}

func TestSearchDiagnosticsWandSkipReasonWithoutScorer(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}}
	idx.entries["beta"] = []DocRef{{ID: "doc-b", Count: 1, Seq: 2}}

	svc := New(idx, WordKeys)
	q := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "alpha"}),
		ShouldClause(TermQuery{Term: "beta"}),
	}}

	res, err := svc.Search(context.Background(), q, 1)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType != "boolean" {
		t.Fatalf("LogicalQueryType = %q, want boolean", d.LogicalQueryType)
	}
	if d.ExecutionStrategy != "bool_or_fast" {
		t.Fatalf("ExecutionStrategy = %q, want bool_or_fast", d.ExecutionStrategy)
	}
	if d.StrategyReason != "wand_disabled_no_scorer" {
		t.Fatalf("StrategyReason = %q, want wand_disabled_no_scorer", d.StrategyReason)
	}
}

func TestSearchDocumentsDiagnosticsNonNil(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.SearchDocuments(context.Background(), "barack", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType != "term" {
		t.Fatalf("LogicalQueryType = %q, want term", d.LogicalQueryType)
	}
	if d.Timings["preprocess"] <= 0 {
		t.Fatalf("expected preprocess timing > 0, got %+v", d.Timings)
	}
}

func TestSearchFieldClausesDiagnosticsNonNil(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.SearchFieldClauses(context.Background(), []FieldQueryClause{
		MustFieldQuery("title", "barack"),
		MustNotFieldQuery("body", "russia"),
	}, 10)
	if err != nil {
		t.Fatalf("SearchFieldClauses() error = %v", err)
	}
	d := requireDiagnostics(t, res)
	if d.LogicalQueryType == "" || d.ExecutionStrategy == "" {
		t.Fatalf("expected non-empty diagnostics labels, got %+v", *d)
	}
	if d.Timings["preprocess"] <= 0 {
		t.Fatalf("expected preprocess timing > 0, got %+v", d.Timings)
	}
}

func TestSearchPhrasePublicMethodsDiagnosticsNonNil(t *testing.T) {
	svc := buildExecService(t)
	ctx := context.Background()

	cases := []struct {
		name string
		run  func() (*SearchResult, error)
	}{
		{name: "SearchPhrase", run: func() (*SearchResult, error) {
			return svc.SearchPhrase(ctx, "barack obama", 10)
		}},
		{name: "SearchPhraseField", run: func() (*SearchResult, error) {
			return svc.SearchPhraseField(ctx, "title", "barack obama", 10)
		}},
		{name: "SearchPhraseFields", run: func() (*SearchResult, error) {
			return svc.SearchPhraseFields(ctx, []string{"title", "body"}, "barack obama", 10)
		}},
	}

	for _, tc := range cases {
		res, err := tc.run()
		if err != nil {
			t.Fatalf("%s() error = %v", tc.name, err)
		}
		d := requireDiagnostics(t, res)
		if d.LogicalQueryType != "phrase" || d.ExecutionStrategy != "phrase_exact" {
			t.Fatalf("%s diagnostics = %+v, want phrase/phrase_exact", tc.name, *d)
		}
	}
}

func TestSearchPhraseNearPublicMethodsDiagnosticsNonNil(t *testing.T) {
	svc := buildExecService(t)
	ctx := context.Background()

	cases := []struct {
		name string
		run  func() (*SearchResult, error)
	}{
		{name: "SearchPhraseNear", run: func() (*SearchResult, error) {
			return svc.SearchPhraseNear(ctx, "barack obama", 1, 10)
		}},
		{name: "SearchPhraseNearField", run: func() (*SearchResult, error) {
			return svc.SearchPhraseNearField(ctx, "title", "barack obama", 1, 10)
		}},
		{name: "SearchPhraseNearFields", run: func() (*SearchResult, error) {
			return svc.SearchPhraseNearFields(ctx, []string{"title", "body"}, "barack obama", 1, 10)
		}},
	}

	for _, tc := range cases {
		res, err := tc.run()
		if err != nil {
			t.Fatalf("%s() error = %v", tc.name, err)
		}
		d := requireDiagnostics(t, res)
		if d.LogicalQueryType != "phrase_near" || d.ExecutionStrategy != "phrase_near" {
			t.Fatalf("%s diagnostics = %+v, want phrase_near/phrase_near", tc.name, *d)
		}
	}
}
