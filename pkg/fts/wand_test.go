package fts

import (
	"context"
	"sort"
	"testing"
)

func scoredResultsFromHits(hits map[DocID]docAccum) []Result {
	results := make([]Result, 0, len(hits))
	for id, h := range hits {
		results = append(results, Result{
			ID:            id,
			UniqueMatches: h.UniqueMatches,
			TotalMatches:  h.TotalMatches,
			Score:         h.Score,
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if results[i].UniqueMatches != results[j].UniqueMatches {
			return results[i].UniqueMatches > results[j].UniqueMatches
		}
		if results[i].TotalMatches != results[j].TotalMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].ID < results[j].ID
	})
	return results
}

func requireSameScoredResults(t *testing.T, got, want []Result) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("result length = %d, want %d\ngot=%+v\nwant=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].UniqueMatches != want[i].UniqueMatches || got[i].TotalMatches != want[i].TotalMatches || got[i].Score != want[i].Score {
			t.Fatalf("result[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func observeDefaultFieldLengths(svc *Service, lengths map[DocID]uint32) {
	for id, length := range lengths {
		svc.collection.observe(DefaultField, id, length)
	}
}

func TestTryExecBooleanOrWandMatchesFullScoringCandidateLimit(t *testing.T) {
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

	wandHits, ok, err := svc.tryExecBooleanOrWand(context.Background(), q, 2)
	if err != nil {
		t.Fatalf("tryExecBooleanOrWand() error = %v", err)
	}
	if !ok {
		t.Fatal("tryExecBooleanOrWand() did not activate")
	}

	fullHits, err := svc.executeQuery(context.Background(), q, 0)
	if err != nil {
		t.Fatalf("executeQuery() error = %v", err)
	}

	got := scoredResultsFromHits(wandHits)
	wantAll := scoredResultsFromHits(fullHits)
	requireSameScoredResults(t, got, wantAll[:2])
}

func TestTryExecBooleanOrWandPreservesTieBreakers(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "z", Count: 1, Seq: 1}, {ID: "a", Count: 1, Seq: 2}}

	svc := New(idx, WordKeys, WithScorer(TFIDF()))
	observeDefaultFieldLengths(svc, map[DocID]uint32{"a": 1, "z": 1})

	q := &BooleanQuery{Clauses: []BoolClause{ShouldClause(TermQuery{Term: "alpha"})}}

	wandHits, ok, err := svc.tryExecBooleanOrWand(context.Background(), q, 1)
	if err != nil {
		t.Fatalf("tryExecBooleanOrWand() error = %v", err)
	}
	if !ok {
		t.Fatal("tryExecBooleanOrWand() did not activate")
	}

	fullHits, err := svc.executeQuery(context.Background(), q, 0)
	if err != nil {
		t.Fatalf("executeQuery() error = %v", err)
	}

	got := scoredResultsFromHits(wandHits)
	want := scoredResultsFromHits(fullHits)[:1]
	requireSameScoredResults(t, got, want)
}

func TestTryExecBooleanOrWandSkipsWithoutCandidateLimit(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}}

	svc := New(idx, WordKeys, WithScorer(TFIDF()))
	observeDefaultFieldLengths(svc, map[DocID]uint32{"doc-a": 1})

	q := &BooleanQuery{Clauses: []BoolClause{ShouldClause(TermQuery{Term: "alpha"})}}
	_, ok, err := svc.tryExecBooleanOrWand(context.Background(), q, 0)
	if err != nil {
		t.Fatalf("tryExecBooleanOrWand() error = %v", err)
	}
	if ok {
		t.Fatal("tryExecBooleanOrWand() unexpectedly activated without candidateLimit")
	}
}

func TestTryExecBooleanOrWandSkipsWithoutScorer(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}}

	svc := New(idx, WordKeys)
	q := &BooleanQuery{Clauses: []BoolClause{ShouldClause(TermQuery{Term: "alpha"})}}
	_, ok, err := svc.tryExecBooleanOrWand(context.Background(), q, 1)
	if err != nil {
		t.Fatalf("tryExecBooleanOrWand() error = %v", err)
	}
	if ok {
		t.Fatal("tryExecBooleanOrWand() unexpectedly activated without scorer")
	}
}

func TestTryExecBooleanOrWandSkipsMultiExpansionClause(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}}
	idx.entries["alpha-alt"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}}

	keyGen := func(token string) ([]string, error) {
		return []string{token, token + "-alt"}, nil
	}
	svc := New(idx, keyGen, WithScorer(TFIDF()))
	observeDefaultFieldLengths(svc, map[DocID]uint32{"doc-a": 1})

	q := &BooleanQuery{Clauses: []BoolClause{ShouldClause(TermQuery{Term: "alpha"})}}
	_, ok, err := svc.tryExecBooleanOrWand(context.Background(), q, 1)
	if err != nil {
		t.Fatalf("tryExecBooleanOrWand() error = %v", err)
	}
	if ok {
		t.Fatal("tryExecBooleanOrWand() unexpectedly activated for multi-expansion clause")
	}
}

func TestTryExecBooleanOrWandSkipsCrossFieldPlan(t *testing.T) {
	title := newMemoryIndex()
	title.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}}
	body := newMemoryIndex()
	body.entries["beta"] = []DocRef{{ID: "doc-b", Count: 1, Seq: 1}}

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithScorer(TFIDF()))
	svc.collection.observe("title", "doc-a", 1)
	svc.collection.observe("body", "doc-b", 1)

	q := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Field: "title", Term: "alpha"}),
		ShouldClause(TermQuery{Field: "body", Term: "beta"}),
	}}
	_, ok, err := svc.tryExecBooleanOrWand(context.Background(), q, 2)
	if err != nil {
		t.Fatalf("tryExecBooleanOrWand() error = %v", err)
	}
	if ok {
		t.Fatal("tryExecBooleanOrWand() unexpectedly activated for cross-field plan")
	}
}

func TestTryExecBooleanOrWandSupportsMustNot(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 2, Seq: 1}, {ID: "doc-b", Count: 1, Seq: 2}, {ID: "doc-c", Count: 1, Seq: 3}}
	idx.entries["beta"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}, {ID: "doc-c", Count: 2, Seq: 3}}
	idx.entries["blocked"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 1}}

	svc := New(idx, WordKeys, WithScorer(TFIDF()))
	observeDefaultFieldLengths(svc, map[DocID]uint32{
		"doc-a": 3,
		"doc-b": 1,
		"doc-c": 3,
	})

	q := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "alpha"}),
		ShouldClause(TermQuery{Term: "beta"}),
		MustNotClause(TermQuery{Term: "blocked"}),
	}}

	wandHits, ok, err := svc.tryExecBooleanOrWand(context.Background(), q, 2)
	if err != nil {
		t.Fatalf("tryExecBooleanOrWand() error = %v", err)
	}
	if !ok {
		t.Fatal("tryExecBooleanOrWand() did not activate with MUST NOT")
	}

	fullHits, err := svc.executeQuery(context.Background(), q, 0)
	if err != nil {
		t.Fatalf("executeQuery() error = %v", err)
	}

	got := scoredResultsFromHits(wandHits)
	wantAll := scoredResultsFromHits(fullHits)
	requireSameScoredResults(t, got, wantAll[:2])
	for _, hit := range got {
		if hit.ID == "doc-a" {
			t.Fatalf("excluded document leaked into WAND results: %+v", got)
		}
	}
}
