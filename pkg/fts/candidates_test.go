package fts

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func TestSearchTextCandidateOptions(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	idx.entries["alpha"] = refsForIDs(
		svc.registry,
		namedPosting{id: "a", count: 3},
		namedPosting{id: "b", count: 1},
		namedPosting{id: "c", count: 2},
	)
	idx.entries["beta"] = refsForIDs(
		svc.registry,
		namedPosting{id: "a", count: 1},
		namedPosting{id: "c", count: 5},
	)

	tests := []struct {
		name       string
		candidates []DocID
		want       []DocID
	}{
		{name: "unrestricted", candidates: nil, want: []DocID{"c", "a", "b"}},
		{name: "selected", candidates: []DocID{"c", "missing", "a", "a"}, want: []DocID{"c", "a"}},
		{name: "empty", candidates: []DocID{}, want: []DocID{}},
		{name: "unknown", candidates: []DocID{"missing"}, want: []DocID{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := svc.SearchText(
				context.Background(),
				"alpha beta",
				SearchOptions{CandidateIDs: tt.candidates, MaxResults: 10},
			)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]DocID, len(res.Results))
			for i, result := range res.Results {
				got[i] = result.ID
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("IDs = %v, want %v", got, tt.want)
			}
			if res.TotalResultsCount != len(tt.want) {
				t.Fatalf("TotalResultsCount = %d, want %d", res.TotalResultsCount, len(tt.want))
			}
		})
	}
}

func TestSearchOptionsCandidateAPIs(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys)
	ctx := context.Background()
	for _, id := range []DocID{"a", "b"} {
		if err := indexDefaultDoc(ctx, svc, id, "alpha"); err != nil {
			t.Fatal(err)
		}
	}
	options := SearchOptions{MaxResults: 10, CandidateIDs: []DocID{"b"}}

	typed, err := svc.SearchQuery(ctx, TermQuery{Term: "alpha"}, options)
	if err != nil {
		t.Fatal(err)
	}
	text, err := svc.SearchText(ctx, "alpha", options)
	if err != nil {
		t.Fatal(err)
	}
	for name, result := range map[string]*SearchResult{"typed": typed, "text": text} {
		if len(result.Results) != 1 || result.Results[0].ID != "b" {
			t.Fatalf("%s results = %+v, want only b", name, result.Results)
		}
	}
}

func TestSearchOptionsPreserveBooleanSemantics(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys)
	ctx := context.Background()
	for _, doc := range []struct {
		id      DocID
		content string
	}{
		{id: "a", content: "alpha beta"},
		{id: "b", content: "alpha"},
		{id: "c", content: "alpha beta gamma"},
		{id: "d", content: "beta"},
	} {
		if err := indexDefaultDoc(ctx, svc, doc.id, doc.content); err != nil {
			t.Fatal(err)
		}
	}

	query := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "alpha"}),
		MustClause(TermQuery{Term: "beta"}),
		MustNotClause(TermQuery{Term: "gamma"}),
	}}
	res, err := svc.SearchQuery(ctx, query, SearchOptions{
		CandidateIDs: []DocID{"d", "c", "a"},
		MaxResults:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "a" {
		t.Fatalf("results = %+v, want only a", res.Results)
	}

	orQuery := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "alpha"}),
		ShouldClause(TermQuery{Term: "beta"}),
	}}
	res, err = svc.SearchQuery(ctx, orQuery, SearchOptions{
		CandidateIDs: []DocID{"b", "d"},
		MaxResults:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 2 || res.Results[0].ID != "b" || res.Results[1].ID != "d" {
		t.Fatalf("OR results = %+v, want b and d", res.Results)
	}
}

func TestSearchOptionsRestrictWANDTopK(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys, WithScorer(TFIDF()))
	ctx := context.Background()
	for _, doc := range []struct {
		id      DocID
		content string
	}{
		{id: "a", content: "alpha alpha beta"},
		{id: "b", content: "alpha"},
		{id: "c", content: "beta"},
	} {
		if err := indexDefaultDoc(ctx, svc, doc.id, doc.content); err != nil {
			t.Fatal(err)
		}
	}
	query := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "alpha"}),
		ShouldClause(TermQuery{Term: "beta"}),
	}}

	result, err := svc.SearchQuery(ctx, query, SearchOptions{
		CandidateIDs: []DocID{"b", "c"},
		MaxResults:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 1 || result.Results[0].ID != "b" {
		t.Fatalf("candidate WAND results = %+v, want only b", result.Results)
	}
}

func TestSearchOptionsCandidateOracleMultiFieldAndNearPhrase(t *testing.T) {
	svc := NewMultiField(func(string) (MutableIndex, error) {
		return newOrdAwareMemoryIndex(), nil
	}, WordKeys, WithScorer(BM25()))
	ctx := context.Background()
	for _, doc := range []Document{
		{ID: "a", Fields: map[string]Field{"title": {Value: "alpha"}, "body": {Value: "james x doe"}}},
		{ID: "b", Fields: map[string]Field{"title": {Value: "alpha beta"}, "body": {Value: "james doe"}}},
		{ID: "c", Fields: map[string]Field{"title": {Value: "beta"}, "body": {Value: "james x y doe"}}},
	} {
		if err := svc.Index(ctx, doc); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		name       string
		query      Query
		candidates []DocID
		maxResults int
	}{
		{
			name: "multi_field_boolean",
			query: &BooleanQuery{Clauses: []BoolClause{
				ShouldClause(TermQuery{Field: "title", Term: "alpha"}),
				ShouldClause(TermQuery{Field: "body", Term: "doe"}),
			}},
			candidates: []DocID{"c", "a"},
			maxResults: 2,
		},
		{
			name:       "near_phrase",
			query:      NearPhraseQuery{Field: "body", Phrase: "james doe", Distance: 1},
			candidates: []DocID{"b", "a"},
			maxResults: 10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full, err := svc.SearchQuery(ctx, tc.query, SearchOptions{MaxResults: 0})
			if err != nil {
				t.Fatal(err)
			}
			allowed := make(map[DocID]bool, len(tc.candidates))
			for _, id := range tc.candidates {
				allowed[id] = true
			}
			want := make([]Result, 0, len(tc.candidates))
			for _, result := range full.Results {
				if allowed[result.ID] {
					want = append(want, result)
				}
			}
			if tc.maxResults > 0 && len(want) > tc.maxResults {
				want = want[:tc.maxResults]
			}

			filtered, err := svc.SearchQuery(ctx, tc.query, SearchOptions{
				CandidateIDs: tc.candidates,
				MaxResults:   tc.maxResults,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(filtered.Results, want) {
				t.Fatalf("filtered results = %+v, oracle = %+v", filtered.Results, want)
			}
		})
	}
}

func TestSearchOptionsPreserveScores(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys, WithScorer(BM25()))
	ctx := context.Background()
	for _, doc := range []struct {
		id      DocID
		content string
	}{
		{id: "a", content: "alpha alpha"},
		{id: "b", content: "alpha beta"},
		{id: "c", content: "alpha gamma delta"},
	} {
		if err := indexDefaultDoc(ctx, svc, doc.id, doc.content); err != nil {
			t.Fatal(err)
		}
	}

	full, err := svc.SearchQuery(ctx, TermQuery{Term: "alpha"}, SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := svc.SearchQuery(ctx, TermQuery{Term: "alpha"}, SearchOptions{
		CandidateIDs: []DocID{"a"},
		MaxResults:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.Results) != 1 || filtered.Results[0].ID != "a" {
		t.Fatalf("filtered results = %+v", filtered.Results)
	}
	var fullA Result
	for _, result := range full.Results {
		if result.ID == "a" {
			fullA = result
			break
		}
	}
	if filtered.Results[0] != fullA {
		t.Fatalf("filtered result = %+v, unrestricted result = %+v", filtered.Results[0], fullA)
	}
}

func TestSearchOptionsFilterPhraseAndPrefix(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys, WithScorer(BM25()))
	ctx := context.Background()
	for _, doc := range []struct {
		id      DocID
		content string
	}{
		{id: "a", content: "james doe barge"},
		{id: "b", content: "james doe barn"},
		{id: "c", content: "james smith bar"},
	} {
		if err := indexDefaultDoc(ctx, svc, doc.id, doc.content); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		name  string
		query Query
		want  DocID
	}{
		{name: "phrase", query: PhraseQuery{Phrase: "james doe"}, want: "b"},
		{name: "prefix", query: PrefixQuery{Prefix: "bar"}, want: "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full, err := svc.SearchQuery(ctx, tc.query, SearchOptions{MaxResults: 10})
			if err != nil {
				t.Fatal(err)
			}
			res, err := svc.SearchQuery(ctx, tc.query, SearchOptions{
				CandidateIDs: []DocID{"b"},
				MaxResults:   10,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Results) != 1 || res.Results[0].ID != tc.want {
				t.Fatalf("results = %+v, want only %s", res.Results, tc.want)
			}
			for _, result := range full.Results {
				if result.ID == tc.want && res.Results[0] != result {
					t.Fatalf("filtered result = %+v, unrestricted result = %+v", res.Results[0], result)
				}
			}
		})
	}
}

func TestSearchOptionsIgnoreDeletedIDs(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys)
	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "deleted", "alpha"); err != nil {
		t.Fatal(err)
	}
	if err := indexDefaultDoc(ctx, svc, "live", "alpha"); err != nil {
		t.Fatal(err)
	}
	if !svc.Delete("deleted") {
		t.Fatal("Delete(deleted) = false")
	}

	res, err := svc.SearchQuery(
		ctx,
		TermQuery{Term: "alpha"},
		SearchOptions{CandidateIDs: []DocID{"deleted", "live"}, MaxResults: 10},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "live" {
		t.Fatalf("results = %+v, want only live", res.Results)
	}
}

func TestEmptyCandidatesAvoidIndexSearch(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	idx.entries["alpha"] = refsForIDs(svc.registry, namedPosting{id: "a", count: 1})

	res, err := svc.SearchText(context.Background(), "alpha", SearchOptions{
		CandidateIDs: []DocID{},
		MaxResults:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 0 || len(idx.searches) != 0 {
		t.Fatalf("results = %+v, searches = %v", res.Results, idx.searches)
	}
}

func TestSearchOptionsHonorCancellation(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := svc.SearchQuery(ctx, TermQuery{Term: "alpha"}, SearchOptions{
		CandidateIDs: []DocID{"a"},
		MaxResults:   10,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
