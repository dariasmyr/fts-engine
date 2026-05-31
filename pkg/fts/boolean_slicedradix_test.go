package fts_test

import (
	"context"
	"slices"
	"sort"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
)

func TestSearchBooleanMustNotExcludes(t *testing.T) {
	for _, tt := range booleanIndexCases() {
		t.Run(tt.name, func(t *testing.T) {
			svc := buildBooleanService(t, tt.newIndex(), true)
			q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
				fts.MustClause(fts.TermQuery{Term: "barack"}),
				fts.MustNotClause(fts.TermQuery{Term: "russia"}),
			}}

			res, err := svc.Search(context.Background(), q, 10)
			if err != nil {
				t.Fatalf("Search() error = %v", err)
			}
			got := sortedResultIDs(res)
			want := []string{"doc-a", "doc-b", "doc-d"}
			if !slices.Equal(got, want) {
				t.Fatalf("result IDs = %v, want %v", got, want)
			}
		})
	}
}

func TestSearchBooleanWandTopKMatchesFullResult(t *testing.T) {
	for _, tt := range booleanIndexCases() {
		t.Run(tt.name, func(t *testing.T) {
			svc := buildBooleanService(t, tt.newIndex(), true)
			q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
				fts.ShouldClause(fts.TermQuery{Term: "barack"}),
				fts.ShouldClause(fts.TermQuery{Term: "obama"}),
			}}

			full, err := svc.Search(context.Background(), q, 100)
			if err != nil {
				t.Fatalf("full Search() error = %v", err)
			}
			top, err := svc.Search(context.Background(), q, 2)
			if err != nil {
				t.Fatalf("top Search() error = %v", err)
			}
			if len(top.Results) != 2 {
				t.Fatalf("len(top.Results) = %d, want 2", len(top.Results))
			}

			wantScores := make(map[fts.DocID]float64, len(full.Results))
			for _, r := range full.Results {
				wantScores[r.ID] = r.Score
			}
			for _, r := range top.Results {
				if r.Score != wantScores[r.ID] {
					t.Fatalf("result %q score = %v, want %v", r.ID, r.Score, wantScores[r.ID])
				}
			}

			wantIDs := []string{string(full.Results[0].ID), string(full.Results[1].ID)}
			sort.Strings(wantIDs)
			gotIDs := sortedResultIDs(top)
			if !slices.Equal(gotIDs, wantIDs) {
				t.Fatalf("top IDs = %v, want %v", gotIDs, wantIDs)
			}
		})
	}
}

type booleanIndexCase struct {
	name     string
	newIndex func() fts.Index
}

func booleanIndexCases() []booleanIndexCase {
	return []booleanIndexCase{
		{name: "slicedradix", newIndex: func() fts.Index { return slicedradix.New() }},
		{name: "hamt", newIndex: func() fts.Index { return hamt.New() }},
	}
}

func buildBooleanService(t *testing.T, index fts.Index, scored bool) *fts.Service {
	t.Helper()
	var opts []fts.Option
	if scored {
		opts = append(opts, fts.WithScorer(fts.BM25()))
	}
	svc := fts.New(index, fts.WordKeys, opts...)

	ctx := context.Background()
	corpus := map[string]string{
		"doc-a": "barack obama gave a speech at inauguration",
		"doc-b": "banana split dessert is barack tasty",
		"doc-c": "russia is a country barack visited it",
		"doc-d": "mars rover exploration barack likes space",
		"doc-e": "the quick brown fox jumps over lazy dogs",
		"doc-f": "obama russia meeting in moscow photo op",
	}
	ids := make([]string, 0, len(corpus))
	for id := range corpus {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := svc.IndexDocument(ctx, fts.DocID(id), corpus[id]); err != nil {
			t.Fatalf("IndexDocument(%q) error = %v", id, err)
		}
	}
	return svc
}

func sortedResultIDs(res *fts.SearchResult) []string {
	out := make([]string, 0, len(res.Results))
	for _, r := range res.Results {
		out = append(out, string(r.ID))
	}
	sort.Strings(out)
	return out
}
