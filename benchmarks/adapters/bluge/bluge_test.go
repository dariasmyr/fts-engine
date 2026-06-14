package bluge

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
)

func TestAdapterSupportsTypedQueries(t *testing.T) {
	a := New()
	ctx := context.Background()
	if err := a.Open(ctx, t.TempDir()); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer a.Close()

	docs := []harness.Document{
		{ID: "doc-1", Body: "distributed database microservice alpha beta"},
		{ID: "doc-2", Body: "distributed systems microkernel alpha"},
		{ID: "doc-3", Body: "microchip beta"},
	}
	if err := a.Index(ctx, docs); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := a.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	tests := []harness.Query{
		{ID: "phrase", Kind: harness.QueryKindPhrase, Text: "distributed database", K: 10},
		{ID: "prefix", Kind: harness.QueryKindPrefix, Text: "micr", K: 10},
		{ID: "boolean", Kind: harness.QueryKindBoolean, K: 10, Boolean: &harness.BoolQuery{Clauses: []harness.BoolClause{{Occur: harness.OccurMust, Atom: harness.Atom{Kind: harness.QueryKindTerm, Text: "alpha"}}, {Occur: harness.OccurMust, Atom: harness.Atom{Kind: harness.QueryKindTerm, Text: "beta"}}}}},
	}
	for _, q := range tests {
		hits, err := a.Search(ctx, q)
		if err != nil {
			t.Fatalf("Search(%s) error = %v", q.ID, err)
		}
		if len(hits) == 0 {
			t.Fatalf("Search(%s) returned no hits", q.ID)
		}
	}
}
