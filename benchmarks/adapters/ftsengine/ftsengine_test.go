package ftsengine

import (
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
)

func TestSearchTreatsMSMARCOQueryAsPlainText(t *testing.T) {
	a := New(Config{})
	ctx := context.Background()

	if err := a.Open(ctx, t.TempDir()); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer a.Close()

	if err := a.Index(ctx, []harness.Document{{ID: "doc-1", Body: "anxiety definition symptoms treatment"}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := a.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	hits, err := a.Search(ctx, harness.Query{ID: "q1", Text: "anxiety: definition", K: 10})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("Search() returned no hits for plain-text query")
	}
}

func TestHAMTFirstSupportsBenchmarkModes(t *testing.T) {
	for _, persist := range []string{"none", "snapshot", "segment"} {
		t.Run(persist, func(t *testing.T) {
			a := New(Config{Index: hamtFirstIndex, Persist: persist})
			ctx := context.Background()
			if err := a.Open(ctx, t.TempDir()); err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer a.Close()

			if err := a.Index(ctx, []harness.Document{
				{ID: "doc-1", Body: "hotel barge"},
				{ID: "doc-2", Body: "hotel market"},
			}); err != nil {
				t.Fatalf("Index() error = %v", err)
			}
			if err := a.Commit(ctx); err != nil {
				t.Fatalf("Commit() error = %v", err)
			}

			for _, query := range []harness.Query{
				{Kind: harness.QueryKindTerm, Text: "hotel", K: 10},
				{Kind: harness.QueryKindPhrase, Text: "hotel barge", K: 10},
				{Kind: harness.QueryKindPrefix, Text: "hot", K: 10},
			} {
				hits, err := a.Search(ctx, query)
				if err != nil {
					t.Fatalf("Search(%s) error = %v", query.Kind, err)
				}
				if len(hits) == 0 {
					t.Fatalf("Search(%s) returned no hits", query.Kind)
				}
			}
		})
	}
}
