package radix

import (
	"context"
	"sort"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func TestIndexInsertAndSearch(t *testing.T) {
	idx := New()

	if err := idx.Insert("hotel", "doc-1"); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].ID != "doc-1" {
		t.Fatalf("doc ID = %q, want %q", docs[0].ID, "doc-1")
	}
}

func TestIndexInsertSameDocIncrementsCount(t *testing.T) {
	idx := New()

	_ = idx.Insert("hotel", "doc-1")
	_ = idx.Insert("hotel", "doc-1")

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].Count != 2 {
		t.Fatalf("doc Count = %d, want 2", docs[0].Count)
	}
}

func TestIndexInsertDifferentDocs(t *testing.T) {
	idx := New()

	_ = idx.Insert("hotel", "doc-1")
	_ = idx.Insert("hotel", "doc-2")

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
}

func TestIndexSearchNotFound(t *testing.T) {
	idx := New()

	docs, err := idx.Search("unknown")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 0 {
		t.Fatalf("len(docs) = %d, want 0", len(docs))
	}
}

func TestIndexAnalyze(t *testing.T) {
	idx := New()
	_ = idx.Insert("hotel", "doc-1")

	stats := idx.Analyze()
	if stats.Nodes == 0 {
		t.Fatalf("stats.Nodes = %d, want > 0", stats.Nodes)
	}
}

func TestIndexSearchPositional(t *testing.T) {
	idx := New()

	_ = idx.InsertAt("hotel", "doc-1", 1)
	_ = idx.InsertAt("hotel", "doc-1", 3)
	_ = idx.InsertAt("hotel", "doc-2", 2)

	docs, err := idx.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}

	sort.Slice(docs, func(i, j int) bool { return docs[i].ID < docs[j].ID })
	if docs[0].ID != "doc-1" {
		t.Fatalf("docs[0].ID = %q, want %q", docs[0].ID, "doc-1")
	}
	if len(docs[0].Positions) != 2 || docs[0].Positions[0] != 1 || docs[0].Positions[1] != 3 {
		t.Fatalf("docs[0].Positions = %v, want [1 3]", docs[0].Positions)
	}
	if docs[1].ID != "doc-2" {
		t.Fatalf("docs[1].ID = %q, want %q", docs[1].ID, "doc-2")
	}
	if len(docs[1].Positions) != 1 || docs[1].Positions[0] != 2 {
		t.Fatalf("docs[1].Positions = %v, want [2]", docs[1].Positions)
	}
}

func TestPhraseSearchWithRadix(t *testing.T) {
	svc := fts.New(New(), fts.WordKeys)

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "barack obama gave a speech",
		"doc-b": "obama speech today barack was there",
		"doc-c": "barack obama said barack obama again",
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, fts.DocID(id), content); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}

	res, err := svc.SearchPhrase(ctx, "barack obama", 10)
	if err != nil {
		t.Fatalf("SearchPhrase() error = %v", err)
	}

	hits := map[fts.DocID]int{}
	for _, r := range res.Results {
		hits[r.ID] = r.TotalMatches
	}

	if _, ok := hits["doc-a"]; !ok {
		t.Fatalf("expected doc-a to match, got %+v", res.Results)
	}
	if _, ok := hits["doc-b"]; ok {
		t.Fatalf("doc-b should not match, got %+v", res.Results)
	}
	if got := hits["doc-c"]; got != 2 {
		t.Fatalf("doc-c TotalMatches = %d, want 2", got)
	}
}

func TestIndexSearchReturnsSeqOrder(t *testing.T) {
	idx := New()

	_ = idx.Insert("hotel", "doc-z")
	_ = idx.Insert("hotel", "doc-a")

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].ID != "doc-z" || docs[0].Seq != 0 {
		t.Fatalf("docs[0] = %+v, want doc-z seq=0", docs[0])
	}
	if docs[1].ID != "doc-a" || docs[1].Seq != 1 {
		t.Fatalf("docs[1] = %+v, want doc-a seq=1", docs[1])
	}
}
