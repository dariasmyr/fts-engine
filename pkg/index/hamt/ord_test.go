package hamt

import (
	"bytes"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

func TestOrdAssignedOnFirstInsertion(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "x", "doc-a", 0)
	insertOrd(t, idx, "x", "doc-b", 1)
	insertOrd(t, idx, "x", "doc-c", 2)

	docs, err := idx.Search("x")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	want := []fts.Posting{
		{Ord: 0, Count: 1},
		{Ord: 1, Count: 1},
		{Ord: 2, Count: 1},
	}
	for i := range want {
		if docs[i] != want[i] {
			t.Fatalf("docs[%d] = %+v, want %+v", i, docs[i], want[i])
		}
	}
}

func TestOrdStableAcrossTerms(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "foo", "doc-a", 0)
	insertOrd(t, idx, "bar", "doc-a", 0)
	insertOrd(t, idx, "foo", "doc-b", 1)

	foo, err := idx.Search("foo")
	if err != nil {
		t.Fatalf("Search(foo) error = %v", err)
	}
	bar, err := idx.Search("bar")
	if err != nil {
		t.Fatalf("Search(bar) error = %v", err)
	}
	if foo[0].Ord != bar[0].Ord {
		t.Fatalf("doc-a Ord differs across terms: foo=%d bar=%d", foo[0].Ord, bar[0].Ord)
	}
	if foo[0].Ord != 0 || foo[1].Ord != 1 {
		t.Fatalf("foo ords = %d,%d, want 0,1", foo[0].Ord, foo[1].Ord)
	}
}

func TestOrdUnchangedByTailCheck(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "hotel", "doc-a", 0)
	insertOrd(t, idx, "hotel", "doc-a", 0)
	insertOrd(t, idx, "hotel", "doc-a", 0)
	insertOrd(t, idx, "hotel", "doc-b", 1)

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].Count != 3 || docs[0].Ord != 0 {
		t.Fatalf("docs[0] = %+v, want Count=3 Ord=0", docs[0])
	}
	if docs[1].Ord != 1 {
		t.Fatalf("docs[1].Ord = %d, want 1", docs[1].Ord)
	}
}

func TestOrdUnchangedByColdPathReindex(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "x", "doc-a", 0)
	insertOrd(t, idx, "x", "doc-b", 1)
	insertOrd(t, idx, "x", "doc-a", 0)

	docs, err := idx.Search("x")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].Ord != 0 || docs[0].Count != 2 {
		t.Fatalf("docs[0] = %+v, want {Ord:0 Count:2}", docs[0])
	}
	if docs[1].Ord != 1 {
		t.Fatalf("docs[1].Ord = %d, want 1", docs[1].Ord)
	}
}

func TestOrdMonotonicInPostings(t *testing.T) {
	idx := New()
	for i, id := range []fts.DocID{"a", "b", "c", "d", "e"} {
		insertOrd(t, idx, "t", id, fts.DocOrd(i))
	}

	docs, err := idx.Search("t")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	var prev fts.DocOrd
	for i, d := range docs {
		if i > 0 && d.Ord <= prev {
			t.Fatalf("postings not strictly Ord-sorted at %d: prev=%d current=%d", i, prev, d.Ord)
		}
		prev = d.Ord
	}
}

func TestOrdSurvivesSerializeLoad(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "foo", "doc-a", 0)
	insertOrd(t, idx, "foo", "doc-b", 1)
	insertOrd(t, idx, "bar", "doc-a", 0)

	var buf bytes.Buffer
	if err := idx.Serialize(&buf); err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	loadedIdx := loaded.(*Index)

	foo, err := loadedIdx.Search("foo")
	if err != nil {
		t.Fatalf("loaded Search(foo) error = %v", err)
	}
	bar, err := loadedIdx.Search("bar")
	if err != nil {
		t.Fatalf("loaded Search(bar) error = %v", err)
	}
	if foo[0].Ord != 0 || foo[1].Ord != 1 {
		t.Fatalf("loaded foo ords = %d,%d, want 0,1", foo[0].Ord, foo[1].Ord)
	}
	if bar[0].Ord != 0 {
		t.Fatalf("loaded bar[0].Ord = %d, want 0", bar[0].Ord)
	}

	if err := loadedIdx.Insert("foo", 2); err != nil {
		t.Fatalf("Insert() after Load() error = %v", err)
	}
	foo2, err := loadedIdx.Search("foo")
	if err != nil {
		t.Fatalf("Search(foo) after Load() error = %v", err)
	}
	if foo2[2].Ord != 2 {
		t.Fatalf("post-load Ord for doc-c = %d, want 2", foo2[2].Ord)
	}
}

func TestSearchPositionalReturnsSharedSlice(t *testing.T) {
	idx := New()
	insertAtOrd(t, idx, "x", "doc-a", 0, 0)
	insertAtOrd(t, idx, "x", "doc-a", 5, 0)

	refs, err := idx.SearchPositional("x")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	if len(refs) != 1 || len(refs[0].Positions) != 2 {
		t.Fatalf("refs = %+v, want one result with two positions", refs)
	}

	first := refs[0].Positions
	again, err := idx.SearchPositional("x")
	if err != nil {
		t.Fatalf("second SearchPositional() error = %v", err)
	}
	second := again[0].Positions

	if &first[0] != &second[0] {
		t.Fatal("SearchPositional() copied positions instead of reusing shared backing storage")
	}
}
