package slicedradix

import (
	"bytes"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func insertSeqOrd(t *testing.T, idx *Index, term string, id fts.DocID, ord fts.DocOrd) {
	t.Helper()
	if err := idx.Insert(term, id, ord); err != nil {
		t.Fatalf("Insert(%q, %q, %d) error = %v", term, id, ord, err)
	}
}

func insertAtSeqOrd(t *testing.T, idx *Index, term string, id fts.DocID, pos uint32, ord fts.DocOrd) {
	t.Helper()
	if err := idx.InsertAt(term, id, pos, ord); err != nil {
		t.Fatalf("InsertAt(%q, %q, %d, %d) error = %v", term, id, pos, ord, err)
	}
}

func TestSeqAssignedOnFirstInsertion(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "x", "doc-a", 0)
	insertSeqOrd(t, idx, "x", "doc-b", 1)
	insertSeqOrd(t, idx, "x", "doc-c", 2)

	docs, err := idx.Search("x")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	want := []fts.DocRef{
		{Ord: 0, Count: 1, Seq: 0},
		{Ord: 1, Count: 1, Seq: 1},
		{Ord: 2, Count: 1, Seq: 2},
	}
	for i := range want {
		if docs[i] != want[i] {
			t.Fatalf("docs[%d] = %+v, want %+v", i, docs[i], want[i])
		}
	}
}

func TestSeqStableAcrossTerms(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "foo", "doc-a", 0)
	insertSeqOrd(t, idx, "bar", "doc-a", 0)
	insertSeqOrd(t, idx, "foo", "doc-b", 1)

	foo, err := idx.Search("foo")
	if err != nil {
		t.Fatalf("Search(foo) error = %v", err)
	}
	bar, err := idx.Search("bar")
	if err != nil {
		t.Fatalf("Search(bar) error = %v", err)
	}
	if foo[0].Seq != bar[0].Seq {
		t.Fatalf("doc-a Seq differs across terms: foo=%d bar=%d", foo[0].Seq, bar[0].Seq)
	}
	if foo[0].Seq != 0 || foo[1].Seq != 1 {
		t.Fatalf("foo seqs = %d,%d, want 0,1", foo[0].Seq, foo[1].Seq)
	}
}

func TestSeqUnchangedByTailCheck(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "hotel", "doc-a", 0)
	insertSeqOrd(t, idx, "hotel", "doc-a", 0)
	insertSeqOrd(t, idx, "hotel", "doc-a", 0)
	insertSeqOrd(t, idx, "hotel", "doc-b", 1)

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].Count != 3 || docs[0].Seq != 0 {
		t.Fatalf("docs[0] = %+v, want Count=3 Seq=0", docs[0])
	}
	if docs[1].Seq != 1 {
		t.Fatalf("docs[1].Seq = %d, want 1", docs[1].Seq)
	}
}

func TestSeqUnchangedByColdPathReindex(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "x", "doc-a", 0)
	insertSeqOrd(t, idx, "x", "doc-b", 1)
	insertSeqOrd(t, idx, "x", "doc-a", 0)

	docs, err := idx.Search("x")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].Ord != 0 || docs[0].Count != 2 || docs[0].Seq != 0 {
		t.Fatalf("docs[0] = %+v, want {ord:0 Count:2 Seq:0}", docs[0])
	}
	if docs[1].Seq != 1 {
		t.Fatalf("docs[1].Seq = %d, want 1", docs[1].Seq)
	}
}

func TestSeqMonotonicInPostings(t *testing.T) {
	idx := New()
	for i, id := range []fts.DocID{"a", "b", "c", "d", "e"} {
		insertSeqOrd(t, idx, "t", id, fts.DocOrd(i))
	}

	docs, err := idx.Search("t")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	var prev uint32
	for i, d := range docs {
		if i > 0 && d.Seq <= prev {
			t.Fatalf("postings not strictly Seq-sorted at %d: prev=%d current=%d", i, prev, d.Seq)
		}
		prev = d.Seq
	}
}

func TestSeqSurvivesSerializeLoad(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "foo", "doc-a", 0)
	insertSeqOrd(t, idx, "foo", "doc-b", 1)
	insertSeqOrd(t, idx, "bar", "doc-a", 0)

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
	if foo[0].Seq != 0 || foo[1].Seq != 1 {
		t.Fatalf("loaded foo seqs = %d,%d, want 0,1", foo[0].Seq, foo[1].Seq)
	}
	if bar[0].Seq != 0 {
		t.Fatalf("loaded bar[0].Seq = %d, want 0", bar[0].Seq)
	}

	if err := loadedIdx.Insert("foo", "doc-c", 2); err != nil {
		t.Fatalf("Insert() after Load() error = %v", err)
	}
	foo2, err := loadedIdx.Search("foo")
	if err != nil {
		t.Fatalf("Search(foo) after Load() error = %v", err)
	}
	if foo2[2].Seq != 2 {
		t.Fatalf("post-load Seq for doc-c = %d, want 2", foo2[2].Seq)
	}
}

func TestSearchPositionalReturnsSharedSlice(t *testing.T) {
	idx := New()
	insertAtSeqOrd(t, idx, "x", "doc-a", 0, 0)
	insertAtSeqOrd(t, idx, "x", "doc-a", 5, 0)

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
