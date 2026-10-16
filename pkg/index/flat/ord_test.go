package flat

import (
	"bytes"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

func insertOrd(t *testing.T, idx *Index, term string, ord fts.DocOrd) {
	t.Helper()
	if err := idx.Insert(term, ord); err != nil {
		t.Fatalf("Insert(%q, %d) error = %v", term, ord, err)
	}
}

func insertAtOrd(t *testing.T, idx *Index, term string, pos uint32, ord fts.DocOrd) {
	t.Helper()
	if err := idx.InsertAt(term, pos, ord); err != nil {
		t.Fatalf("InsertAt(%q, %d, %d) error = %v", term, pos, ord, err)
	}
}

func TestOrdAssignedOnFirstInsertion(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "x", 0)
	insertOrd(t, idx, "x", 1)
	insertOrd(t, idx, "x", 2)

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
	insertOrd(t, idx, "foo", 0)
	insertOrd(t, idx, "bar", 0)
	insertOrd(t, idx, "foo", 1)

	foo, err := idx.Search("foo")
	if err != nil {
		t.Fatalf("Search(foo) error = %v", err)
	}
	bar, err := idx.Search("bar")
	if err != nil {
		t.Fatalf("Search(bar) error = %v", err)
	}
	if foo[0].Ord != bar[0].Ord || foo[0].Ord != 0 || foo[1].Ord != 1 {
		t.Fatalf("seqs: foo=%+v bar=%+v, want stable ord-derived sequence", foo, bar)
	}
}

func TestOrdUnchangedByTailCheck(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "hotel", 0)
	insertOrd(t, idx, "hotel", 0)
	insertOrd(t, idx, "hotel", 0)
	insertOrd(t, idx, "hotel", 1)

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 || docs[0].Count != 3 || docs[0].Ord != 0 || docs[1].Ord != 1 {
		t.Fatalf("Search() = %+v, want stable ords and first Count=3", docs)
	}
}

func TestOrdUnchangedByColdPathReindex(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "x", 4)
	insertOrd(t, idx, "x", 9)
	insertOrd(t, idx, "x", 1)
	insertOrd(t, idx, "x", 4)

	docs, err := idx.Search("x")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 3 || docs[0].Ord != 1 || docs[1].Ord != 4 || docs[1].Count != 2 || docs[2].Ord != 9 {
		t.Fatalf("Search() = %+v, want ord-sorted postings with ord 4 counted twice", docs)
	}
}

func TestOrdMonotonicInPostings(t *testing.T) {
	idx := New()
	for _, ord := range []fts.DocOrd{8, 3, 5, 1, 6} {
		insertOrd(t, idx, "t", ord)
	}
	docs, err := idx.Search("t")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	for i := 1; i < len(docs); i++ {
		if docs[i].Ord <= docs[i-1].Ord {
			t.Fatalf("postings not strictly Ord-sorted at %d: %+v", i, docs)
		}
	}
}

func TestOrdSurvivesSerializeLoad(t *testing.T) {
	idx := New()
	insertOrd(t, idx, "foo", 0)
	insertOrd(t, idx, "foo", 1)
	insertOrd(t, idx, "bar", 0)

	var buf bytes.Buffer
	if err := idx.Serialize(&buf); err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	loadedIdx := loaded.(*Index)
	if err := loadedIdx.Insert("foo", 2); err != nil {
		t.Fatalf("Insert() after Load() error = %v", err)
	}
	foo, err := loadedIdx.Search("foo")
	if err != nil {
		t.Fatalf("Search(foo) error = %v", err)
	}
	if len(foo) != 3 || foo[0].Ord != 0 || foo[1].Ord != 1 || foo[2].Ord != 2 {
		t.Fatalf("loaded foo = %+v, want ords [0 1 2]", foo)
	}
}

func TestSearchPositionalReturnsSharedSlice(t *testing.T) {
	idx := New()
	insertAtOrd(t, idx, "x", 0, 0)
	insertAtOrd(t, idx, "x", 5, 0)

	refs, err := idx.SearchPositional("x")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	again, err := idx.SearchPositional("x")
	if err != nil {
		t.Fatalf("second SearchPositional() error = %v", err)
	}
	if len(refs) != 1 || len(refs[0].Positions) != 2 || &refs[0].Positions[0] != &again[0].Positions[0] {
		t.Fatal("SearchPositional() did not reuse read-only position backing storage")
	}
}
