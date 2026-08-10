package flat

import (
	"bytes"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func insertSeqOrd(t *testing.T, idx *Index, term string, ord fts.DocOrd) {
	t.Helper()
	if err := idx.Insert(term, ord); err != nil {
		t.Fatalf("Insert(%q, %d) error = %v", term, ord, err)
	}
}

func insertAtSeqOrd(t *testing.T, idx *Index, term string, pos uint32, ord fts.DocOrd) {
	t.Helper()
	if err := idx.InsertAt(term, pos, ord); err != nil {
		t.Fatalf("InsertAt(%q, %d, %d) error = %v", term, pos, ord, err)
	}
}

func TestSeqAssignedOnFirstInsertion(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "x", 0)
	insertSeqOrd(t, idx, "x", 1)
	insertSeqOrd(t, idx, "x", 2)

	docs, err := idx.Search("x")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	want := []fts.Posting{
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
	insertSeqOrd(t, idx, "foo", 0)
	insertSeqOrd(t, idx, "bar", 0)
	insertSeqOrd(t, idx, "foo", 1)

	foo, err := idx.Search("foo")
	if err != nil {
		t.Fatalf("Search(foo) error = %v", err)
	}
	bar, err := idx.Search("bar")
	if err != nil {
		t.Fatalf("Search(bar) error = %v", err)
	}
	if foo[0].Seq != bar[0].Seq || foo[0].Seq != 0 || foo[1].Seq != 1 {
		t.Fatalf("seqs: foo=%+v bar=%+v, want stable ord-derived sequence", foo, bar)
	}
}

func TestSeqUnchangedByTailCheck(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "hotel", 0)
	insertSeqOrd(t, idx, "hotel", 0)
	insertSeqOrd(t, idx, "hotel", 0)
	insertSeqOrd(t, idx, "hotel", 1)

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 || docs[0].Count != 3 || docs[0].Seq != 0 || docs[1].Seq != 1 {
		t.Fatalf("Search() = %+v, want stable seqs and first Count=3", docs)
	}
}

func TestSeqUnchangedByColdPathReindex(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "x", 4)
	insertSeqOrd(t, idx, "x", 9)
	insertSeqOrd(t, idx, "x", 1)
	insertSeqOrd(t, idx, "x", 4)

	docs, err := idx.Search("x")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 3 || docs[0].Ord != 1 || docs[1].Ord != 4 || docs[1].Count != 2 || docs[2].Ord != 9 {
		t.Fatalf("Search() = %+v, want ord-sorted postings with ord 4 counted twice", docs)
	}
	for _, doc := range docs {
		if doc.Seq != uint32(doc.Ord) {
			t.Fatalf("posting = %+v, want Seq derived from Ord", doc)
		}
	}
}

func TestSeqMonotonicInPostings(t *testing.T) {
	idx := New()
	for _, ord := range []fts.DocOrd{8, 3, 5, 1, 6} {
		insertSeqOrd(t, idx, "t", ord)
	}
	docs, err := idx.Search("t")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	for i := 1; i < len(docs); i++ {
		if docs[i].Seq <= docs[i-1].Seq {
			t.Fatalf("postings not strictly Seq-sorted at %d: %+v", i, docs)
		}
	}
}

func TestSeqSurvivesSerializeLoad(t *testing.T) {
	idx := New()
	insertSeqOrd(t, idx, "foo", 0)
	insertSeqOrd(t, idx, "foo", 1)
	insertSeqOrd(t, idx, "bar", 0)

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
	if len(foo) != 3 || foo[0].Seq != 0 || foo[1].Seq != 1 || foo[2].Seq != 2 {
		t.Fatalf("loaded foo = %+v, want seqs [0 1 2]", foo)
	}
}

func TestSearchPositionalReturnsSharedSlice(t *testing.T) {
	idx := New()
	insertAtSeqOrd(t, idx, "x", 0, 0)
	insertAtSeqOrd(t, idx, "x", 5, 0)

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
