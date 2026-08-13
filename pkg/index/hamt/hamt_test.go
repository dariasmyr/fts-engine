package hamt

import (
	"context"
	"sort"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func insertOrd(t *testing.T, idx *Index, term string, id fts.DocID, ord fts.DocOrd) {
	t.Helper()
	if err := idx.Insert(term, ord); err != nil {
		t.Fatalf("Insert(%q, %q, %d) error = %v", term, id, ord, err)
	}
}

func insertAtOrd(t *testing.T, idx *Index, term string, id fts.DocID, pos uint32, ord fts.DocOrd) {
	t.Helper()
	if err := idx.InsertAt(term, pos, ord); err != nil {
		t.Fatalf("InsertAt(%q, %q, %d, %d) error = %v", term, id, pos, ord, err)
	}
}

func TestIndexInsertAndSearch(t *testing.T) {
	idx := New()

	insertOrd(t, idx, "hotel", "doc-1", 0)

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("len(docs) = %d, want 1", len(docs))
	}
	if docs[0].Ord != 0 {
		t.Fatalf("doc Ord = %d, want 0", docs[0].Ord)
	}
}

func TestIndexInsertSameDocIncrementsCount(t *testing.T) {
	idx := New()

	insertOrd(t, idx, "hotel", "doc-1", 0)
	insertOrd(t, idx, "hotel", "doc-1", 0)

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

	insertOrd(t, idx, "hotel", "doc-1", 0)
	insertOrd(t, idx, "hotel", "doc-2", 1)

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
}

func TestIndexInsertOutOfOrderKeepsPostingsSorted(t *testing.T) {
	idx := New()
	for _, ord := range []fts.DocOrd{1, 3, 7, 5, 3, 9} {
		insertOrd(t, idx, "hotel", "doc", ord)
	}

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	want := []fts.DocRef{
		{Ord: 1, Count: 1, Seq: 1},
		{Ord: 3, Count: 2, Seq: 3},
		{Ord: 5, Count: 1, Seq: 5},
		{Ord: 7, Count: 1, Seq: 7},
		{Ord: 9, Count: 1, Seq: 9},
	}
	if len(docs) != len(want) {
		t.Fatalf("docs = %+v, want %+v", docs, want)
	}
	for i := range want {
		if docs[i] != want[i] {
			t.Fatalf("docs[%d] = %+v, want %+v", i, docs[i], want[i])
		}
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

func TestIndexSearchPositional(t *testing.T) {
	idx := New()

	insertAtOrd(t, idx, "hotel", "doc-1", 1, 0)
	insertAtOrd(t, idx, "hotel", "doc-1", 3, 0)
	insertAtOrd(t, idx, "hotel", "doc-2", 2, 1)

	docs, err := idx.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}

	sort.Slice(docs, func(i, j int) bool { return docs[i].Ord < docs[j].Ord })
	if docs[0].Ord != 0 {
		t.Fatalf("docs[0].Ord = %d, want 0", docs[0].Ord)
	}
	if len(docs[0].Positions) != 2 || docs[0].Positions[0] != 1 || docs[0].Positions[1] != 3 {
		t.Fatalf("docs[0].Positions = %v, want [1 3]", docs[0].Positions)
	}
	if docs[1].Ord != 1 {
		t.Fatalf("docs[1].Ord = %d, want 1", docs[1].Ord)
	}
	if len(docs[1].Positions) != 1 || docs[1].Positions[0] != 2 {
		t.Fatalf("docs[1].Positions = %v, want [2]", docs[1].Positions)
	}
}

func TestIndexInsertAtOutOfOrderKeepsPositionsAligned(t *testing.T) {
	idx := New()
	for _, insert := range []struct {
		ord fts.DocOrd
		pos uint32
	}{
		{ord: 1, pos: 10},
		{ord: 1, pos: 11},
		{ord: 3, pos: 30},
		{ord: 7, pos: 70},
		{ord: 5, pos: 50},
		{ord: 3, pos: 31},
		{ord: 9, pos: 90},
	} {
		insertAtOrd(t, idx, "hotel", "doc", insert.pos, insert.ord)
	}

	docs, err := idx.SearchPositional("hotel")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	wantOrds := []fts.DocOrd{1, 3, 5, 7, 9}
	wantPositions := [][]uint32{{10, 11}, {30, 31}, {50}, {70}, {90}}
	if len(docs) != len(wantOrds) {
		t.Fatalf("docs = %+v, want ords %v", docs, wantOrds)
	}
	for i := range wantOrds {
		if docs[i].Ord != wantOrds[i] {
			t.Fatalf("docs[%d].Ord = %d, want %d", i, docs[i].Ord, wantOrds[i])
		}
		if len(docs[i].Positions) != len(wantPositions[i]) {
			t.Fatalf("docs[%d].Positions = %v, want %v", i, docs[i].Positions, wantPositions[i])
		}
		for j := range wantPositions[i] {
			if docs[i].Positions[j] != wantPositions[i][j] {
				t.Fatalf("docs[%d].Positions = %v, want %v", i, docs[i].Positions, wantPositions[i])
			}
		}
	}
}

func TestPhraseSearchWithHAMT(t *testing.T) {
	svc := fts.New(New(), fts.WordKeys)

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "hotel barge gave a speech",
		"doc-b": "barge speech today hotel was there",
		"doc-c": "hotel barge said hotel barge again",
	}
	for id, content := range docs {
		if err := svc.Index(ctx, fts.Document{ID: fts.DocID(id), Fields: map[string]fts.Field{fts.DefaultField: {Value: content}}}); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}

	res, err := svc.SearchPhrase(ctx, "hotel barge", 10)
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

	insertOrd(t, idx, "hotel", "doc-z", 0)
	insertOrd(t, idx, "hotel", "doc-a", 1)

	docs, err := idx.Search("hotel")
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
	if docs[0].Ord != 0 || docs[0].Seq != 0 {
		t.Fatalf("docs[0] = %+v, want ord=0 seq=0", docs[0])
	}
	if docs[1].Ord != 1 || docs[1].Seq != 1 {
		t.Fatalf("docs[1] = %+v, want ord=1 seq=1", docs[1])
	}
}
