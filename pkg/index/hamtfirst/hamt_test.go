package hamtfirst

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
)

func TestPostingListAddPaths(t *testing.T) {
	p := newPostingList(3)
	if p.Len() != 1 || p.first.Ord != 3 || p.rest != nil {
		t.Fatalf("newPostingList(3) = %+v", p)
	}

	for _, tc := range []struct {
		ord       fts.DocOrd
		wantIdx   int
		wantAdded bool
	}{
		{ord: 3, wantIdx: 0, wantAdded: false},
		{ord: 7, wantIdx: 1, wantAdded: true},
		{ord: 5, wantIdx: 1, wantAdded: true},
		{ord: 1, wantIdx: 0, wantAdded: true},
		{ord: 5, wantIdx: 2, wantAdded: false},
		{ord: 9, wantIdx: 4, wantAdded: true},
	} {
		idx, added := p.Add(tc.ord)
		if idx != tc.wantIdx || added != tc.wantAdded {
			t.Fatalf("Add(%d) = (%d, %v), want (%d, %v)", tc.ord, idx, added, tc.wantIdx, tc.wantAdded)
		}
	}

	want := []fts.Posting{
		{Ord: 1, Count: 1, Seq: 1},
		{Ord: 3, Count: 2, Seq: 3},
		{Ord: 5, Count: 2, Seq: 5},
		{Ord: 7, Count: 1, Seq: 7},
		{Ord: 9, Count: 1, Seq: 9},
	}
	if got := p.CloneSlice(); !reflect.DeepEqual(got, want) {
		t.Fatalf("CloneSlice() = %+v, want %+v", got, want)
	}
}

func TestIndexMatchesHAMT(t *testing.T) {
	baseline := hamt.New()
	candidate := New()
	rng := rand.New(rand.NewSource(42))
	terms := make([]string, 32)
	for i := range terms {
		terms[i] = "term-" + string(rune('a'+i))
	}

	for i := range 2000 {
		term := terms[rng.Intn(len(terms))]
		ord := fts.DocOrd(rng.Intn(80))
		var baselineErr, candidateErr error
		if rng.Intn(3) == 0 {
			baselineErr = baseline.Insert(term, ord)
			candidateErr = candidate.Insert(term, ord)
		} else {
			pos := uint32(i)
			baselineErr = baseline.InsertAt(term, pos, ord)
			candidateErr = candidate.InsertAt(term, pos, ord)
		}
		if baselineErr != nil || candidateErr != nil {
			t.Fatalf("insert %q ord=%d: baseline=%v candidate=%v", term, ord, baselineErr, candidateErr)
		}
	}

	for _, term := range terms {
		assertSearchEqual(t, baseline, candidate, term)
	}
	assertSearchEqual(t, baseline, candidate, "missing")

	for _, prefix := range []string{"term-a", "term-", "missing"} {
		want, err := baseline.SearchPrefix(prefix)
		if err != nil {
			t.Fatalf("baseline SearchPrefix(%q): %v", prefix, err)
		}
		got, err := candidate.SearchPrefix(prefix)
		if err != nil {
			t.Fatalf("candidate SearchPrefix(%q): %v", prefix, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("SearchPrefix(%q) = %+v, want %+v", prefix, got, want)
		}
	}
}

func TestPositionsStayAlignedAcrossMixedInsertions(t *testing.T) {
	idx := New()
	if err := idx.InsertAt("x", 70, 7); err != nil {
		t.Fatal(err)
	}
	if err := idx.InsertAt("x", 30, 3); err != nil {
		t.Fatal(err)
	}
	if err := idx.Insert("x", 5); err != nil {
		t.Fatal(err)
	}
	if err := idx.InsertAt("x", 10, 1); err != nil {
		t.Fatal(err)
	}
	if err := idx.InsertAt("x", 31, 3); err != nil {
		t.Fatal(err)
	}

	got, err := idx.SearchPositional("x")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	want := []fts.PositionalPosting{
		{Ord: 1, Positions: []uint32{10}},
		{Ord: 3, Positions: []uint32{30, 31}},
		{Ord: 5},
		{Ord: 7, Positions: []uint32{70}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchPositional() = %+v, want %+v", got, want)
	}
}

func TestSerializeLoadRoundTrip(t *testing.T) {
	idx := New()
	for _, insert := range []struct {
		ord fts.DocOrd
		pos uint32
	}{{7, 70}, {1, 10}, {5, 50}, {1, 11}} {
		if err := idx.InsertAt("x", insert.pos, insert.ord); err != nil {
			t.Fatal(err)
		}
	}

	var buf bytes.Buffer
	if err := idx.Serialize(&buf); err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	loaded, err := Load(&buf)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	loadedIdx := loaded.(*Index)
	if err := loadedIdx.InsertAt("x", 90, 9); err != nil {
		t.Fatalf("InsertAt() after Load error = %v", err)
	}

	want, _ := idx.SearchPositional("x")
	want = append(want, fts.PositionalPosting{Ord: 9, Positions: []uint32{90}})
	got, err := loadedIdx.SearchPositional("x")
	if err != nil {
		t.Fatalf("SearchPositional() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded postings = %+v, want %+v", got, want)
	}
}

func TestSearchReturnsCopyAndPositionalSearchSharesPositions(t *testing.T) {
	idx := New()
	if err := idx.InsertAt("x", 10, 1); err != nil {
		t.Fatal(err)
	}

	postings, _ := idx.Search("x")
	postings[0].Count = 99
	again, _ := idx.Search("x")
	if again[0].Count != 1 {
		t.Fatalf("Search() exposed mutable storage: %+v", again)
	}

	positions, _ := idx.SearchPositional("x")
	againPositions, _ := idx.SearchPositional("x")
	if &positions[0].Positions[0] != &againPositions[0].Positions[0] {
		t.Fatal("SearchPositional() copied position storage")
	}
}

func assertSearchEqual(t *testing.T, baseline *hamt.Index, candidate *Index, term string) {
	t.Helper()
	want, err := baseline.Search(term)
	if err != nil {
		t.Fatalf("baseline Search(%q): %v", term, err)
	}
	got, err := candidate.Search(term)
	if err != nil {
		t.Fatalf("candidate Search(%q): %v", term, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Search(%q) = %+v, want %+v", term, got, want)
	}

	wantPos, err := baseline.SearchPositional(term)
	if err != nil {
		t.Fatalf("baseline SearchPositional(%q): %v", term, err)
	}
	gotPos, err := candidate.SearchPositional(term)
	if err != nil {
		t.Fatalf("candidate SearchPositional(%q): %v", term, err)
	}
	if !reflect.DeepEqual(gotPos, wantPos) {
		t.Fatalf("SearchPositional(%q) = %+v, want %+v", term, gotPos, wantPos)
	}
}
