package segment_test

import (
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

func TestBuildFromSourceRoundTripSlicedRadix(t *testing.T) {
	idx := slicedradix.New()
	seedSegmentSource(t, idx)
	testBuildFromSourceRoundTrip(t, idx)
}

func TestBuildFromSourceRoundTripHamt(t *testing.T) {
	idx := hamt.New()
	seedSegmentSource(t, idx)
	testBuildFromSourceRoundTrip(t, idx)
}

func testBuildFromSourceRoundTrip(t *testing.T, source segment.Source) {
	t.Helper()

	data, err := segment.BuildFromSource(source)
	if err != nil {
		t.Fatalf("BuildFromSource() error = %v", err)
	}

	r, err := segment.Open(data)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	alpha, err := r.Search("alpha")
	if err != nil {
		t.Fatalf("Search(alpha) error = %v", err)
	}
	if len(alpha) != 2 || alpha[0].Ord != 1 || alpha[0].Count != 1 || alpha[1].Ord != 5 || alpha[1].Count != 2 {
		t.Fatalf("Search(alpha) = %+v, want ord/count [{1 1} {5 2}]", alpha)
	}

	betaPos, err := r.SearchPositional("beta")
	if err != nil {
		t.Fatalf("SearchPositional(beta) error = %v", err)
	}
	if len(betaPos) != 2 || betaPos[0].Ord != 1 || len(betaPos[0].Positions) != 1 || betaPos[0].Positions[0] != 4 {
		t.Fatalf("SearchPositional(beta) = %+v, want first ord=1 positions=[4]", betaPos)
	}
	if betaPos[1].Ord != 5 || len(betaPos[1].Positions) != 2 || betaPos[1].Positions[0] != 1 || betaPos[1].Positions[1] != 3 {
		t.Fatalf("SearchPositional(beta) = %+v, want second ord=5 positions=[1 3]", betaPos)
	}

	prefix, err := r.SearchPrefix("al")
	if err != nil {
		t.Fatalf("SearchPrefix(al) error = %v", err)
	}
	if len(prefix) != 2 || prefix[0].Ord != 1 || prefix[0].Count != 2 || prefix[1].Ord != 5 || prefix[1].Count != 2 {
		t.Fatalf("SearchPrefix(al) = %+v, want ord/count [{1 2} {5 2}]", prefix)
	}
}

func seedSegmentSource(t *testing.T, idx fts.PositionalIndex) {
	t.Helper()

	for _, tc := range []struct {
		term string
		ord  fts.DocOrd
		pos  uint32
	}{
		{term: "alpha", ord: 5},
		{term: "alpha", ord: 1},
		{term: "alpha", ord: 5},
		{term: "alpine", ord: 1},
		{term: "beta", ord: 5, pos: 1},
		{term: "beta", ord: 1, pos: 4},
		{term: "beta", ord: 5, pos: 3},
	} {
		var err error
		if tc.pos == 0 {
			err = idx.Insert(tc.term, "doc", tc.ord)
		} else {
			err = idx.InsertAt(tc.term, "doc", tc.pos, tc.ord)
		}
		if err != nil {
			t.Fatalf("insert %q ord=%d pos=%d: %v", tc.term, tc.ord, tc.pos, err)
		}
	}
}
