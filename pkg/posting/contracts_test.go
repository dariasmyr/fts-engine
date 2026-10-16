package posting_test

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
	"github.com/dariasmyr/fts-engine/v2/pkg/presence"
)

type contractExporter []posting.TermData

func (terms contractExporter) ExportPostings(yield func(posting.TermData) error) error {
	for _, term := range terms {
		if err := yield(term); err != nil {
			return err
		}
	}
	return nil
}

type memoryOpener map[string][]posting.Ord

func (s memoryOpener) OpenOrdinalPostings(_ context.Context, term string) (posting.OrdinalIterator, uint64, error) {
	ords := s[term]
	words := make([]uint64, 2)
	for _, ord := range ords {
		words[ord/64] |= uint64(1) << (ord % 64)
	}
	return posting.NewBitmapIterator(words), uint64(len(ords)), nil
}

func TestSourceContractAcrossFormats(t *testing.T) {
	terms := contractExporter{
		{Term: "alpha", Postings: []posting.Posting{{Ord: 0, Count: 2}, {Ord: 1, Count: 7}, {Ord: 2, Count: 3}}},
		{Term: "common", Postings: []posting.Posting{{Ord: 0, Count: 1}, {Ord: 1, Count: 2}, {Ord: 2, Count: 3}, {Ord: 9, Count: 1}}},
	}
	v3Data, err := ftse.Build([]posting.TermData(terms))
	if err != nil {
		t.Fatal(err)
	}
	v3, err := ftse.Open(v3Data)
	if err != nil {
		t.Fatal(err)
	}
	v2Data, err := os.ReadFile("../ftse/testdata/ftse-v2.golden")
	if err != nil {
		t.Fatal(err)
	}
	v2, err := ftse.Open(v2Data)
	if err != nil {
		t.Fatal(err)
	}
	fprData, err := presence.BuildFromSource(terms)
	if err != nil {
		t.Fatal(err)
	}
	fpr, err := presence.Open(fprData)
	if err != nil {
		t.Fatal(err)
	}

	sources := []struct {
		name   string
		opener posting.OrdinalPostingOpener
	}{
		{name: "memory", opener: memoryOpener{"alpha": {0, 1, 2}, "common": {0, 1, 2, 9}}},
		{name: "ftse_v2", opener: v2},
		{name: "ftse_v3", opener: v3},
		{name: "fpr1", opener: fpr},
	}
	for _, test := range sources {
		t.Run(test.name, func(t *testing.T) {
			assertIntersection(t, test.opener, nil, []posting.Ord{0, 1, 2})
			assertIntersection(t, test.opener, []posting.Ord{1, 9}, []posting.Ord{1})
			cursor, count, err := test.opener.OpenOrdinalPostings(context.Background(), "common")
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			if count < 4 || !cursor.Advance(2) || cursor.Ord() != 2 || !cursor.Next() || cursor.Ord() <= 2 {
				t.Fatalf("iterator count=%d ord=%d err=%v", count, cursor.Ord(), cursor.Err())
			}
		})
	}
}

func assertIntersection(t *testing.T, opener posting.OrdinalPostingOpener, candidates, want []posting.Ord) {
	t.Helper()
	var got []posting.Ord
	err := posting.Intersect(context.Background(), opener, []string{"alpha", "common"}, candidates, func(ord posting.Ord) bool {
		got = append(got, ord)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Intersect() = %v, want %v", got, want)
	}
}
