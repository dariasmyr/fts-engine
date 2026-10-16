package fts

import (
	"context"
	"slices"
	"sort"
	"testing"
)

func TestGallopPostingLowerBound(t *testing.T) {
	postings := []Posting{{Ord: 2}, {Ord: 5}, {Ord: 9}, {Ord: 14}, {Ord: 20}, {Ord: 27}}
	tests := []struct {
		name   string
		from   int
		target DocOrd
		want   int
	}{
		{name: "before first", target: 1, want: 0},
		{name: "first", target: 2, want: 0},
		{name: "next", from: 2, target: 14, want: 3},
		{name: "between", target: 10, want: 3},
		{name: "exact after offset", from: 2, target: 20, want: 4},
		{name: "past end", target: 30, want: len(postings)},
		{name: "offset at end", from: len(postings), target: 30, want: len(postings)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := gallopPostingLowerBound(postings, tt.from, tt.target); got != tt.want {
				t.Fatalf("gallopPostingLowerBound() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGallopPostingLowerBoundExhaustive(t *testing.T) {
	postings := []Posting{{Ord: 2}, {Ord: 5}, {Ord: 9}, {Ord: 14}, {Ord: 20}, {Ord: 27}}
	for from := 0; from <= len(postings); from++ {
		for target := DocOrd(0); target <= 30; target++ {
			want := from + sort.Search(len(postings)-from, func(i int) bool {
				return postings[from+i].Ord >= target
			})
			if got := gallopPostingLowerBound(postings, from, target); got != want {
				t.Fatalf("gallopPostingLowerBound(from=%d, target=%d) = %d, want %d", from, target, got, want)
			}
		}
	}
}

func TestExecBooleanAndSortMergeRanges(t *testing.T) {
	tests := []struct {
		name  string
		musts []fastMust
		want  []DocOrd
	}{
		{
			name: "disjoint ranges",
			musts: []fastMust{
				fastMustForOrds(10, 11),
				fastMustForOrds(1, 2, 3, 4),
			},
			want: []DocOrd{},
		},
		{
			name: "overlap starts after shortest first ordinal",
			musts: []fastMust{
				fastMustForOrds(1, 5, 9),
				fastMustForOrds(5, 7, 9, 11),
			},
			want: []DocOrd{5, 9},
		},
		{
			name: "ranges touch at boundary",
			musts: []fastMust{
				fastMustForOrds(1, 5),
				fastMustForOrds(5, 9, 13),
			},
			want: []DocOrd{5},
		},
		{
			name: "three lists",
			musts: []fastMust{
				fastMustForOrds(1, 5, 9),
				fastMustForOrds(3, 5, 7, 9),
				fastMustForOrds(5, 6, 8, 9, 10),
			},
			want: []DocOrd{5, 9},
		},
	}

	service := &Service{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, used, err := service.execBooleanAndSortMerge(
				tt.musts,
				nil,
				nil,
				context.Background(),
				nil,
				queryFieldScope{},
			)
			if err != nil {
				t.Fatal(err)
			}
			if !used {
				t.Fatal("execBooleanAndSortMerge() used = false, want true")
			}
			ords := make([]DocOrd, 0, len(got))
			for ord := range got {
				ords = append(ords, ord)
			}
			slices.Sort(ords)
			if !slices.Equal(ords, tt.want) {
				t.Fatalf("execBooleanAndSortMerge() ords = %v, want %v", ords, tt.want)
			}
		})
	}
}

func fastMustForOrds(ords ...DocOrd) fastMust {
	docs := make([]Posting, len(ords))
	for i, ord := range ords {
		docs[i] = Posting{Ord: ord, Count: 1}
	}
	return fastMust{
		expansions: []termExpansion{{field: DefaultField, docs: docs}},
		totalDocs:  len(docs),
	}
}
