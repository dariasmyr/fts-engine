package fts

import "testing"

func TestGallopPostingLowerBound(t *testing.T) {
	postings := []Posting{{Seq: 2}, {Seq: 5}, {Seq: 9}, {Seq: 14}, {Seq: 20}, {Seq: 27}}
	tests := []struct {
		name   string
		from   int
		target uint32
		want   int
	}{
		{name: "before first", target: 1, want: 0},
		{name: "first", target: 2, want: 0},
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
