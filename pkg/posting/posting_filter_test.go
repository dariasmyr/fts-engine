package posting

import (
	"context"
	"reflect"
	"testing"
)

type testFrequencyOpener struct {
	postings []Posting
	last     *testFrequencyIterator
	opened   int
	closed   int
}

type testFrequencyIterator struct {
	sliceIterator
	postings     []Posting
	owner        *testFrequencyOpener
	nextCalls    int
	advanceCalls int
}

func (i *testFrequencyIterator) Frequency() uint32 { return i.postings[i.pos].Count }

func (i *testFrequencyIterator) Next() bool {
	i.nextCalls++
	return i.sliceIterator.Next()
}

func (i *testFrequencyIterator) Advance(target Ord) bool {
	i.advanceCalls++
	return i.sliceIterator.Advance(target)
}

func (i *testFrequencyIterator) Close() error {
	i.owner.closed++
	return i.sliceIterator.Close()
}

func (s *testFrequencyOpener) OpenFrequencyPostings(context.Context, string) (FrequencyIterator, uint64, error) {
	s.opened++
	ords := make([]Ord, len(s.postings))
	for i, posting := range s.postings {
		ords[i] = posting.Ord
	}
	cursor := &testFrequencyIterator{
		sliceIterator: sliceIterator{ords: ords, pos: -1},
		postings:      s.postings,
		owner:         s,
	}
	s.last = cursor
	return cursor, uint64(len(ords)), nil
}

func TestFilterPostings(t *testing.T) {
	for _, test := range []struct {
		name       string
		candidates []Ord
		want       []Posting
	}{
		{name: "unrestricted", candidates: nil, want: []Posting{{Ord: 1, Count: 2}, {Ord: 4, Count: 3}}},
		{name: "filtered", candidates: []Ord{0, 4, 9}, want: []Posting{{Ord: 4, Count: 3}}},
		{name: "empty", candidates: []Ord{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			opener := &testFrequencyOpener{postings: []Posting{{Ord: 1, Count: 2}, {Ord: 4, Count: 3}}}
			var got []Posting
			documentFrequency, err := FilterPostings(
				context.Background(),
				opener,
				"key",
				test.candidates,
				func(posting Posting) bool {
					got = append(got, posting)
					return true
				},
			)
			if err != nil || documentFrequency != 2 || !reflect.DeepEqual(got, test.want) {
				t.Fatalf(
					"FilterPostings() = df %d, postings %v, error %v; want df 2, postings %v",
					documentFrequency,
					got,
					err,
					test.want,
				)
			}
			if opener.opened != 1 || opener.closed != 1 {
				t.Fatalf("iterator opened %d and closed %d times, want 1 and 1", opener.opened, opener.closed)
			}
		})
	}
}

func TestFilterPostingsAdaptsCandidateDriver(t *testing.T) {
	postings := make([]Posting, 10)
	for i := range postings {
		postings[i] = Posting{Ord: Ord(i), Count: 1}
	}
	for _, tt := range []struct {
		name           string
		candidates     []Ord
		wantSequential bool
	}{
		{name: "sparse_candidates_use_advance", candidates: []Ord{1, 8}},
		{name: "equal_size_candidates_keep_advance", candidates: []Ord{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{name: "dense_candidates_use_sequential_postings", candidates: []Ord{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, wantSequential: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opener := &testFrequencyOpener{postings: postings}
			if _, err := FilterPostings(
				context.Background(),
				opener,
				"term",
				tt.candidates,
				func(Posting) bool { return true },
			); err != nil {
				t.Fatal(err)
			}
			if tt.wantSequential {
				if opener.last.nextCalls == 0 || opener.last.advanceCalls != 0 {
					t.Fatalf("posting cursor Next=%d Advance=%d, want sequential traversal", opener.last.nextCalls, opener.last.advanceCalls)
				}
				return
			}
			if opener.last.nextCalls != 0 || opener.last.advanceCalls == 0 {
				t.Fatalf("posting cursor Next=%d Advance=%d, want candidate-driven traversal", opener.last.nextCalls, opener.last.advanceCalls)
			}
		})
	}
}

func TestFilterPostingsRejectsUnsortedCandidatesBeforeOpeningSource(t *testing.T) {
	opener := &testFrequencyOpener{postings: []Posting{{Ord: 1, Count: 1}}}
	if _, err := FilterPostings(context.Background(), opener, "key", []Ord{2, 1}, func(Posting) bool { return true }); err == nil {
		t.Fatal("FilterPostings() accepted unsorted candidates")
	}
	if opener.opened != 0 {
		t.Fatalf("opener opened %d times, want 0", opener.opened)
	}
}

func TestFilterPostingsWithIteratorOwnsCandidates(t *testing.T) {
	opener := &testFrequencyOpener{postings: []Posting{{Ord: 1, Count: 2}, {Ord: 4, Count: 3}}}
	candidateOwner := &testOpener{}
	candidates := &testIterator{
		sliceIterator: sliceIterator{ords: []Ord{4}, pos: -1},
		opener:        candidateOwner,
	}
	var got []Posting
	_, err := FilterPostingsWithIterator(
		context.Background(),
		opener,
		"key",
		candidates,
		1,
		func(posting Posting) bool {
			got = append(got, posting)
			return true
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := []Posting{{Ord: 4, Count: 3}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FilterPostingsWithIterator() = %v, want %v", got, want)
	}
	if candidateOwner.closed != 1 || opener.closed != 1 {
		t.Fatalf("closed candidates %d and postings %d times, want 1 and 1", candidateOwner.closed, opener.closed)
	}
}

func TestFilterPostingIterator(t *testing.T) {
	opener := &testFrequencyOpener{postings: []Posting{{Ord: 1, Count: 2}, {Ord: 4, Count: 3}}}
	postingCursor, postingCount, err := opener.OpenFrequencyPostings(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	candidateOwner := &testOpener{}
	candidates := &testIterator{
		sliceIterator: sliceIterator{ords: []Ord{4}, pos: -1},
		opener:        candidateOwner,
	}
	var got []Posting
	err = FilterPostingIterator(
		context.Background(),
		postingCursor,
		postingCount,
		candidates,
		1,
		func(posting Posting) bool {
			got = append(got, posting)
			return true
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := []Posting{{Ord: 4, Count: 3}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FilterPostingIterator() = %v, want %v", got, want)
	}
	if candidateOwner.closed != 1 || opener.closed != 1 {
		t.Fatalf("closed candidates %d and postings %d times, want 1 and 1", candidateOwner.closed, opener.closed)
	}
}
