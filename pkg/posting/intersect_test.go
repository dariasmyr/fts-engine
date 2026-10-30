package posting

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type testOpener struct {
	data      map[string][]Ord
	iterators []*testIterator
	opened    int
	closed    int
	openErr   error
	readErr   error
	closeErr  error
}

func (s *testOpener) OpenOrdinalPostings(_ context.Context, term string) (OrdinalIterator, uint64, error) {
	if s.openErr != nil {
		return nil, 0, s.openErr
	}
	s.opened++
	cursor := &testIterator{
		sliceIterator: sliceIterator{ords: s.data[term], pos: -1},
		opener:        s,
	}
	s.iterators = append(s.iterators, cursor)
	return cursor, uint64(len(s.data[term])), nil
}

func (i *testIterator) Err() error { return i.opener.readErr }

type testIterator struct {
	sliceIterator
	opener       *testOpener
	nextCalls    int
	advanceCalls int
}

func (i *testIterator) Next() bool {
	i.nextCalls++
	return i.sliceIterator.Next()
}

func (i *testIterator) Advance(target Ord) bool {
	i.advanceCalls++
	return i.sliceIterator.Advance(target)
}

func (i *testIterator) Close() error {
	i.opener.closed++
	return i.opener.closeErr
}

func TestIntersect(t *testing.T) {
	tests := []struct {
		name       string
		terms      []string
		candidates []Ord
		want       []Ord
		wantErr    bool
	}{
		{name: "all", terms: []string{"a", "b"}, want: []Ord{0, 4, 9}},
		{name: "filtered", terms: []string{"a", "b"}, candidates: []Ord{1, 4, 8}, want: []Ord{4}},
		{name: "empty candidates", terms: []string{"a"}, candidates: []Ord{}},
		{name: "missing term", terms: []string{"a", "missing"}},
		{name: "duplicate terms", terms: []string{"a", "a", "b"}, want: []Ord{0, 4, 9}},
		{name: "unsorted candidates", terms: []string{"a"}, candidates: []Ord{4, 0}, wantErr: true},
		{name: "duplicate candidates", terms: []string{"a"}, candidates: []Ord{4, 4}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opener := &testOpener{data: map[string][]Ord{
				"a": {0, 1, 4, 9},
				"b": {0, 4, 8, 9},
			}}
			var got []Ord
			err := Intersect(context.Background(), opener, tt.terms, tt.candidates, func(ord Ord) bool {
				got = append(got, ord)
				return true
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Intersect() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Intersect() = %v, want %v", got, tt.want)
			}
			if opener.opened != opener.closed {
				t.Fatalf("iterator lifecycle opened=%d closed=%d", opener.opened, opener.closed)
			}
		})
	}
}

func TestIntersectAdaptsCandidateDriver(t *testing.T) {
	for _, tt := range []struct {
		name           string
		candidates     []Ord
		wantSequential bool
	}{
		{name: "sparse_candidates_use_advance", candidates: []Ord{1, 8}},
		{name: "dense_candidates_use_sequential_postings", candidates: []Ord{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, wantSequential: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opener := &testOpener{data: map[string][]Ord{
				"term": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			}}
			if err := Intersect(context.Background(), opener, []string{"term"}, tt.candidates, func(Ord) bool {
				return true
			}); err != nil {
				t.Fatal(err)
			}
			cursor := opener.iterators[0]
			if tt.wantSequential {
				if cursor.nextCalls == 0 || cursor.advanceCalls != 0 {
					t.Fatalf("posting cursor Next=%d Advance=%d, want sequential traversal", cursor.nextCalls, cursor.advanceCalls)
				}
				return
			}
			if cursor.nextCalls != 0 || cursor.advanceCalls == 0 {
				t.Fatalf("posting cursor Next=%d Advance=%d, want candidate-driven traversal", cursor.nextCalls, cursor.advanceCalls)
			}
		})
	}
}

func TestIntersectWithoutCandidatesUsesShortestPostingList(t *testing.T) {
	opener := &testOpener{data: map[string][]Ord{
		"long":  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		"short": {1, 8},
	}}
	var got []Ord
	if err := Intersect(context.Background(), opener, []string{"long", "short"}, nil, func(ord Ord) bool {
		got = append(got, ord)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if want := []Ord{1, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Intersect() = %v, want %v", got, want)
	}

	longPostings := opener.iterators[0]
	shortPostings := opener.iterators[1]
	if shortPostings.nextCalls == 0 || shortPostings.advanceCalls != 0 {
		t.Fatalf(
			"short posting list Next=%d Advance=%d, want sequential driver",
			shortPostings.nextCalls,
			shortPostings.advanceCalls,
		)
	}
	if longPostings.nextCalls != 0 || longPostings.advanceCalls == 0 {
		t.Fatalf(
			"long posting list Next=%d Advance=%d, want aligned iterator",
			longPostings.nextCalls,
			longPostings.advanceCalls,
		)
	}
}

func TestIntersectLifecycle(t *testing.T) {
	sentinel := errors.New("injected")
	for _, stage := range []string{"open", "read", "close", "stop", "cancel"} {
		t.Run(stage, func(t *testing.T) {
			opener := &testOpener{data: map[string][]Ord{"a": {0, 1, 2}}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch stage {
			case "open":
				opener.openErr = sentinel
			case "read":
				opener.readErr = sentinel
			case "close":
				opener.closeErr = sentinel
			}
			calls := 0
			err := Intersect(ctx, opener, []string{"a"}, nil, func(Ord) bool {
				calls++
				if stage == "cancel" {
					cancel()
				}
				return stage != "stop"
			})
			switch stage {
			case "stop":
				if err != nil || calls != 1 {
					t.Fatalf("stop: calls=%d error=%v", calls, err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v, want context.Canceled", err)
				}
			default:
				if !errors.Is(err, sentinel) {
					t.Fatalf("error = %v, want %v", err, sentinel)
				}
			}
			if opener.opened != opener.closed {
				t.Fatalf("iterator lifecycle opened=%d closed=%d", opener.opened, opener.closed)
			}
		})
	}
}

func TestIntersectWithIteratorOwnsCandidates(t *testing.T) {
	readErr := errors.New("read")
	closeErr := errors.New("close")
	for _, stage := range []string{"nil opener", "nil yield", "no terms", "empty", "open", "missing", "stop", "read", "cancel"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opener := &testOpener{data: map[string][]Ord{"a": {0, 1, 2}}}
			candidateOwner := &testOpener{}
			candidate := &testIterator{
				sliceIterator: sliceIterator{ords: []Ord{0, 2}, pos: -1},
				opener:        candidateOwner,
			}
			var input OrdinalPostingOpener = opener
			terms := []string{"a"}
			count := uint64(2)
			yield := func(Ord) bool { return stage != "stop" }
			switch stage {
			case "nil opener":
				input = nil
			case "nil yield":
				yield = nil
			case "no terms":
				terms = nil
			case "empty":
				count = 0
			case "open":
				opener.openErr = readErr
			case "missing":
				terms = []string{"a", "absent"}
			case "read":
				candidateOwner.readErr = readErr
				candidateOwner.closeErr = closeErr
			case "cancel":
				cancel()
			}
			err := IntersectWithIterator(ctx, input, terms, candidate, count, yield)
			if candidateOwner.closed != 1 || opener.opened != opener.closed {
				t.Fatalf("leaked cursors: candidate=%d opener=%d/%d", candidateOwner.closed, opener.closed, opener.opened)
			}
			if stage == "read" && (!errors.Is(err, readErr) || !errors.Is(err, closeErr)) {
				t.Fatalf("error = %v, want joined read and close errors", err)
			}
			if stage == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		})
	}
}

func TestIntersectCancellationAtLastResult(t *testing.T) {
	opener := &testOpener{data: map[string][]Ord{"a": {0}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := Intersect(ctx, opener, []string{"a"}, nil, func(Ord) bool {
		cancel()
		return true
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestIntersectJoinsCloseError(t *testing.T) {
	wantErr := errors.New("close")
	opener := &testOpener{
		data:     map[string][]Ord{"a": {1}},
		closeErr: wantErr,
	}
	err := Intersect(context.Background(), opener, []string{"a"}, nil, func(Ord) bool { return true })
	if !errors.Is(err, wantErr) {
		t.Fatalf("Intersect() error = %v, want %v", err, wantErr)
	}
}

func TestBitmapIterator(t *testing.T) {
	iterator := NewBitmapIterator([]uint64{1<<1 | 1<<63, 1 << 6})
	defer iterator.Close()

	var got []Ord
	for iterator.Next() {
		got = append(got, iterator.Ord())
	}
	want := []Ord{1, 63, 70}
	if !reflect.DeepEqual(got, want) || iterator.Err() != nil {
		t.Fatalf("bitmap = %v err=%v, want %v", got, iterator.Err(), want)
	}
}
