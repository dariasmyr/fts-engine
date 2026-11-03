package presence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

type sourceFunc func(func(posting.TermData) error) error

func (s sourceFunc) ExportPostings(yield func(posting.TermData) error) error { return s(yield) }

type writerFunc func([]byte) (int, error)

func (w writerFunc) Write(p []byte) (int, error) { return w(p) }

func TestStreamingWrite(t *testing.T) {
	s := benchmarkSource()
	s = append(s, posting.TermData{Term: "singleton", Postings: postings(math.MaxUint32)})
	var buf bytes.Buffer
	if err := WriteFromSource(context.Background(), &buf, s); err != nil {
		t.Fatal(err)
	}
	r, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for _, tp := range s {
		got := collect(t, r, []string{tp.Term}, nil)
		want := make([]posting.Ord, len(tp.Postings))
		for i, p := range tp.Postings {
			want[i] = p.Ord
		}
		if !slices.Equal(got, want) {
			t.Fatalf("roundtrip %q", tp.Term)
		}
	}
	// The callback's posting storage may be reused immediately after it returns.
	reused := sourceFunc(func(yield func(posting.TermData) error) error {
		p := postings(0, 1, math.MaxUint32)
		if err := yield(posting.TermData{Term: "a", Postings: p}); err != nil {
			return err
		}
		p[0].Ord, p[1].Ord, p[2].Ord = 2, 3, 4
		return yield(posting.TermData{Term: "b", Postings: p})
	})
	blob, err := BuildFromSource(reused)
	if err != nil {
		t.Fatal(err)
	}
	r, err = Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(t, r, []string{"a"}, nil); !slices.Equal(got, []posting.Ord{0, 1, math.MaxUint32}) {
		t.Fatal(got)
	}
}

// Keep this digest stable: the payload covers singleton extremes and three
// delta blocks, so a change requires an explicit FPR version decision.
func TestFPR1Compatibility(t *testing.T) {
	p := make([]posting.Posting, 300)
	for i := range p {
		p[i] = posting.Posting{Ord: posting.Ord(i * 1000000), Count: 1}
	}
	got, err := BuildFromSource(source{{Term: "z", Postings: postings(math.MaxUint32)}, {Term: "a", Postings: p}, {Term: "zero", Postings: postings(0)}})
	if err != nil {
		t.Fatal(err)
	}
	const wantDigest = "8ef024390dc2f551105a03114acf90ae1ebaf29edb03fd147bef8ab20823ddf9"
	if digest := fmt.Sprintf("%x", sha256.Sum256(got)); digest != wantDigest {
		t.Fatalf("FPR1 encoding changed: digest=%s", digest)
	}
	r, err := Open(got)
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(t, r, []string{"a"}, nil); len(got) != 300 || got[299] != 299000000 {
		t.Fatal(got)
	}
}

func TestStreamingWriteFailures(t *testing.T) {
	sentinel := errors.New("write failed")
	s := benchmarkSource()
	// Fail at the initial flush, in postings, in the dictionary and in the footer.
	blob, err := BuildFromSource(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, 40000, len(blob) - footerSize - 1, len(blob) - 1} {
		left := limit
		w := writerFunc(func(p []byte) (int, error) {
			n := min(left, len(p))
			left -= n
			if n < len(p) {
				return n, sentinel
			}
			return n, nil
		})
		if err := WriteFromSource(context.Background(), w, s); !errors.Is(err, sentinel) {
			t.Fatalf("limit %d: %v", limit, err)
		}
	}
	short := writerFunc(func(p []byte) (int, error) { return len(p) - 1, nil })
	if err := WriteFromSource(context.Background(), short, s); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	if err := WriteFromSource(context.Background(), io.Discard, sourceFunc(func(func(posting.TermData) error) error { return sentinel })); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := writerFunc(func(p []byte) (int, error) {
		cancel()
		return len(p), nil
	})
	if err := WriteFromSource(ctx, w, s); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := WriteFromSource(ctx, io.Discard, s); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLazyMetadataAndReadAhead(t *testing.T) {
	blob, err := BuildFromSource(benchmarkSource())
	if err != nil {
		t.Fatal(err)
	}
	backing := &countedReader{Reader: bytes.NewReader(blob)}
	r, err := OpenReaderAt(backing, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	backing.calls, backing.read = 0, 0
	collect(t, r, []string{"common", "missing"}, nil)
	if backing.calls != 0 {
		t.Fatal("missing term read postings")
	}
	if err := posting.Intersect(context.Background(), r, []string{"common"}, []posting.Ord{}, func(posting.Ord) bool {
		t.Fatal("unexpected match")
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if backing.calls != 0 {
		t.Fatal("empty candidate stream read postings")
	}
	got := collect(t, r, []string{"common"}, nil)
	if len(got) != 100000 {
		t.Fatal(len(got))
	}
	if backing.calls > 10 {
		t.Fatalf("sequential traversal made %d reads", backing.calls)
	}
	if backing.read > 120000 {
		t.Fatalf("sequential traversal read %d bytes", backing.read)
	}
	// Cross read-ahead windows and alternate sequential access with long jumps.
	c, _, err := r.OpenOrdinalPostings(context.Background(), "common")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, ord := range []posting.Ord{0, 128, 130, 40000, 40128, 99999} {
		if !c.Advance(ord) || c.Ord() != ord {
			t.Fatalf("advance %d: %d %v", ord, c.Ord(), c.Err())
		}
		if ord < 99999 && (!c.Next() || c.Ord() != ord+1) {
			t.Fatal(c.Err())
		}
	}
	if c.Next() || c.Err() != nil {
		t.Fatal(c.Err())
	}
}
