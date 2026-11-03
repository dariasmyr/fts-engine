package presence

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

type source []posting.TermData

func (s source) ExportPostings(yield func(posting.TermData) error) error {
	for _, tp := range s {
		if err := yield(tp); err != nil {
			return err
		}
	}
	return nil
}

func postings(ords ...posting.Ord) []posting.Posting {
	out := make([]posting.Posting, len(ords))
	for i, o := range ords {
		out[i] = posting.Posting{Ord: o, Count: 3}
	}
	return out
}

func collect(t *testing.T, idx posting.OrdinalPostingOpener, keys []string, candidates []posting.Ord) []posting.Ord {
	t.Helper()
	var got []posting.Ord
	if err := posting.Intersect(context.Background(), idx, keys, candidates, func(o posting.Ord) bool {
		got = append(got, o)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestRandomizedIntersection(t *testing.T) {
	rng := rand.New(rand.NewSource(732))
	s := make(source, 12)
	truth := make([]map[posting.Ord]bool, len(s))
	for k := range s {
		s[k].Term = fmt.Sprint(k)
		truth[k] = map[posting.Ord]bool{}
		for o := range posting.Ord(3000) {
			if rng.Intn(k+2) == 0 {
				s[k].Postings = append(s[k].Postings, posting.Posting{Ord: o, Count: 1})
				truth[k][o] = true
			}
		}
	}
	blob, err := BuildFromSource(s)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	regular, err := ftse.Build(s)
	if err != nil {
		t.Fatal(err)
	}
	old, err := ftse.Open(regular)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "index.fpr")
	if err := os.WriteFile(path, blob, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for trial := range 150 {
		a, b := rng.Intn(len(s)), rng.Intn(len(s))
		keys := []string{fmt.Sprint(a), fmt.Sprint(b), fmt.Sprint(a)}
		var candidates []posting.Ord
		if trial%3 != 0 {
			candidates = make([]posting.Ord, 0)
			for o := range posting.Ord(3000) {
				if rng.Intn(30) == 0 {
					candidates = append(candidates, o)
				}
			}
		}
		var want []posting.Ord
		for o := range posting.Ord(3000) {
			if truth[a][o] && truth[b][o] && (candidates == nil || slices.Contains(candidates, o)) {
				want = append(want, o)
			}
		}
		for _, idx := range []posting.OrdinalPostingOpener{r, old, file} {
			if got := collect(t, idx, keys, candidates); !reflect.DeepEqual(got, want) {
				t.Fatalf("trial=%d got=%v want=%v", trial, got, want)
			}
		}
	}
}

func TestCursorTransitions(t *testing.T) {
	s := source{{Term: "edge", Postings: postings(0, 1, 128, math.MaxUint32)}, {Term: "single", Postings: postings(math.MaxUint32)}}
	blob, err := BuildFromSource(s)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"edge", "single", "absent"} {
		c, n, err := r.OpenOrdinalPostings(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if key == "absent" {
			if n != 0 || c.Next() || c.Advance(0) {
				t.Fatal("missing key")
			}
			_ = c.Close()
			continue
		}
		if !c.Advance(0) {
			t.Fatal(c.Err())
		}
		first := c.Ord()
		if !c.Advance(0) || c.Ord() != first {
			t.Fatal("Advance moved backwards")
		}
		if !c.Advance(math.MaxUint32) || c.Ord() != math.MaxUint32 {
			t.Fatal("max ordinal")
		}
		if c.Next() || c.Advance(0) || c.Next() {
			t.Fatal("exhaustion is not permanent")
		}
		if c.Err() != nil {
			t.Fatal(c.Err())
		}
		_ = c.Close()
	}
}

type countedReader struct {
	*bytes.Reader
	calls, read int
	fail        bool
}

var errRead = errors.New("injected read error")

func (r *countedReader) ReadAt(p []byte, off int64) (int, error) {
	r.calls++
	if r.fail {
		return 0, errRead
	}
	n, e := r.Reader.ReadAt(p, off)
	r.read += n
	return n, e
}

func TestSkippingCancellationAndReadErrors(t *testing.T) {
	p := make([]posting.Posting, 100000)
	for i := range p {
		p[i] = posting.Posting{Ord: posting.Ord(i), Count: 1}
	}
	blob, err := BuildFromSource(source{{Term: "common", Postings: p}})
	if err != nil {
		t.Fatal(err)
	}
	backing := &countedReader{Reader: bytes.NewReader(blob)}
	r, err := OpenReaderAt(backing, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, _, err := r.OpenOrdinalPostings(ctx, "common")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	backing.read = 0
	backing.calls = 0
	if !c.Advance(99999) || c.Ord() != 99999 {
		t.Fatal(c.Err())
	}
	dirBytes := ((len(p)-1)/blockSize + 1) * blockEntrySize
	if backing.calls != 2 || backing.read > dirBytes+blockSize*5 {
		t.Fatalf("skip read %d bytes in %d calls", backing.read, backing.calls)
	}
	cancel()
	if c.Next() || !errors.Is(c.Err(), context.Canceled) {
		t.Fatal(c.Err())
	}
	if _, _, err = r.OpenOrdinalPostings(ctx, "common"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c, _, err = r.OpenOrdinalPostings(context.Background(), "common")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	backing.fail = true
	if c.Next() || !errors.Is(c.Err(), errRead) {
		t.Fatal(c.Err())
	}
	c, _, err = r.OpenOrdinalPostings(context.Background(), "common")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.Advance(99999) || !errors.Is(c.Err(), errRead) {
		t.Fatal(c.Err())
	}
}

func TestInvalidSources(t *testing.T) {
	for _, s := range []source{
		{{Term: "a"}}, {{Postings: postings(1)}},
		{{Term: "a", Postings: postings(1, 1)}}, {{Term: "a", Postings: postings(2, 1)}},
		{{Term: "a", Postings: postings(1)}, {Term: "a", Postings: postings(2)}},
	} {
		if _, err := BuildFromSource(s); err == nil {
			t.Fatalf("accepted %v", s)
		}
	}
	blob, err := BuildFromSource(source{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(t, r, []string{"missing"}, nil); len(got) != 0 {
		t.Fatal(got)
	}
}

func fixCRC(blob []byte) {
	binary.LittleEndian.PutUint32(blob[len(blob)-8:], crc32.ChecksumIEEE(blob[:len(blob)-footerSize]))
}

func TestCorruption(t *testing.T) {
	blob, err := BuildFromSource(source{{Term: "a", Postings: postings(0, 1, 2)}})
	if err != nil {
		t.Fatal(err)
	}
	for n := range blob {
		if _, err := Open(blob[:n]); err == nil {
			t.Fatalf("accepted truncation at %d", n)
		}
	}
	for i := range blob {
		bad := bytes.Clone(blob)
		bad[i] ^= 0xff
		if _, err := Open(bad); err == nil {
			t.Fatalf("accepted corruption at %d", i)
		}
	}
	for _, mutate := range []func([]byte){
		func(b []byte) { binary.LittleEndian.PutUint32(b[4:], 0) }, // block maximum inconsistent
		func(b []byte) { binary.LittleEndian.PutUint32(b[8:], 0) }, // block offset into directory
		func(b []byte) { b[4+blockEntrySize+1] = 0 },               // duplicate ordinal
	} {
		bad := bytes.Clone(blob)
		mutate(bad)
		fixCRC(bad)
		r, err := Open(bad)
		if err != nil {
			continue
		}
		c, _, err := r.OpenOrdinalPostings(context.Background(), "a")
		if err != nil {
			continue
		}
		for c.Next() {
		}
		err = c.Err()
		_ = c.Close()
		if err == nil {
			t.Fatal("accepted invalid postings")
		}
	}
}

func FuzzOpen(f *testing.F) {
	blob, err := BuildFromSource(source{{Term: "a", Postings: postings(0, 1, math.MaxUint32)}})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(blob)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		// Also exercise structural validation past the checksum gate.
		if len(b) >= 4+1+footerSize {
			b = bytes.Clone(b)
			fixCRC(b)
		}
		r, err := Open(b)
		if err != nil {
			return
		}
		for _, term := range r.terms {
			c, _, err := r.OpenOrdinalPostings(context.Background(), term.key)
			if err != nil {
				continue
			}
			for c.Next() {
			}
			_ = c.Close()
		}
	})
}

var _ io.ReaderAt = (*countedReader)(nil)

func TestConcurrentCursors(t *testing.T) {
	blob, err := BuildFromSource(benchmarkSource())
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			count := 0
			err := posting.Intersect(context.Background(), r, []string{"common", "rare"}, nil, func(posting.Ord) bool {
				count++
				return true
			})
			if err != nil || count != 100 {
				t.Errorf("count=%d err=%v", count, err)
			}
		})
	}
	wg.Wait()
}
