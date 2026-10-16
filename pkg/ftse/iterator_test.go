package ftse_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

type failingReader struct {
	*bytes.Reader
	fail        bool
	calls, read int
}

var errIteratorRead = errors.New("injected I/O error")

type indexOnly struct{ fts.Index }

func (r *failingReader) ReadAt(p []byte, off int64) (int, error) {
	r.calls++
	if r.fail {
		return 0, errIteratorRead
	}
	n, err := r.Reader.ReadAt(p, off)
	r.read += n
	return n, err
}

func TestOrdinalIteratorLifecycle(t *testing.T) {
	blob := readV2Golden(t)
	backing := &failingReader{Reader: bytes.NewReader(blob)}
	r, err := ftse.OpenReaderAt(backing, int64(len(blob)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, n, err := r.OpenOrdinalPostings(ctx, "a")
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	defer c.Close()
	values := c.(posting.FrequencyIterator)
	if !c.Advance(0) || c.Ord() != 0 || values.Frequency() != 2 ||
		!c.Next() || c.Ord() != 7 || values.Frequency() != 1 ||
		!c.Advance(1) || c.Ord() != 7 || values.Frequency() != 1 {
		t.Fatal("bad cursor transitions")
	}
	if !c.Advance(math.MaxUint32) || c.Ord() != math.MaxUint32 || values.Frequency() != 3 ||
		c.Next() || c.Advance(0) || c.Err() != nil {
		t.Fatalf("bad exhaustion: %v", c.Err())
	}
	c, _, err = r.OpenOrdinalPostings(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	if c.Next() || !errors.Is(c.Err(), context.Canceled) {
		t.Fatalf("cancel: %v", c.Err())
	}
	if _, _, err = r.OpenOrdinalPostings(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c, n, err = r.OpenOrdinalPostings(context.Background(), "missing")
	if err != nil || n != 0 || c.Next() || c.Err() != nil {
		t.Fatal("missing key")
	}
	_ = c.Close()
	backing.fail = true
	if _, _, err = r.OpenOrdinalPostings(context.Background(), "a"); !errors.Is(err, errIteratorRead) {
		t.Fatalf("I/O error lost: %v", err)
	}
}

func TestFrequencyIteratorDrivesFilteredServiceSearch(t *testing.T) {
	blob := readV2Golden(t)
	reader, err := ftse.Open(blob)
	if err != nil {
		t.Fatal(err)
	}
	service := fts.New(
		reader,
		fts.WordKeys,
		fts.WithDocRegistrySnapshot([]fts.DocID{"a", "b", "c"}),
		fts.WithCollectionStatsSnapshot(&fts.CollectionStatsSnapshot{
			DocsSeen: map[fts.DocID]bool{"a": true, "b": true, "c": true},
			DocLen: map[string]map[fts.DocID]uint32{
				fts.DefaultField: {"a": 2, "b": 7, "c": 3},
			},
			TotalLen: map[string]uint64{fts.DefaultField: 12},
		}),
		fts.WithScorer(fts.BM25()),
	)

	full, err := service.SearchQuery(context.Background(), fts.TermQuery{Term: "alpha"}, fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.SearchQuery(
		fts.WithDiagnostics(context.Background()),
		fts.TermQuery{Term: "alpha"},
		fts.SearchOptions{CandidateIDs: []fts.DocID{"b"}, MaxResults: 10},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 1 || result.Results[0].ID != "b" || result.Results[0].TotalMatches != 7 {
		t.Fatalf("results = %+v, want b with TotalMatches=7", result.Results)
	}
	if result.Diagnostics == nil || result.Diagnostics.PostingAccess.CursorPostingLists != 1 {
		t.Fatalf("v2 traversal diagnostics = %+v", result.Diagnostics)
	}
	repeated, err := service.SearchQuery(
		fts.WithDiagnostics(context.Background()),
		fts.TermQuery{Term: "alpha"},
		fts.SearchOptions{CandidateIDs: []fts.DocID{"b"}, MaxResults: 10},
	)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.Diagnostics == nil || repeated.Diagnostics.PostingAccess.CursorPostingLists != 1 {
		t.Fatalf("repeated v2 traversal diagnostics = %+v", repeated.Diagnostics)
	}
	for _, item := range full.Results {
		if item.ID == "b" && result.Results[0] != item {
			t.Fatalf("filtered result = %+v, unrestricted result = %+v", result.Results[0], item)
		}
	}
	dense, err := service.SearchQuery(
		context.Background(),
		fts.TermQuery{Term: "alpha"},
		fts.SearchOptions{CandidateIDs: []fts.DocID{"a", "b", "c"}, MaxResults: 10},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dense, full) {
		t.Fatalf("dense filtered result = %+v, unrestricted result = %+v", dense, full)
	}
}

func TestV2V3RankedAndPositionalCompatibility(t *testing.T) {
	v2, err := ftse.Open(readV2Golden(t))
	if err != nil {
		t.Fatal(err)
	}
	v3Data, err := ftse.Build([]posting.TermData{
		{
			Term:      "alpha",
			Postings:  []posting.Posting{{Ord: 0, Count: 2}, {Ord: 1, Count: 7}, {Ord: 2, Count: 3}},
			Positions: [][]uint32{{0, 4}, {1}, {2, 5, 8}},
		},
		{
			Term:      "brown",
			Postings:  []posting.Posting{{Ord: 0, Count: 1}, {Ord: 1, Count: 1}},
			Positions: [][]uint32{{1}, {1}},
		},
		{
			Term:      "quick",
			Postings:  []posting.Posting{{Ord: 0, Count: 1}, {Ord: 1, Count: 1}},
			Positions: [][]uint32{{0}, {2}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	v3, err := ftse.Open(v3Data)
	if err != nil {
		t.Fatal(err)
	}
	ids := []fts.DocID{"a", "b", "c"}
	stats := &fts.CollectionStatsSnapshot{
		DocsSeen: map[fts.DocID]bool{"a": true, "b": true, "c": true},
		DocLen: map[string]map[fts.DocID]uint32{
			fts.DefaultField: {"a": 4, "b": 9, "c": 3},
		},
		TotalLen: map[string]uint64{fts.DefaultField: 16},
	}
	newService := func(index fts.Index) *fts.Service {
		return fts.New(
			index,
			fts.WordKeys,
			fts.WithDocRegistrySnapshot(ids),
			fts.WithCollectionStatsSnapshot(stats),
			fts.WithScorer(fts.BM25()),
		)
	}
	v2Service, v3Service := newService(v2), newService(v3)
	queries := []fts.Query{
		fts.TermQuery{Term: "alpha"},
		fts.PhraseQuery{Phrase: "quick brown"},
		fts.PrefixQuery{Prefix: "al"},
		&fts.BooleanQuery{Clauses: []fts.BoolClause{
			fts.MustClause(fts.TermQuery{Term: "alpha"}),
			fts.ShouldClause(fts.TermQuery{Term: "quick"}),
		}},
	}
	for _, query := range queries {
		v2Result, err := v2Service.SearchQuery(context.Background(), query, fts.SearchOptions{MaxResults: 10})
		if err != nil {
			t.Fatalf("v2 search %T: %v", query, err)
		}
		v3Result, err := v3Service.SearchQuery(context.Background(), query, fts.SearchOptions{MaxResults: 10})
		if err != nil {
			t.Fatalf("v3 search %T: %v", query, err)
		}
		if !reflect.DeepEqual(v2Result, v3Result) {
			t.Fatalf("query %T differs\nv2=%+v\nv3=%+v", query, v2Result, v3Result)
		}
	}
}

func readV2Golden(t testing.TB) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/ftse-v2.golden")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func BenchmarkServiceSearchCandidateOptions(b *testing.B) {
	const documentCount = 100_000
	postings := make([]fts.Posting, documentCount)
	ids := make([]fts.DocID, documentCount)
	for i := range documentCount {
		postings[i] = fts.Posting{Ord: fts.DocOrd(i), Count: uint32(i%5 + 1)}
		ids[i] = fts.DocID(fmt.Sprintf("doc-%06d", i))
	}
	blob, err := ftse.Build([]posting.TermData{{Term: "common", Postings: postings}})
	if err != nil {
		b.Fatal(err)
	}
	reader, err := ftse.Open(blob)
	if err != nil {
		b.Fatal(err)
	}
	candidates := []fts.DocID{ids[0], ids[10_000], ids[50_000], ids[99_999]}

	for _, scoring := range []struct {
		name   string
		scorer fts.Scorer
	}{
		{name: "unscored"},
		{name: "scored", scorer: fts.BM25()},
	} {
		b.Run(scoring.name, func(b *testing.B) {
			options := func() []*fts.Option {
				out := []*fts.Option{fts.WithDocRegistrySnapshot(ids)}
				if scoring.scorer != nil {
					out = append(out, fts.WithScorer(scoring.scorer))
				}
				return out
			}
			for _, tc := range []struct {
				name  string
				index fts.Index
			}{
				{name: "materialized", index: indexOnly{Index: reader}},
				{name: "cursor_reader_reused", index: reader},
			} {
				b.Run(tc.name, func(b *testing.B) {
					service := fts.New(tc.index, fts.WordKeys, options()...)
					b.ReportAllocs()
					for b.Loop() {
						result, err := service.SearchQuery(context.Background(), fts.TermQuery{Term: "common"}, fts.SearchOptions{
							CandidateIDs: candidates,
						})
						if err != nil {
							b.Fatal(err)
						}
						if len(result.Results) != len(candidates) {
							b.Fatalf("results = %d, want %d", len(result.Results), len(candidates))
						}
					}
				})
			}

			b.Run("cursor_reader_reopened", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					coldReader, err := ftse.Open(blob)
					if err != nil {
						b.Fatal(err)
					}
					service := fts.New(coldReader, fts.WordKeys, options()...)
					b.StartTimer()
					result, err := service.SearchQuery(context.Background(), fts.TermQuery{Term: "common"}, fts.SearchOptions{
						CandidateIDs: candidates,
					})
					if err != nil {
						b.Fatal(err)
					}
					if len(result.Results) != len(candidates) {
						b.Fatalf("results = %d, want %d", len(result.Results), len(candidates))
					}
				}
			})
		})
	}
}
