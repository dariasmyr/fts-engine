package presence

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

func benchmarkSource() source {
	s := source{{Term: "common"}, {Term: "half"}, {Term: "rare"}}
	for o := range posting.Ord(100000) {
		p := posting.Posting{Ord: o, Count: 1}
		s[0].Postings = append(s[0].Postings, p)
		if o%2 == 0 {
			s[1].Postings = append(s[1].Postings, p)
		}
		if o%1000 == 0 {
			s[2].Postings = append(s[2].Postings, p)
		}
	}
	return s
}

// This baseline is the existing slice-returning sealed API followed by sorted
// intersection and external candidate filtering. All paths stream the same final
// ordinals to the same callback, so result allocation is not charged selectively.
func materialized(ctx context.Context, r *ftse.Reader, keys []string, candidates []posting.Ord, yield func(posting.Ord) bool) error {
	var acc []posting.Posting
	for i, key := range keys {
		p, err := r.Search(key)
		if err != nil {
			return err
		}
		if i == 0 {
			acc = p
			continue
		}
		out := acc[:0]
		a, b := 0, 0
		for a < len(acc) && b < len(p) {
			if acc[a].Ord == p[b].Ord {
				out = append(out, acc[a])
				a++
				b++
			} else if acc[a].Ord < p[b].Ord {
				a++
			} else {
				b++
			}
		}
		acc = out
	}
	j := 0
	for _, p := range acc {
		if err := ctx.Err(); err != nil {
			return err
		}
		if candidates != nil {
			for j < len(candidates) && candidates[j] < p.Ord {
				j++
			}
			if j == len(candidates) {
				break
			}
			if candidates[j] != p.Ord {
				continue
			}
		}
		if !yield(p.Ord) {
			break
		}
	}
	return nil
}

func BenchmarkIntersection(b *testing.B) {
	s := benchmarkSource()
	regular, err := ftse.Build(s)
	if err != nil {
		b.Fatal(err)
	}
	compact, err := BuildFromSource(s)
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		keys       []string
		candidates []posting.Ord
		want       int
	}{
		{"all", []string{"common", "half"}, nil, 50000},
		{"single", []string{"common"}, nil, 100000},
		{"missing", []string{"common", "missing"}, nil, 0},
		{"rare", []string{"common", "rare"}, nil, 100},
		{"candidates", []string{"common", "half"}, []posting.Ord{0, 10000, 50000, 99998}, 4},
	} {
		for _, mode := range []string{"materialized", "segment_iterator", "presence"} {
			b.Run(tc.name+"/"+mode, func(b *testing.B) {
				data := regular
				if mode == "presence" {
					data = compact
				}
				backing := &countedReader{Reader: bytes.NewReader(data)}
				var index posting.OrdinalPostingOpener
				var old *ftse.Reader
				if mode == "presence" {
					index, err = OpenReaderAt(backing, int64(len(data)))
				} else {
					old, err = ftse.OpenReaderAt(backing, int64(len(data)))
					index = old
				}
				if err != nil {
					b.Fatal(err)
				}
				backing.calls = 0
				backing.read = 0
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					count := 0
					yield := func(posting.Ord) bool {
						count++
						return true
					}
					if mode == "materialized" {
						err = materialized(context.Background(), old, tc.keys, tc.candidates, yield)
					} else {
						err = posting.Intersect(context.Background(), index, tc.keys, tc.candidates, yield)
					}
					if err != nil || count != tc.want {
						b.Fatalf("count=%d want=%d err=%v", count, tc.want, err)
					}
				}
				b.ReportMetric(float64(backing.read)/float64(b.N), "read-B/op")
				b.ReportMetric(float64(backing.calls)/float64(b.N), "reads/op")
				b.ReportMetric(float64(len(data)), "index-B")
			})
		}
	}
}

func BenchmarkBuild(b *testing.B) {
	for _, kind := range []string{"frequent", "singletons"} {
		s := benchmarkSource()
		if kind == "singletons" {
			s = make(source, 100000)
			for i := range s {
				s[i] = posting.TermData{Term: fmt.Sprintf("trace-%032x", i), Postings: postings(posting.Ord(i))}
			}
		}
		for _, mode := range []string{"segment", "presence", "presence_stream"} {
			b.Run(kind+"/"+mode, func(b *testing.B) {
				var data []byte
				var err error
				encoded, err := BuildFromSource(s)
				if err != nil {
					b.Fatal(err)
				}
				size := len(encoded)
				b.ReportAllocs()
				for b.Loop() {
					switch mode {
					case "segment":
						data, err = ftse.Build(s)
					case "presence_stream":
						err = WriteFromSource(context.Background(), io.Discard, s)
					default:
						data, err = BuildFromSource(s)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if mode == "presence_stream" {
					b.ReportMetric(float64(size), "index-B")
				} else {
					b.ReportMetric(float64(len(data)), "index-B")
				}
			})
		}
	}
}

// Includes candidate validation, lazy ID-to-ordinal conversion, and final ID
// resolution. IDs are sparse, and the input includes IDs outside the ftse.
func BenchmarkExternalIDs(b *testing.B) {
	blob, err := BuildFromSource(benchmarkSource())
	if err != nil {
		b.Fatal(err)
	}
	r, err := Open(blob)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]uint64, 100000)
	for i := range ids {
		ids[i] = uint64(i) * 1000000007
	}
	for _, tc := range []struct {
		name       string
		candidates []uint64
		keys       []string
		want       int
	}{
		{"sparse", []uint64{0, 1, ids[10000], ids[50000], ids[99998], ids[99998] + 1}, []string{"common", "half"}, 4},
		{"dense", ids, []string{"common", "half"}, 50000},
		{"dense_rare", ids, []string{"common", "rare"}, 100},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				candidates := externalCandidateOrdinals(ids, tc.candidates)
				count := 0
				err = posting.Intersect(context.Background(), r, tc.keys, candidates, func(o posting.Ord) bool {
					if uint64(o) >= uint64(len(ids)) {
						b.Fatal("invalid ordinal")
					}
					_ = ids[o]
					count++
					return true
				})
				if err != nil || count != tc.want {
					b.Fatalf("count=%d err=%v", count, err)
				}
			}
		})
	}
}

func externalCandidateOrdinals(ids, candidates []uint64) []posting.Ord {
	if candidates == nil {
		return nil
	}
	ords := make([]posting.Ord, 0, len(candidates))
	for _, id := range candidates {
		if i, ok := slices.BinarySearch(ids, id); ok {
			ords = append(ords, posting.Ord(i))
		}
	}
	return ords
}

// BenchmarkCandidateMatrix compares blocked FTSE reader reuse, blocked FTSE
// reader reopening, and FPR1 across the posting/candidate/term cardinalities
// used by filtered presence clients. FTSE has no runtime navigation cache. Run a
// focused slice with, for example:
//
//	go test ./pkg/presence -run '^$' \
//	  -bench 'BenchmarkCandidateMatrix/postings=10000/candidates=4/terms=2' \
//	  -benchmem -count=6
func BenchmarkCandidateMatrix(b *testing.B) {
	for _, postingCount := range []int{100, 10_000, 1_000_000} {
		b.Run(fmt.Sprintf("postings=%d", postingCount), func(b *testing.B) {
			postings := make([]posting.Posting, postingCount)
			for i := range postings {
				postings[i] = posting.Posting{Ord: posting.Ord(i), Count: 1}
			}
			s := make(source, 5)
			for i := range s {
				s[i] = posting.TermData{Term: fmt.Sprintf("term-%d", i), Postings: postings}
			}
			ftseData, err := ftse.Build(s)
			if err != nil {
				b.Fatal(err)
			}
			fprData, err := BuildFromSource(s)
			if err != nil {
				b.Fatal(err)
			}

			candidateCounts := []struct {
				name  string
				count int
			}{
				{name: "0", count: 0},
				{name: "1", count: 1},
				{name: "4", count: min(4, postingCount)},
				{name: "0.1pct", count: max(1, postingCount/1000)},
				{name: "10pct", count: max(1, postingCount/10)},
				{name: "100pct", count: postingCount},
			}
			for _, candidateCase := range candidateCounts {
				candidates := evenlySpacedOrdinals(postingCount, candidateCase.count)
				b.Run("candidates="+candidateCase.name, func(b *testing.B) {
					for _, termCount := range []int{1, 2, 5} {
						keys := make([]string, termCount)
						for i := range keys {
							keys[i] = fmt.Sprintf("term-%d", i)
						}
						b.Run(fmt.Sprintf("terms=%d", termCount), func(b *testing.B) {
							benchmarkMatrixBackend(b, "ftse_reader_reused", ftseData, keys, candidates, candidateCase.count)
							benchmarkMatrixBackend(b, "ftse_reader_reopened", ftseData, keys, candidates, candidateCase.count)
							benchmarkMatrixBackend(b, "fpr1", fprData, keys, candidates, candidateCase.count)
						})
					}
				})
			}
		})
	}
}

func BenchmarkBitmapCandidatePushdown(b *testing.B) {
	data, err := BuildFromSource(benchmarkSource())
	if err != nil {
		b.Fatal(err)
	}
	r, err := Open(data)
	if err != nil {
		b.Fatal(err)
	}
	candidates := []posting.Ord{0, 10_000, 50_000, 99_999}
	words := make([]uint64, (100_000+63)/64)
	for _, ord := range candidates {
		words[ord/64] |= uint64(1) << (ord % 64)
	}
	keys := []string{"common"}
	ctx := context.Background()

	b.Run("external_filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			count := 0
			err = posting.Intersect(ctx, r, keys, nil, func(ord posting.Ord) bool {
				if words[ord/64]&(uint64(1)<<(ord%64)) != 0 {
					count++
				}
				return true
			})
			if err != nil || count != len(candidates) {
				b.Fatalf("count=%d want=%d err=%v", count, len(candidates), err)
			}
		}
	})

	b.Run("slice_pushdown", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			count := 0
			err = posting.Intersect(ctx, r, keys, candidates, func(posting.Ord) bool {
				count++
				return true
			})
			if err != nil || count != len(candidates) {
				b.Fatalf("count=%d want=%d err=%v", count, len(candidates), err)
			}
		}
	})

	b.Run("bitmap_pushdown", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			count := 0
			iterator := posting.NewBitmapIterator(words)
			err = posting.IntersectWithIterator(
				ctx,
				r,
				keys,
				iterator,
				uint64(len(candidates)),
				func(posting.Ord) bool {
					count++
					return true
				},
			)
			if err != nil || count != len(candidates) {
				b.Fatalf("count=%d want=%d err=%v", count, len(candidates), err)
			}
		}
	})
}

func benchmarkMatrixBackend(b *testing.B, mode string, data []byte, keys []string, candidates []posting.Ord, want int) {
	b.Helper()
	b.Run(mode, func(b *testing.B) {
		var index posting.OrdinalPostingOpener
		var err error
		switch mode {
		case "fpr1":
			index, err = Open(data)
		default:
			index, err = ftse.Open(data)
		}
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			current := index
			if mode == "ftse_reader_reopened" {
				b.StopTimer()
				current, err = ftse.Open(data)
				b.StartTimer()
				if err != nil {
					b.Fatal(err)
				}
			}
			if err := runMatrixIntersection(current, keys, candidates, want); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(data)), "index-B")
	})
}

func runMatrixIntersection(index posting.OrdinalPostingOpener, keys []string, candidates []posting.Ord, want int) error {
	count := 0
	err := posting.Intersect(context.Background(), index, keys, candidates, func(posting.Ord) bool {
		count++
		return true
	})
	if err != nil {
		return err
	}
	if count != want {
		return fmt.Errorf("result count %d, want %d", count, want)
	}
	return nil
}

func evenlySpacedOrdinals(total, count int) []posting.Ord {
	ords := make([]posting.Ord, count)
	if count == 0 {
		return ords
	}
	for i := range ords {
		ords[i] = posting.Ord(uint64(i) * uint64(total) / uint64(count))
	}
	return ords
}
