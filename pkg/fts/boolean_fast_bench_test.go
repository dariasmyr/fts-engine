package fts

import (
	"context"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

var postingBenchmarkSink int

func BenchmarkPostingIntersection(b *testing.B) {
	for _, shortSize := range []int{8, 128, 2048} {
		for _, ratio := range []int{1, 2, 4, 8, 16, 64, 256} {
			for _, overlapPercent := range []int{0, 10, 100} {
				name := fmt.Sprintf("short=%d/ratio=%d/overlap=%d%%", shortSize, ratio, overlapPercent)
				short, long, want := makePostingIntersectionBenchmarkData(shortSize, ratio, overlapPercent)

				b.Run(name+"/two-pointer", func(b *testing.B) {
					if got := intersectPostingsTwoPointer(short, long); got != want {
						b.Fatalf("intersection count = %d, want %d", got, want)
					}
					for b.Loop() {
						intersectPostingsTwoPointer(short, long)
					}
				})

				b.Run(name+"/galloping", func(b *testing.B) {
					if got := intersectPostingsGalloping(short, long); got != want {
						b.Fatalf("intersection count = %d, want %d", got, want)
					}
					for b.Loop() {
						intersectPostingsGalloping(short, long)
					}
				})
			}
		}
	}
}

func BenchmarkGallopPostingLowerBound(b *testing.B) {
	postings := make([]Posting, 8192)
	for i := range postings {
		postings[i].Ord = DocOrd(i * 2)
	}

	for _, benchmark := range []struct {
		name   string
		from   int
		target DocOrd
	}{
		{name: "current", from: 100, target: postings[100].Ord},
		{name: "next", from: 100, target: postings[101].Ord},
		{name: "distant", from: 100, target: postings[4196].Ord},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				postingBenchmarkSink = gallopPostingLowerBound(postings, benchmark.from, benchmark.target)
			}
		})
	}
}

func BenchmarkBooleanAndSortMergeRange(b *testing.B) {
	for _, benchmark := range []struct {
		name  string
		musts []fastMust
	}{
		{
			name: "disjoint-comparable",
			musts: []fastMust{
				benchmarkFastMust(10_000, 128, 1),
				benchmarkFastMust(0, 512, 1),
			},
		},
		{
			name: "tiny-overlap",
			musts: []fastMust{
				benchmarkFastMust(8, 2, 7),
				benchmarkFastMust(0, 16, 1),
			},
		},
		{
			name: "comparable-overlap",
			musts: []fastMust{
				benchmarkFastMust(0, 128, 4),
				benchmarkFastMust(0, 512, 1),
			},
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			service := &Service{}
			b.ReportAllocs()
			for b.Loop() {
				got, _, err := service.execBooleanAndSortMerge(
					benchmark.musts,
					nil,
					nil,
					context.Background(),
					nil,
					queryFieldScope{},
				)
				if err != nil {
					b.Fatal(err)
				}
				postingBenchmarkSink = len(got)
			}
		})
	}
}

func BenchmarkBooleanAndPostingIntersect(b *testing.B) {
	const documentCount = 10_000
	common := make([]Posting, documentCount)
	half := make([]Posting, documentCount/2)
	for i := range common {
		common[i] = Posting{Ord: DocOrd(i), Count: 1}
		if i%2 == 0 {
			half[i/2] = Posting{Ord: DocOrd(i), Count: 1}
		}
	}
	data, err := ftse.Build([]posting.TermData{
		{Term: "common", Postings: common},
		{Term: "half", Postings: half},
	})
	if err != nil {
		b.Fatal(err)
	}
	reader, err := ftse.Open(data)
	if err != nil {
		b.Fatal(err)
	}
	service := New(reader, WordKeys)
	mustTerms := []TermQuery{{Term: "common"}, {Term: "half"}}
	ctx := context.Background()

	for _, benchmark := range []struct {
		name       string
		candidates []DocOrd
	}{
		{name: "candidates=4", candidates: []DocOrd{0, 100, 5_000, 9_998}},
		{name: "candidates=1024", candidates: evenlySpacedBenchmarkOrds(documentCount, 1_024)},
		{name: "unrestricted"},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			var candidates *candidateSet
			if benchmark.candidates != nil {
				candidates = candidateSetFromSortedOrds(benchmark.candidates)
			}
			b.ReportAllocs()
			for b.Loop() {
				got, used, err := service.tryExecPostingAnd(ctx, candidates, mustTerms, nil, nil, queryFieldScope{})
				if err != nil || !used {
					b.Fatalf("used=%v err=%v", used, err)
				}
				postingBenchmarkSink = len(got)
			}
		})
	}
}

func BenchmarkServiceCandidateSelectivity(b *testing.B) {
	benchmarkServiceCandidateSelectivity(b, false)
}

func BenchmarkServicePreparedCandidateSelectivity(b *testing.B) {
	benchmarkServiceCandidateSelectivity(b, true)
}

func benchmarkServiceCandidateSelectivity(b *testing.B, prepared bool) {
	const documentCount = 10_000
	common := make([]Posting, documentCount)
	half := make([]Posting, documentCount/2)
	rare := make([]Posting, documentCount/100)
	ids := make([]DocID, documentCount)
	for i := range documentCount {
		ids[i] = DocID(fmt.Sprintf("doc-%05d", i))
		common[i] = Posting{Ord: DocOrd(i), Count: uint32(i%3 + 1)}
		if i%2 == 0 {
			half[i/2] = Posting{Ord: DocOrd(i), Count: 1}
		}
		if i%100 == 0 {
			rare[i/100] = Posting{Ord: DocOrd(i), Count: 1}
		}
	}
	data, err := ftse.Build([]posting.TermData{
		{Term: "common", Postings: common},
		{Term: "half", Postings: half},
		{Term: "rare", Postings: rare},
	})
	if err != nil {
		b.Fatal(err)
	}
	reader, err := ftse.Open(data)
	if err != nil {
		b.Fatal(err)
	}
	unranked := New(reader, WordKeys, WithDocRegistrySnapshot(ids))
	ranked := New(reader, WordKeys, WithDocRegistrySnapshot(ids), WithScorer(BM25()))
	andQuery := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "common"}),
		MustClause(TermQuery{Term: "half"}),
	}}
	rankedQuery := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "common"}),
		ShouldClause(TermQuery{Term: "half"}),
	}}
	queries := []struct {
		name          string
		service       *Service
		query         Query
		maxResults    int
		queryPostings int
		postingLists  int
		minPostings   int
	}{
		{
			name: "frequent", service: unranked, query: TermQuery{Term: "common"},
			queryPostings: len(common), postingLists: 1, minPostings: len(common),
		},
		{
			name: "and", service: unranked, query: andQuery,
			queryPostings: len(common) + len(half), postingLists: 2, minPostings: len(half),
		},
		{
			name: "rare", service: unranked, query: TermQuery{Term: "rare"},
			queryPostings: len(rare), postingLists: 1, minPostings: len(rare),
		},
		{
			name: "ranked_top10", service: ranked, query: rankedQuery, maxResults: 10,
			queryPostings: len(common) + len(half), postingLists: 2, minPostings: len(half),
		},
	}
	candidateCases := []struct {
		name string
		ords []DocOrd
	}{
		{name: "count=4_distribution=even", ords: evenlySpacedBenchmarkOrds(documentCount, 4)},
		{name: "count=4_distribution=same-block", ords: []DocOrd{0, 1, 2, 3}},
		{name: "count=4_distribution=different-blocks", ords: []DocOrd{0, 128, 256, 384}},
		{name: "count=4_distribution=tail", ords: []DocOrd{9_996, 9_997, 9_998, 9_999}},
		{name: "count=100_distribution=even", ords: evenlySpacedBenchmarkOrds(documentCount, 100)},
		{name: "count=1000_distribution=even", ords: evenlySpacedBenchmarkOrds(documentCount, 1_000)},
		{name: "count=5000_distribution=even", ords: evenlySpacedBenchmarkOrds(documentCount, 5_000)},
	}
	for _, candidates := range candidateCases {
		candidateIDs := make([]DocID, len(candidates.ords))
		for i, ord := range candidates.ords {
			candidateIDs[i] = ids[ord]
		}
		preparedCandidates, err := unranked.newCandidateSet(context.Background(), candidateIDs)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(candidates.name, func(b *testing.B) {
			for _, query := range queries {
				b.Run(query.name, func(b *testing.B) {
					ctx, traversal := posting.WithTraversalMetrics(context.Background())
					var result *SearchResult
					b.ReportAllocs()
					for b.Loop() {
						if prepared {
							result, err = query.service.searchResultForQuery(
								ctx,
								preparedCandidates,
								query.query,
								query.maxResults,
								queryFieldScope{},
							)
						} else {
							result, err = query.service.SearchQuery(ctx, query.query, SearchOptions{
								CandidateIDs: candidateIDs,
								MaxResults:   query.maxResults,
							})
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					metrics := traversal.Snapshot()
					b.ReportMetric(float64(query.postingLists), "posting-lists/op")
					b.ReportMetric(float64(query.minPostings), "min-postings/op")
					b.ReportMetric(float64(query.queryPostings), "query-postings/op")
					b.ReportMetric(float64(metrics.DecodedBlocks)/float64(b.N), "decoded-blocks/op")
					b.ReportMetric(float64(metrics.DecodedPostings)/float64(b.N), "decoded-postings/op")
					b.ReportMetric(float64(result.TotalResultsCount), "hits-before-sort/op")
				})
			}
		})
	}
}

func BenchmarkCandidateSetPreparation(b *testing.B) {
	const documentCount = 10_000
	ids := make([]DocID, documentCount)
	for i := range ids {
		ids[i] = DocID(fmt.Sprintf("doc-%05d", i))
	}
	service := New(newMemoryIndex(), WordKeys, WithDocRegistrySnapshot(ids))
	for _, count := range []int{4, 100, 1_000, 5_000} {
		candidateIDs := make([]DocID, count)
		for i, ord := range evenlySpacedBenchmarkOrds(documentCount, count) {
			candidateIDs[i] = ids[ord]
		}
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				candidates, err := service.newCandidateSet(ctx, candidateIDs)
				if err != nil {
					b.Fatal(err)
				}
				postingBenchmarkSink = candidates.cardinality()
			}
		})
	}
}

func evenlySpacedBenchmarkOrds(total, count int) []DocOrd {
	ords := make([]DocOrd, count)
	for i := range ords {
		ords[i] = DocOrd(i * total / count)
	}
	return ords
}

func benchmarkFastMust(start uint32, size int, step uint32) fastMust {
	docs := make([]Posting, size)
	for i := range docs {
		ord := start + uint32(i)*step
		docs[i] = Posting{Ord: DocOrd(ord), Count: 1}
	}
	return fastMust{
		expansions: []termExpansion{{field: DefaultField, docs: docs}},
		totalDocs:  len(docs),
	}
}

func makePostingIntersectionBenchmarkData(shortSize, ratio, overlapPercent int) ([]Posting, []Posting, int) {
	longSize := shortSize * ratio
	long := make([]Posting, longSize)
	for i := range long {
		long[i].Ord = DocOrd(i * 2)
	}

	short := make([]Posting, shortSize)
	want := 0
	for i := range short {
		longIndex := i * ratio
		ord := DocOrd(longIndex * 2)
		matches := overlapPercent == 100 || overlapPercent > 0 && i%(100/overlapPercent) == 0
		if matches {
			want++
		} else {
			ord++
		}
		short[i].Ord = ord
	}
	return short, long, want
}

func intersectPostingsTwoPointer(left, right []Posting) int {
	i, j, matches := 0, 0, 0
	for i < len(left) && j < len(right) {
		switch {
		case left[i].Ord < right[j].Ord:
			i++
		case left[i].Ord > right[j].Ord:
			j++
		default:
			matches++
			i++
			j++
		}
	}
	return matches
}

func intersectPostingsGalloping(short, long []Posting) int {
	if len(short) > len(long) {
		short, long = long, short
	}

	longIndex := 0
	matches := 0
	for _, posting := range short {
		longIndex = gallopPostingLowerBound(long, longIndex, posting.Ord)
		if longIndex == len(long) {
			break
		}
		if long[longIndex].Ord == posting.Ord {
			matches++
			longIndex++
		}
	}
	return matches
}
