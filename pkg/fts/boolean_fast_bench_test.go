package fts

import (
	"context"
	"fmt"
	"testing"
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
