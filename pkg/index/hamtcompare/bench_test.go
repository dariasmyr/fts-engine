package hamtcompare

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/hamtfirst"
)

type positionalIndex interface {
	fts.Index
	InsertAt(string, uint32, fts.DocOrd) error
	SearchPositional(string) ([]fts.PositionalPosting, error)
}

var positionalResult []fts.PositionalPosting

var implementations = []struct {
	name string
	new  func() positionalIndex
}{
	{name: "hamt", new: func() positionalIndex { return hamt.New() }},
	{name: "hamt-first", new: func() positionalIndex { return hamtfirst.New() }},
}

func BenchmarkBuildSingletonTerms(b *testing.B) {
	terms := makeTerms(4096)
	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				idx := impl.new()
				for i, term := range terms {
					if err := idx.Insert(term, fts.DocOrd(i)); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkBuildPositionalSingletonTerms(b *testing.B) {
	terms := makeTerms(4096)
	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				idx := impl.new()
				for i, term := range terms {
					if err := idx.InsertAt(term, uint32(i%20), fts.DocOrd(i)); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkBuildZipfPostings(b *testing.B) {
	terms := makeTerms(2048)
	type operation struct {
		term string
		ord  fts.DocOrd
		pos  uint32
	}
	ops := make([]operation, 0, 20_000)
	rng := rand.New(rand.NewSource(42))
	zipf := rand.NewZipf(rng, 1.07, 1, uint64(len(terms)-1))
	for doc := range 1000 {
		for pos := range 20 {
			ops = append(ops, operation{
				term: terms[zipf.Uint64()],
				ord:  fts.DocOrd(doc),
				pos:  uint32(pos),
			})
		}
	}

	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				idx := impl.new()
				for _, op := range ops {
					if err := idx.InsertAt(op.term, op.pos, op.ord); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkSearchSingletonTerms(b *testing.B) {
	terms := makeTerms(4096)
	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			idx := impl.new()
			for i, term := range terms {
				if err := idx.Insert(term, fts.DocOrd(i)); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			i := 0
			for b.Loop() {
				if _, err := idx.Search(terms[i%len(terms)]); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

func BenchmarkSearchPositionalPostings(b *testing.B) {
	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			idx := impl.new()
			for doc := range 4096 {
				ord := fts.DocOrd(doc)
				if err := idx.InsertAt("shared-term", 1, ord); err != nil {
					b.Fatal(err)
				}
				if err := idx.InsertAt("shared-term", 7, ord); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				positionalResult, err = idx.SearchPositional("shared-term")
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func makeTerms(n int) []string {
	terms := make([]string, n)
	for i := range terms {
		terms[i] = fmt.Sprintf("term-%06d", i)
	}
	return terms
}
