package flat

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

func benchmarkIndex(b *testing.B, terms, docs int) *Index {
	b.Helper()
	idx := New()
	rng := rand.New(rand.NewSource(42))
	words := make([]string, terms)
	for i := range words {
		words[i] = fmt.Sprintf("word%04d", i)
	}
	for doc := range docs {
		for range 20 {
			if err := idx.Insert(words[rng.Intn(len(words))], fts.DocOrd(doc)); err != nil {
				b.Fatalf("Insert() error = %v", err)
			}
		}
	}
	return idx
}

func benchmarkPositionalIndex(b *testing.B, terms, docs int) *Index {
	b.Helper()
	idx := New()
	rng := rand.New(rand.NewSource(42))
	words := make([]string, terms)
	for i := range words {
		words[i] = fmt.Sprintf("word%04d", i)
	}
	for doc := range docs {
		for pos := range 20 {
			if err := idx.InsertAt(words[rng.Intn(len(words))], uint32(pos), fts.DocOrd(doc)); err != nil {
				b.Fatalf("InsertAt() error = %v", err)
			}
		}
	}
	return idx
}

func benchmarkBuildInput() []string {
	const postingCount = 10_000
	words := make([]string, postingCount)
	for i := range words {
		words[i] = fmt.Sprintf("word%04d", i%1_000)
	}
	return words
}

func BenchmarkBuild(b *testing.B) {
	words := benchmarkBuildInput()
	b.ReportAllocs()
	b.ReportMetric(float64(len(words)), "postings/op")
	for b.Loop() {
		idx := New()
		for i, word := range words {
			if err := idx.Insert(word, fts.DocOrd(i/20)); err != nil {
				b.Fatalf("Insert() error = %v", err)
			}
		}
	}
}

func BenchmarkBuildPositional(b *testing.B) {
	words := benchmarkBuildInput()
	b.ReportAllocs()
	b.ReportMetric(float64(len(words)), "postings/op")
	for b.Loop() {
		idx := New()
		for i, word := range words {
			if err := idx.InsertAt(word, uint32(i%20), fts.DocOrd(i/20)); err != nil {
				b.Fatalf("InsertAt() error = %v", err)
			}
		}
	}
}

func BenchmarkSearch(b *testing.B) {
	idx := benchmarkIndex(b, 500, 500)
	b.ResetTimer()
	for b.Loop() {
		if _, err := idx.Search("word0001"); err != nil {
			b.Fatalf("Search() error = %v", err)
		}
	}
}

func BenchmarkSearchPositional(b *testing.B) {
	idx := benchmarkPositionalIndex(b, 500, 500)
	b.ResetTimer()
	for b.Loop() {
		if _, err := idx.SearchPositional("word0001"); err != nil {
			b.Fatalf("SearchPositional() error = %v", err)
		}
	}
}

func BenchmarkSearchPrefix(b *testing.B) {
	b.Run("first", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			idx := benchmarkIndex(b, 500, 500)
			b.StartTimer()
			if _, err := idx.SearchPrefix("word00"); err != nil {
				b.Fatalf("SearchPrefix() error = %v", err)
			}
		}
	})
	b.Run("warm", func(b *testing.B) {
		idx := benchmarkIndex(b, 500, 500)
		if _, err := idx.SearchPrefix("word00"); err != nil {
			b.Fatalf("SearchPrefix() warmup error = %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if _, err := idx.SearchPrefix("word00"); err != nil {
				b.Fatalf("SearchPrefix() error = %v", err)
			}
		}
	})
}
