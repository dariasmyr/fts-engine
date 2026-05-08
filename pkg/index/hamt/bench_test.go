package hamt

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

func benchmarkIndex(b *testing.B, terms int, docs int) *Index {
	b.Helper()
	idx := New()
	rng := rand.New(rand.NewSource(42))
	words := make([]string, terms)
	for i := range words {
		words[i] = fmt.Sprintf("word%04d", i)
	}
	for doc := 0; doc < docs; doc++ {
		docID := fts.DocID(fmt.Sprintf("doc-%d", doc))
		for j := 0; j < 20; j++ {
			if err := idx.Insert(words[rng.Intn(len(words))], docID); err != nil {
				b.Fatalf("Insert() error = %v", err)
			}
		}
	}
	return idx
}

func benchmarkPositionalIndex(b *testing.B, terms int, docs int) *Index {
	b.Helper()
	idx := New()
	rng := rand.New(rand.NewSource(42))
	words := make([]string, terms)
	for i := range words {
		words[i] = fmt.Sprintf("word%04d", i)
	}
	for doc := 0; doc < docs; doc++ {
		docID := fts.DocID(fmt.Sprintf("doc-%d", doc))
		for pos := 0; pos < 20; pos++ {
			if err := idx.InsertAt(words[rng.Intn(len(words))], docID, uint32(pos)); err != nil {
				b.Fatalf("InsertAt() error = %v", err)
			}
		}
	}
	return idx
}

func BenchmarkInsert(b *testing.B) {
	idx := New()
	for i := 0; b.Loop(); i++ {
		if err := idx.Insert(fmt.Sprintf("word%06d", i), fts.DocID(fmt.Sprintf("doc-%d", i))); err != nil {
			b.Fatalf("Insert() error = %v", err)
		}
	}
}

func BenchmarkInsertAt(b *testing.B) {
	idx := New()
	for i := 0; b.Loop(); i++ {
		if err := idx.InsertAt(fmt.Sprintf("word%06d", i), fts.DocID(fmt.Sprintf("doc-%d", i)), uint32(i%20)); err != nil {
			b.Fatalf("InsertAt() error = %v", err)
		}
	}
}

func BenchmarkSearch(b *testing.B) {
	idx := benchmarkIndex(b, 500, 500)
	query := "word0001"

	b.ResetTimer()
	for b.Loop() {
		_, err := idx.Search(query)
		if err != nil {
			b.Fatalf("Search() error = %v", err)
		}
	}
}

func BenchmarkSearchPositional(b *testing.B) {
	idx := benchmarkPositionalIndex(b, 500, 500)
	query := "word0001"

	b.ResetTimer()
	for b.Loop() {
		_, err := idx.SearchPositional(query)
		if err != nil {
			b.Fatalf("SearchPositional() error = %v", err)
		}
	}
}
