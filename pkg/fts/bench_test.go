package fts

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func benchSvc(b *testing.B, docs int, wordsPerDoc int, vocab int) (*Service, []string) {
	b.Helper()
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	rng := rand.New(rand.NewSource(42))
	words := make([]string, vocab)
	for i := range words {
		words[i] = fmt.Sprintf("word%04d", i)
	}

	ctx := context.Background()
	for doc := 0; doc < docs; doc++ {
		buf := make([]byte, 0, wordsPerDoc*10)
		for word := 0; word < wordsPerDoc; word++ {
			if word > 0 {
				buf = append(buf, ' ')
			}
			buf = append(buf, words[rng.Intn(vocab)]...)
		}
		if err := svc.IndexDocument(ctx, DocID(fmt.Sprintf("doc-%d", doc)), string(buf)); err != nil {
			b.Fatalf("IndexDocument() error = %v", err)
		}
	}
	return svc, words
}

func BenchmarkServiceIndexDocument(b *testing.B) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	ctx := context.Background()
	content := "alpha beta gamma delta epsilon zeta eta theta iota kappa"

	for i := 0; b.Loop(); i++ {
		_ = svc.IndexDocument(ctx, DocID(fmt.Sprintf("d-%d", i)), content)
	}
}

func BenchmarkServiceSearchDocumentsSingleTerm(b *testing.B) {
	svc, words := benchSvc(b, 500, 40, 200)
	ctx := context.Background()
	query := words[0]

	for b.Loop() {
		_, err := svc.SearchDocuments(ctx, query, 10)
		if err != nil {
			b.Fatalf("SearchDocuments() error = %v", err)
		}
	}
}

func BenchmarkServiceSearchDocumentsMultiTerm(b *testing.B) {
	svc, words := benchSvc(b, 500, 40, 200)
	ctx := context.Background()
	query := words[0] + " " + words[1] + " " + words[2]

	for b.Loop() {
		_, err := svc.SearchDocuments(ctx, query, 10)
		if err != nil {
			b.Fatalf("SearchDocuments() error = %v", err)
		}
	}
}

func BenchmarkServiceSearchDocumentsMiss(b *testing.B) {
	svc, _ := benchSvc(b, 500, 40, 200)
	ctx := context.Background()

	for b.Loop() {
		_, err := svc.SearchDocuments(ctx, "nonexistentword", 10)
		if err != nil {
			b.Fatalf("SearchDocuments() error = %v", err)
		}
	}
}
