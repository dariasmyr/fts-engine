package fts

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
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
		if err := svc.Index(ctx, defaultDoc(DocID(fmt.Sprintf("doc-%d", doc)), string(buf))); err != nil {
			b.Fatalf("Index() error = %v", err)
		}
	}
	return svc, words
}

func BenchmarkServiceIndex(b *testing.B) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	ctx := context.Background()
	content := "alpha beta gamma delta epsilon zeta eta theta iota kappa"

	for i := 0; b.Loop(); i++ {
		_ = svc.Index(ctx, defaultDoc(DocID(fmt.Sprintf("d-%d", i)), content))
	}
}

func BenchmarkServiceSearchTextSingleTerm(b *testing.B) {
	svc, words := benchSvc(b, 500, 40, 200)
	ctx := context.Background()
	query := words[0]

	for b.Loop() {
		_, err := svc.SearchText(ctx, query, SearchOptions{MaxResults: 10})
		if err != nil {
			b.Fatalf("SearchText() error = %v", err)
		}
	}
}

func BenchmarkServiceSearchTextMultiTerm(b *testing.B) {
	svc, words := benchSvc(b, 500, 40, 200)
	ctx := context.Background()
	query := words[0] + " " + words[1] + " " + words[2]

	for b.Loop() {
		_, err := svc.SearchText(ctx, query, SearchOptions{MaxResults: 10})
		if err != nil {
			b.Fatalf("SearchText() error = %v", err)
		}
	}
}

func BenchmarkServiceSearchTextMiss(b *testing.B) {
	svc, _ := benchSvc(b, 500, 40, 200)
	ctx := context.Background()

	for b.Loop() {
		_, err := svc.SearchText(ctx, "nonexistentword", SearchOptions{MaxResults: 10})
		if err != nil {
			b.Fatalf("SearchText() error = %v", err)
		}
	}
}

func BenchmarkServiceCandidateFiltering(b *testing.B) {
	const documentCount = 10_000
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	postings := make([]Posting, documentCount)
	ids := make([]DocID, documentCount)
	for i := range documentCount {
		id := DocID(fmt.Sprintf("doc-%05d", i))
		ord := svc.registry.GetOrAssign(id)
		ids[i] = id
		postings[i] = Posting{Ord: ord, Count: 1}
	}
	idx.entries["common"] = postings
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		candidates []DocID
		want       int
	}{
		{name: "unrestricted", candidates: nil, want: documentCount},
		{name: "sparse", candidates: []DocID{ids[0], ids[100], ids[5000], ids[9999]}, want: 4},
		{name: "dense", candidates: ids, want: documentCount},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				res, err := svc.SearchQuery(ctx, TermQuery{Term: "common"}, SearchOptions{
					CandidateIDs: tc.candidates,
				})
				if err != nil {
					b.Fatal(err)
				}
				if len(res.Results) != tc.want {
					b.Fatalf("results = %d, want %d", len(res.Results), tc.want)
				}
			}
		})
	}
}

func BenchmarkServiceCandidatePushdown(b *testing.B) {
	const documentCount = 10_000
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)
	postings := make([]Posting, documentCount)
	ids := make([]DocID, documentCount)
	for i := range documentCount {
		id := DocID(fmt.Sprintf("doc-%05d", i))
		ord := svc.registry.GetOrAssign(id)
		ids[i] = id
		postings[i] = Posting{Ord: ord, Count: 1}
	}
	idx.entries["common"] = postings
	candidateIDs := []DocID{ids[0], ids[100], ids[5000], ids[9999]}
	candidateSet := make(map[DocID]struct{}, len(candidateIDs))
	for _, id := range candidateIDs {
		candidateSet[id] = struct{}{}
	}
	ctx := context.Background()

	b.Run("external_filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			res, err := svc.SearchQuery(ctx, TermQuery{Term: "common"}, SearchOptions{})
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for _, result := range res.Results {
				if _, ok := candidateSet[result.ID]; ok {
					count++
				}
			}
			if count != len(candidateIDs) {
				b.Fatalf("results = %d, want %d", count, len(candidateIDs))
			}
		}
	})

	b.Run("candidate_pushdown", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			res, err := svc.SearchQuery(ctx, TermQuery{Term: "common"}, SearchOptions{
				CandidateIDs: candidateIDs,
			})
			if err != nil {
				b.Fatal(err)
			}
			if len(res.Results) != len(candidateIDs) {
				b.Fatalf("results = %d, want %d", len(res.Results), len(candidateIDs))
			}
		}
	})
}

func BenchmarkServiceSealedCandidatePushdown(b *testing.B) {
	const documentCount = 10_000
	postings := make([]Posting, documentCount)
	ids := make([]DocID, documentCount)
	for i := range documentCount {
		ids[i] = DocID(fmt.Sprintf("doc-%05d", i))
		postings[i] = Posting{Ord: DocOrd(i), Count: 1}
	}
	data, err := ftse.Build([]posting.TermData{{Term: "common", Postings: postings}})
	if err != nil {
		b.Fatal(err)
	}
	reader, err := ftse.Open(data)
	if err != nil {
		b.Fatal(err)
	}
	svc := New(reader, WordKeys, WithDocRegistrySnapshot(ids))
	candidateIDs := []DocID{ids[0], ids[100], ids[5000], ids[9999]}
	candidateSet := make(map[DocID]struct{}, len(candidateIDs))
	for _, id := range candidateIDs {
		candidateSet[id] = struct{}{}
	}
	ctx := context.Background()

	b.Run("external_filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			res, err := svc.SearchQuery(ctx, TermQuery{Term: "common"}, SearchOptions{})
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for _, result := range res.Results {
				if _, ok := candidateSet[result.ID]; ok {
					count++
				}
			}
			if count != len(candidateIDs) {
				b.Fatalf("results = %d, want %d", count, len(candidateIDs))
			}
		}
	})

	b.Run("candidate_pushdown", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			res, err := svc.SearchQuery(ctx, TermQuery{Term: "common"}, SearchOptions{
				CandidateIDs: candidateIDs,
			})
			if err != nil {
				b.Fatal(err)
			}
			if len(res.Results) != len(candidateIDs) {
				b.Fatalf("results = %d, want %d", len(res.Results), len(candidateIDs))
			}
		}
	})
}
