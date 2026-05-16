package fts_test

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
)

func TestPropertyAndFastPathMatchesSetSemantics(t *testing.T) {
	for _, tt := range propertyIndexCases() {
		t.Run(tt.name, func(t *testing.T) {
			const iterations = 40
			base := rand.New(rand.NewSource(2026))
			vocab := propertyVocab(30)

			for i := 0; i < iterations; i++ {
				seed := base.Int63()
				corpus := propertyCorpus(seed, 200, vocab)
				svc := buildPropertyService(t, tt.newIndex(), corpus)
				a, b := pickTwoPresentPropertyTerms(seed, corpus, vocab)

				q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
					fts.MustClause(fts.TermQuery{Term: a}),
					fts.MustClause(fts.TermQuery{Term: b}),
				}}
				res, err := svc.Search(context.Background(), q, 1000)
				if err != nil {
					t.Fatalf("iter=%d seed=%d Search() error = %v", i, seed, err)
				}

				got := propertyIDSet(res)
				want := make(map[fts.DocID]bool)
				for id, tokens := range corpus {
					if propertyContains(tokens, a) && propertyContains(tokens, b) {
						want[id] = true
					}
				}
				if !propertySetsEqual(got, want) {
					t.Fatalf("iter=%d seed=%d a=%q b=%q got=%v want=%v", i, seed, a, b, propertySortedKeys(got), propertySortedKeys(want))
				}
			}
		})
	}
}

func TestPropertyOrFastPathMatchesSetSemantics(t *testing.T) {
	for _, tt := range propertyIndexCases() {
		t.Run(tt.name, func(t *testing.T) {
			const iterations = 40
			base := rand.New(rand.NewSource(4077))
			vocab := propertyVocab(30)

			for i := 0; i < iterations; i++ {
				seed := base.Int63()
				corpus := propertyCorpus(seed, 200, vocab)
				svc := buildPropertyService(t, tt.newIndex(), corpus)
				a, b := pickTwoPresentPropertyTerms(seed, corpus, vocab)

				q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
					fts.ShouldClause(fts.TermQuery{Term: a}),
					fts.ShouldClause(fts.TermQuery{Term: b}),
				}}
				res, err := svc.Search(context.Background(), q, 10000)
				if err != nil {
					t.Fatalf("iter=%d seed=%d Search() error = %v", i, seed, err)
				}

				got := propertyIDSet(res)
				want := make(map[fts.DocID]bool)
				for id, tokens := range corpus {
					if propertyContains(tokens, a) || propertyContains(tokens, b) {
						want[id] = true
					}
				}
				if !propertySetsEqual(got, want) {
					t.Fatalf("iter=%d seed=%d a=%q b=%q got=%v want=%v", i, seed, a, b, propertySortedKeys(got), propertySortedKeys(want))
				}
			}
		})
	}
}

func TestPropertyWandSubsetOfFullOr(t *testing.T) {
	for _, tt := range propertyIndexCases() {
		t.Run(tt.name, func(t *testing.T) {
			const iterations = 40
			base := rand.New(rand.NewSource(9001))
			vocab := propertyVocab(30)

			for i := 0; i < iterations; i++ {
				seed := base.Int63()
				corpus := propertyCorpus(seed, 200, vocab)
				svc := buildPropertyService(t, tt.newIndex(), corpus)
				a, b := pickTwoPresentPropertyTerms(seed, corpus, vocab)

				q := &fts.BooleanQuery{Clauses: []fts.BoolClause{
					fts.ShouldClause(fts.TermQuery{Term: a}),
					fts.ShouldClause(fts.TermQuery{Term: b}),
				}}

				full, err := svc.Search(context.Background(), q, 10000)
				if err != nil {
					t.Fatalf("iter=%d seed=%d full Search() error = %v", i, seed, err)
				}
				fullSet := propertyIDSet(full)

				top, err := svc.Search(context.Background(), q, 5)
				if err != nil {
					t.Fatalf("iter=%d seed=%d top Search() error = %v", i, seed, err)
				}
				if len(top.Results) > 5 {
					t.Fatalf("iter=%d seed=%d len(top.Results) = %d, want <= 5", i, seed, len(top.Results))
				}
				for _, r := range top.Results {
					if !fullSet[r.ID] {
						t.Fatalf("iter=%d seed=%d top result %q not present in full result set", i, seed, r.ID)
					}
				}
			}
		})
	}
}

type propertyIndexCase struct {
	name     string
	newIndex func() fts.Index
}

func propertyIndexCases() []propertyIndexCase {
	return []propertyIndexCase{
		{name: "slicedradix", newIndex: func() fts.Index { return slicedradix.New() }},
		{name: "hamt", newIndex: func() fts.Index { return hamt.New() }},
	}
}

func propertyVocab(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("t%03d", i)
	}
	return out
}

func propertyCorpus(seed int64, nDocs int, vocab []string) map[fts.DocID][]string {
	rng := rand.New(rand.NewSource(seed))
	out := make(map[fts.DocID][]string, nDocs)
	for i := 0; i < nDocs; i++ {
		id := fts.DocID(fmt.Sprintf("d%04d", i))
		nTokens := 1 + rng.Intn(10)
		seen := make(map[string]struct{}, nTokens)
		tokens := make([]string, 0, nTokens)
		for len(tokens) < nTokens {
			word := vocab[rng.Intn(len(vocab))]
			if _, ok := seen[word]; ok {
				continue
			}
			seen[word] = struct{}{}
			tokens = append(tokens, word)
		}
		out[id] = tokens
	}
	return out
}

func buildPropertyService(t *testing.T, index fts.Index, corpus map[fts.DocID][]string) *fts.Service {
	t.Helper()
	svc := fts.New(index, fts.WordKeys, fts.WithScorer(fts.BM25()))
	ids := make([]fts.DocID, 0, len(corpus))
	for id := range corpus {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	ctx := context.Background()
	for _, id := range ids {
		if err := svc.IndexDocument(ctx, id, strings.Join(corpus[id], " ")); err != nil {
			t.Fatalf("IndexDocument(%q) error = %v", id, err)
		}
	}
	return svc
}

func pickTwoPresentPropertyTerms(seed int64, corpus map[fts.DocID][]string, vocab []string) (string, string) {
	seen := map[string]bool{}
	for _, tokens := range corpus {
		for _, token := range tokens {
			seen[token] = true
		}
	}
	present := make([]string, 0, len(seen))
	for token := range seen {
		present = append(present, token)
	}
	sort.Strings(present)
	if len(present) < 2 {
		return vocab[0], vocab[1]
	}

	rng := rand.New(rand.NewSource(seed ^ 0xbeef))
	a := present[rng.Intn(len(present))]
	b := present[rng.Intn(len(present))]
	for a == b {
		b = present[rng.Intn(len(present))]
	}
	return a, b
}

func propertyContains(tokens []string, token string) bool {
	return slices.Contains(tokens, token)
}

func propertyIDSet(res *fts.SearchResult) map[fts.DocID]bool {
	out := make(map[fts.DocID]bool, len(res.Results))
	for _, r := range res.Results {
		out[r.ID] = true
	}
	return out
}

func propertySetsEqual(a, b map[fts.DocID]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}

func propertySortedKeys(m map[fts.DocID]bool) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, string(id))
	}
	sort.Strings(out)
	return out
}
