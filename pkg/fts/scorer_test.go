package fts

import (
	"context"
	"sort"
	"strings"
	"testing"
)

type prefixMemoryIndex struct {
	entries map[string][]DocRef
}

func newPrefixMemoryIndex() *prefixMemoryIndex {
	return &prefixMemoryIndex{entries: make(map[string][]DocRef)}
}

func (p *prefixMemoryIndex) Insert(key string, ord DocOrd) error {
	entries := p.entries[key]
	for i := range entries {
		if entries[i].Ord == ord {
			entries[i].Count++
			p.entries[key] = entries
			return nil
		}
	}
	p.entries[key] = append(entries, DocRef{Ord: ord, Count: 1, Seq: uint32(ord)})
	return nil
}

func (p *prefixMemoryIndex) Search(key string) ([]DocRef, error) {
	return p.entries[key], nil
}

func (p *prefixMemoryIndex) SearchPrefix(prefix string) ([]DocRef, error) {
	merged := make(map[DocOrd]uint32)
	for key, docs := range p.entries {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, doc := range docs {
			merged[doc.Ord] += doc.Count
		}
	}

	out := make([]DocRef, 0, len(merged))
	for ord, count := range merged {
		out = append(out, DocRef{Ord: ord, Count: count, Seq: uint32(ord)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ord < out[j].Ord })
	return out, nil
}

func TestBM25RareTermScoresHigherThanCommon(t *testing.T) {
	scorer := BM25()

	fieldStats := FieldStats{N: 1000, AvgLength: 20}
	doc := DocStats{Ord: 1, Length: 20}

	rare := scorer.Score(TermStats{Term: "rosa", TF: 1, DF: 3}, doc, fieldStats)
	common := scorer.Score(TermStats{Term: "the", TF: 1, DF: 900}, doc, fieldStats)

	if rare <= common {
		t.Fatalf("expected rare term score > common term score, got rare=%v common=%v", rare, common)
	}
}

func TestBM25LengthNormalization(t *testing.T) {
	scorer := BM25()
	fieldStats := FieldStats{N: 100, AvgLength: 50}
	term := TermStats{Term: "x", TF: 2, DF: 10}

	short := scorer.Score(term, DocStats{Ord: 1, Length: 10}, fieldStats)
	long := scorer.Score(term, DocStats{Ord: 2, Length: 200}, fieldStats)

	if short <= long {
		t.Fatalf("expected shorter document to score higher, got short=%v long=%v", short, long)
	}
}

func TestTFIDFMonotonicInTF(t *testing.T) {
	scorer := TFIDF()
	fieldStats := FieldStats{N: 100, AvgLength: 10}

	low := scorer.Score(TermStats{Term: "alpha", TF: 1, DF: 5}, DocStats{Ord: 1, Length: 10}, fieldStats)
	high := scorer.Score(TermStats{Term: "alpha", TF: 10, DF: 5}, DocStats{Ord: 1, Length: 10}, fieldStats)

	if high <= low {
		t.Fatalf("expected TF-IDF to increase with TF, got low=%v high=%v", low, high)
	}
}

func TestSearchWithBM25RanksRareDocumentFirst(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys, WithScorer(BM25()))

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "rosa barge",
		"doc-b": "barge barge barge",
	}
	for _, id := range []string{"doc-c", "doc-d", "doc-e", "doc-f", "doc-g", "doc-h", "doc-i", "doc-j"} {
		docs[id] = "barge"
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("IndexDocument(%q) error = %v", id, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, "rosa barge", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) < 2 {
		t.Fatalf("expected at least 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("expected doc-a to rank first, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected scores to be non-increasing, got %v then %v", res.Results[0].Score, res.Results[1].Score)
	}
}

func TestSearchWithTFIDFRanksRareDocumentFirst(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys, WithScorer(TFIDF()))

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "rosa barge",
		"doc-b": "barge barge barge",
	}
	for _, id := range []string{"doc-c", "doc-d", "doc-e", "doc-f", "doc-g", "doc-h", "doc-i", "doc-j"} {
		docs[id] = "barge"
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("IndexDocument(%q) error = %v", id, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, "rosa barge", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) < 2 {
		t.Fatalf("expected at least 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("expected doc-a to rank first, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected scores to be non-increasing, got %v then %v", res.Results[0].Score, res.Results[1].Score)
	}
}

func TestBooleanScoringAppliesShouldBoost(t *testing.T) {
	idx := newMemoryIndex()

	svc := New(idx, WordKeys, WithScorer(TFIDF()))
	idx.entries["alpha"] = refsForIDs(svc.registry, namedPosting{"doc-a", 1}, namedPosting{"doc-b", 1}, namedPosting{"doc-c", 1})
	idx.entries["beta"] = refsForIDs(svc.registry, namedPosting{"doc-a", 1}, namedPosting{"doc-b", 1})
	idx.entries["delta"] = refsForIDs(svc.registry, namedPosting{"doc-b", 1}, namedPosting{"doc-d", 1})
	for _, doc := range []struct {
		id     DocID
		length uint32
	}{
		{id: "doc-a", length: 2},
		{id: "doc-b", length: 3},
		{id: "doc-c", length: 1},
		{id: "doc-d", length: 1},
	} {
		svc.collection.observe(DefaultField, svc.registry.GetOrAssign(doc.id), doc.length)
	}

	q := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "alpha"}),
		MustClause(TermQuery{Term: "beta"}),
		ShouldClause(TermQuery{Term: "delta"}),
	}}

	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 results, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-b" {
		t.Fatalf("expected doc-b to rank first after SHOULD boost, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected boosted score to be higher, got %+v", res.Results)
	}
}

func TestSearchPhraseWithTFIDFScoresAcrossFields(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys, WithScorer(TFIDF()))

	ctx := context.Background()
	docs := []Document{
		{ID: "doc-a", Fields: map[string]Field{"title": {Value: "barack obama"}}},
		{ID: "doc-b", Fields: map[string]Field{"body": {Value: "barack obama barack obama"}}},
		{ID: "doc-c", Fields: map[string]Field{"title": {Value: "speech only"}}},
		{ID: "doc-d", Fields: map[string]Field{"body": {Value: "speech only"}}},
	}
	for _, doc := range docs {
		if err := svc.Index(ctx, doc); err != nil {
			t.Fatalf("Index(%q) error = %v", doc.ID, err)
		}
	}

	res, err := svc.SearchDocuments(ctx, `"barack obama"`, 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 phrase hits, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-b" {
		t.Fatalf("expected doc-b to rank first by phrase TF, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected scores to be non-increasing, got %+v", res.Results)
	}
}

func TestSearchWithPrefixScoringRanksHigherFrequencyFirst(t *testing.T) {
	svc := New(newPrefixMemoryIndex(), WordKeys, WithScorer(TFIDF()))

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "bar barge"); err != nil {
		t.Fatalf("IndexDocument(doc-a) error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-b", "bar"); err != nil {
		t.Fatalf("IndexDocument(doc-b) error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-c", "hotel"); err != nil {
		t.Fatalf("IndexDocument(doc-c) error = %v", err)
	}

	res, err := svc.SearchDocuments(ctx, "bar*", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected 2 prefix hits, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-a" {
		t.Fatalf("expected doc-a to rank first by prefix TF, got %+v", res.Results)
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("expected scores to be non-increasing, got %+v", res.Results)
	}
}

func TestSearchScoreStaysZeroWithoutScorer(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "alpha beta"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchDocuments(ctx, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 1 {
		t.Fatalf("expected 1 result, got %+v", res.Results)
	}
	if res.Results[0].Score != 0 {
		t.Fatalf("expected zero score without scorer, got %v", res.Results[0].Score)
	}
}
