package fts

import (
	"context"
	"sort"
	"testing"
)

type positionalMemoryIndex struct {
	postings  map[string][]DocRef
	positions map[string]map[DocID][]uint32
}

func newPositionalMemoryIndex() *positionalMemoryIndex {
	return &positionalMemoryIndex{
		postings:  make(map[string][]DocRef),
		positions: make(map[string]map[DocID][]uint32),
	}
}

func (p *positionalMemoryIndex) Insert(key string, id DocID) error {
	p.bumpCount(key, id)
	return nil
}

func (p *positionalMemoryIndex) InsertAt(key string, id DocID, pos uint32) error {
	p.bumpCount(key, id)
	if _, ok := p.positions[key]; !ok {
		p.positions[key] = make(map[DocID][]uint32)
	}
	ps := append(p.positions[key][id], pos)
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	p.positions[key][id] = ps
	return nil
}

func (p *positionalMemoryIndex) bumpCount(key string, id DocID) {
	entries := p.postings[key]
	for i := range entries {
		if entries[i].ID == id {
			entries[i].Count++
			p.postings[key] = entries
			return
		}
	}
	p.postings[key] = append(entries, DocRef{ID: id, Count: 1})
}

func (p *positionalMemoryIndex) Search(key string) ([]DocRef, error) {
	return p.postings[key], nil
}

func (p *positionalMemoryIndex) SearchPositional(key string) ([]PositionalDocRef, error) {
	entries := p.postings[key]
	out := make([]PositionalDocRef, 0, len(entries))
	for _, entry := range entries {
		out = append(out, PositionalDocRef{
			ID:        entry.ID,
			Positions: p.positions[key][entry.ID],
		})
	}
	return out, nil
}

func TestSearchPhraseMatchesExactOrder(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "barack obama gave a speech",
		"doc-b": "obama speech today barack was there",
		"doc-c": "barack obama said barack obama again",
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}

	res, err := svc.SearchPhrase(ctx, "barack obama", 10)
	if err != nil {
		t.Fatalf("SearchPhrase() error = %v", err)
	}

	hits := map[DocID]int{}
	for _, r := range res.Results {
		hits[r.ID] = r.TotalMatches
	}

	if _, ok := hits["doc-a"]; !ok {
		t.Fatalf("expected doc-a to match, got %+v", res.Results)
	}
	if _, ok := hits["doc-b"]; ok {
		t.Fatalf("doc-b should not match, got %+v", res.Results)
	}
	if got := hits["doc-c"]; got != 2 {
		t.Fatalf("doc-c TotalMatches = %d, want 2", got)
	}
}

func TestSearchPhraseSingleTokenFallsBackToSearch(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "hello world"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchPhrase(ctx, "hello", 10)
	if err != nil {
		t.Fatalf("SearchPhrase() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("unexpected phrase results: %+v", res.Results)
	}
}

func TestSearchPhraseEmptyQuery(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	_ = svc.IndexDocument(ctx, "doc-a", "hello world")

	res, err := svc.SearchPhrase(ctx, "   ", 10)
	if err != nil {
		t.Fatalf("SearchPhrase() error = %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("len(Results) = %d, want 0", len(res.Results))
	}
}

func TestSearchPhraseSkipsNonPositionalIndexes(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "barack obama"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchPhrase(ctx, "barack obama", 10)
	if err != nil {
		t.Fatalf("SearchPhrase() error = %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("len(Results) = %d, want 0", len(res.Results))
	}
}

func TestSearchPhraseMergesMultipleKeys(t *testing.T) {
	keyGen := func(token string) ([]string, error) {
		return []string{token, token + "-alt"}, nil
	}
	svc := New(newPositionalMemoryIndex(), keyGen)

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "barack obama barack obama"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchPhrase(ctx, "barack obama", 10)
	if err != nil {
		t.Fatalf("SearchPhrase() error = %v", err)
	}
	if len(res.Results) != 1 {
		t.Fatalf("len(Results) = %d, want 1", len(res.Results))
	}
	if got := res.Results[0].TotalMatches; got != 2 {
		t.Fatalf("TotalMatches = %d, want 2", got)
	}
}
