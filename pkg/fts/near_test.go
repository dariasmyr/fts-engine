package fts

import (
	"context"
	"testing"
)

func TestSearchPhraseNearMatchesOrderedWindow(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "james hussein doe",
		"doc-b": "doe james",
		"doc-c": "james speech today now doe",
		"doc-d": "james doe james x doe",
	}
	for id, content := range docs {
		if err := indexDefaultDoc(ctx, svc, DocID(id), content); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}

	res, err := svc.SearchPhraseNear(ctx, "james doe", 1, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}

	hits := map[DocID]int{}
	for _, r := range res.Results {
		hits[r.ID] = r.TotalMatches
	}

	if got := hits["doc-d"]; got != 2 {
		t.Fatalf("doc-d TotalMatches = %d, want 2", got)
	}
	if got := hits["doc-a"]; got != 1 {
		t.Fatalf("doc-a TotalMatches = %d, want 1", got)
	}
	if _, ok := hits["doc-b"]; ok {
		t.Fatalf("doc-b should not match because order is reversed, got %+v", res.Results)
	}
	if _, ok := hits["doc-c"]; ok {
		t.Fatalf("doc-c should not match because distance is too large, got %+v", res.Results)
	}
	if len(res.Results) < 2 || res.Results[0].ID != "doc-d" {
		t.Fatalf("expected doc-d to rank first, got %+v", res.Results)
	}
}

func TestSearchPhraseNearDistanceZeroMatchesAdjacency(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "james doe james x doe"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "james doe", 0, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].TotalMatches != 1 {
		t.Fatalf("unexpected phrase near results: %+v", res.Results)
	}
}

func TestSearchPhraseNearRejectsNegativeDistance(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	_, err := svc.SearchPhraseNear(ctx, "james doe", -1, 10)
	if err == nil {
		t.Fatalf("expected negative distance error")
	}
}

func TestSearchPhraseNearSkipsNonPositionalIndexes(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "james doe"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "james doe", 1, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("len(Results) = %d, want 0", len(res.Results))
	}
}

func TestSearchPhraseNearMergesMultipleKeys(t *testing.T) {
	keyGen := func(token string) ([]string, error) {
		return []string{token, token + "-alt"}, nil
	}
	svc := New(newPositionalMemoryIndex(), keyGen)

	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "james x doe"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "james doe", 1, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].TotalMatches != 1 {
		t.Fatalf("unexpected phrase near results: %+v", res.Results)
	}
}

func TestSearchPhraseNearMatchesThreeTokenWindow(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "james x doe speech",
		"doc-b": "james x y doe speech",
		"doc-c": "james doe x speech",
		"doc-d": "speech james x doe",
		"doc-e": "james x doe speech james doe x speech",
	}
	for id, content := range docs {
		if err := indexDefaultDoc(ctx, svc, DocID(id), content); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}

	res, err := svc.SearchPhraseNear(ctx, "james doe speech", 1, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}

	hits := map[DocID]int{}
	for _, r := range res.Results {
		hits[r.ID] = r.TotalMatches
	}

	if got := hits["doc-e"]; got != 2 {
		t.Fatalf("doc-e TotalMatches = %d, want 2", got)
	}
	if got := hits["doc-a"]; got != 1 {
		t.Fatalf("doc-a TotalMatches = %d, want 1", got)
	}
	if got := hits["doc-c"]; got != 1 {
		t.Fatalf("doc-c TotalMatches = %d, want 1", got)
	}
	if _, ok := hits["doc-b"]; ok {
		t.Fatalf("doc-b should not match because first gap is too large, got %+v", res.Results)
	}
	if _, ok := hits["doc-d"]; ok {
		t.Fatalf("doc-d should not match because the sequence is incomplete, got %+v", res.Results)
	}
	if len(res.Results) < 3 || res.Results[0].ID != "doc-e" {
		t.Fatalf("expected doc-e to rank first, got %+v", res.Results)
	}
}

func TestSearchPhraseNearDistanceZeroMatchesExactPhrase(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "james doe speech james x doe speech"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "james doe speech", 0, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].TotalMatches != 1 {
		t.Fatalf("unexpected phrase near results: %+v", res.Results)
	}
}

func TestSearchPhraseNearSingleTokenFallsBackToSearch(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "hello world hello"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "hello", 3, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" || res.Results[0].TotalMatches != 2 {
		t.Fatalf("unexpected phrase near results: %+v", res.Results)
	}
}
