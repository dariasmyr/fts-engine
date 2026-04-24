package fts

import (
	"context"
	"testing"
)

func TestSearchPhraseNearMatchesOrderedWindow(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	docs := map[string]string{
		"doc-a": "barack hussein obama",
		"doc-b": "obama barack",
		"doc-c": "barack speech today now obama",
		"doc-d": "barack obama barack x obama",
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}

	res, err := svc.SearchPhraseNear(ctx, "barack obama", 1, 10)
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
	if err := svc.IndexDocument(ctx, "doc-a", "barack obama barack x obama"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "barack obama", 0, 10)
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
	_, err := svc.SearchPhraseNear(ctx, "barack obama", -1, 10)
	if err == nil {
		t.Fatalf("expected negative distance error")
	}
}

func TestSearchPhraseNearSkipsNonPositionalIndexes(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "barack obama"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "barack obama", 1, 10)
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
	if err := svc.IndexDocument(ctx, "doc-a", "barack x obama"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "barack obama", 1, 10)
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
		"doc-a": "barack x obama speech",
		"doc-b": "barack x y obama speech",
		"doc-c": "barack obama x speech",
		"doc-d": "speech barack x obama",
		"doc-e": "barack x obama speech barack obama x speech",
	}
	for id, content := range docs {
		if err := svc.IndexDocument(ctx, DocID(id), content); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}

	res, err := svc.SearchPhraseNear(ctx, "barack obama speech", 1, 10)
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
	if err := svc.IndexDocument(ctx, "doc-a", "barack obama speech barack x obama speech"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "barack obama speech", 0, 10)
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
	if err := svc.IndexDocument(ctx, "doc-a", "hello world hello"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "hello", 3, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" || res.Results[0].TotalMatches != 2 {
		t.Fatalf("unexpected phrase near results: %+v", res.Results)
	}
}
