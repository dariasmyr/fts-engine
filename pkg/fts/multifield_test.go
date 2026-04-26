package fts

import (
	"context"
	"testing"
)

func TestIndexMultiFieldPopulatesPerFieldIndex(t *testing.T) {
	factories := map[string]*memoryIndex{}
	factory := func(name string) (Index, error) {
		idx := newMemoryIndex()
		factories[name] = idx
		return idx, nil
	}

	svc := NewMultiField(factory, WordKeys)

	doc := Document{
		ID: "doc-1",
		Fields: map[string]Field{
			"title": {Value: "rosa"},
			"body":  {Value: "barge"},
		},
	}
	if err := svc.Index(context.Background(), doc); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	if got := len(factories["title"].inserts); got != 1 {
		t.Fatalf("title inserts = %d, want 1", got)
	}
	if got := len(factories["body"].inserts); got != 1 {
		t.Fatalf("body inserts = %d, want 1", got)
	}
	if factories["title"].inserts[0].key != "rosa" {
		t.Fatalf("title got key %q, want %q", factories["title"].inserts[0].key, "rosa")
	}
}

func TestIndexDocumentStillWorksAsSugar(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	if err := svc.IndexDocument(context.Background(), "doc-1", "alpha"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}
	if len(idx.inserts) != 1 || idx.inserts[0].key != "alpha" {
		t.Fatalf("legacy IndexDocument did not populate default field: %+v", idx.inserts)
	}

	fields := svc.Fields()
	if len(fields) != 1 || fields[0] != DefaultField {
		t.Fatalf("Fields() = %v, want [%s]", fields, DefaultField)
	}
}

func TestSingleFieldServiceRejectsOtherFields(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	err := svc.Index(context.Background(), Document{
		ID:     "doc-1",
		Fields: map[string]Field{"title": {Value: "oops"}},
	})
	if err == nil {
		t.Fatal("expected error for unknown field on single-field service")
	}
}

type uppercasePipeline struct{}

func (uppercasePipeline) Process(text string) []string {
	out := make([]rune, 0, len(text))
	for _, r := range text {
		if r >= 'a' && r <= 'z' {
			out = append(out, r-32)
		} else if r >= 'A' && r <= 'Z' {
			out = append(out, r)
		}
	}
	return []string{string(out)}
}

func TestPerFieldPipelineOverridesDefault(t *testing.T) {
	idx := newMemoryIndex()
	svc := NewMultiField(
		func(name string) (Index, error) { return idx, nil },
		WordKeys,
	)

	err := svc.Index(context.Background(), Document{
		ID: "doc-1",
		Fields: map[string]Field{
			"title": {Value: "abc", Pipeline: uppercasePipeline{}},
		},
	})
	if err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	if len(idx.inserts) != 1 || idx.inserts[0].key != "ABC" {
		t.Fatalf("per-field pipeline not applied: %+v", idx.inserts)
	}
}

func TestSearchDocumentsAcrossFields(t *testing.T) {
	title := newMemoryIndex()
	title.entries["alpha"] = []DocRef{{ID: "a", Count: 2}}
	body := newMemoryIndex()
	body.entries["beta"] = []DocRef{{ID: "b", Count: 3}, {ID: "a", Count: 1}}

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "alpha beta", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if res.TotalResultsCount < 2 {
		t.Fatalf("expected matches across both fields, got %+v", res.Results)
	}

	ids := map[DocID]bool{}
	for _, r := range res.Results {
		ids[r.ID] = true
	}
	if !ids["a"] || !ids["b"] {
		t.Fatalf("expected both 'a' and 'b' in results, got %+v", res.Results)
	}
}

func TestSearchDocumentsDedupsSameTokenAcrossFields(t *testing.T) {
	title := newMemoryIndex()
	title.entries["alpha"] = []DocRef{{ID: "doc-1", Count: 1}}
	body := newMemoryIndex()
	body.entries["alpha"] = []DocRef{{ID: "doc-1", Count: 2}}

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 1 {
		t.Fatalf("expected one result, got %+v", res.Results)
	}
	if got := res.Results[0]; got.ID != "doc-1" || got.UniqueMatches != 1 || got.TotalMatches != 3 {
		t.Fatalf("result = %+v, want doc-1 UniqueMatches=1 TotalMatches=3", got)
	}
}

func TestSearchDocumentsFieldScopedTerm(t *testing.T) {
	title := newMemoryIndex()
	title.entries["alpha"] = []DocRef{{ID: "a", Count: 1}}
	body := newMemoryIndex()
	body.entries["alpha"] = []DocRef{{ID: "b", Count: 1}}

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "title:alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if res.TotalResultsCount != 1 || res.Results[0].ID != "a" {
		t.Fatalf("expected only title hit, got %+v", res.Results)
	}
}

func TestSearchDocumentsQuotedPhraseAcrossFields(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "barack obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "barack obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchDocuments(ctx, `"barack obama"`, 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected phrase matches in both fields, got %+v", res.Results)
	}
}

func TestSearchPhraseNearAcrossFields(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "barack x obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "barack x obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "barack obama", 1, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNear() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected near matches in both fields, got %+v", res.Results)
	}
}
