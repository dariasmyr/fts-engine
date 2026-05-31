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
	want := "fts: index document \"doc-1\": field \"title\" is not available in single-field mode; use \"_default\" or fts.NewMultiField"
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
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
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["alpha"] = refsForIDs(registry, namedPosting{"a", 2})
	body.entries["beta"] = refsForIDs(registry, namedPosting{"b", 3}, namedPosting{"a", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

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
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["alpha"] = refsForIDs(registry, namedPosting{"doc-1", 1})
	body.entries["alpha"] = refsForIDs(registry, namedPosting{"doc-1", 2})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

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
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["alpha"] = refsForIDs(registry, namedPosting{"a", 1})
	body.entries["alpha"] = refsForIDs(registry, namedPosting{"b", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchDocuments(context.Background(), "title:alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if res.TotalResultsCount != 1 || res.Results[0].ID != "a" {
		t.Fatalf("expected only title hit, got %+v", res.Results)
	}
}

func TestSearchFieldRestrictsUnscopedQueryToOneField(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["alpha"] = refsForIDs(registry, namedPosting{"a", 1})
	body.entries["alpha"] = refsForIDs(registry, namedPosting{"b", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchField(context.Background(), "title", "alpha", 10)
	if err != nil {
		t.Fatalf("SearchField() error = %v", err)
	}
	if res.TotalResultsCount != 1 || len(res.Results) != 1 || res.Results[0].ID != "a" {
		t.Fatalf("expected only title hit, got %+v", res.Results)
	}
}

func TestSearchFieldPreservesExplicitFieldScope(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["alpha"] = refsForIDs(registry, namedPosting{"a", 1})
	body.entries["alpha"] = refsForIDs(registry, namedPosting{"b", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchField(context.Background(), "title", "body:alpha", 10)
	if err != nil {
		t.Fatalf("SearchField() error = %v", err)
	}
	if res.TotalResultsCount != 1 || len(res.Results) != 1 || res.Results[0].ID != "b" {
		t.Fatalf("expected explicit body scope to be preserved, got %+v", res.Results)
	}
	if res.Results[0].ID == "a" {
		t.Fatalf("default field binding overwrote explicit scope: %+v", res.Results)
	}
}

func TestSearchFieldsRestrictsUnscopedQueryToSubset(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["alpha"] = refsForIDs(registry, namedPosting{"a", 1})
	body.entries["alpha"] = refsForIDs(registry, namedPosting{"b", 1})
	tags := newMemoryIndex()
	tags.entries["alpha"] = refsForIDs(registry, namedPosting{"c", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
		"tags":  tags,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchFields(context.Background(), []string{"title", "body"}, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchFields() error = %v", err)
	}
	if len(res.Results) != 2 {
		t.Fatalf("expected subset hits from title/body only, got %+v", res.Results)
	}
	ids := map[DocID]bool{}
	for _, result := range res.Results {
		ids[result.ID] = true
	}
	if !ids["a"] || !ids["b"] || ids["c"] {
		t.Fatalf("expected only title/body results, got %+v", res.Results)
	}
}

func TestSearchFieldsRestrictsExplicitFieldToSubset(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["alpha"] = refsForIDs(registry, namedPosting{"a", 1})
	body.entries["alpha"] = refsForIDs(registry, namedPosting{"b", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchFields(context.Background(), []string{"title"}, "body:alpha", 10)
	if err != nil {
		t.Fatalf("SearchFields() error = %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("expected explicit field outside subset to be excluded, got %+v", res.Results)
	}
}

func TestSearchQueryFieldsRestrictsASTQueryToSubset(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["barack"] = refsForIDs(registry, namedPosting{"doc-1", 1})
	body.entries["obama"] = refsForIDs(registry, namedPosting{"doc-1", 1}, namedPosting{"doc-2", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	q := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "barack"}),
		MustClause(TermQuery{Term: "obama"}),
	}}

	res, err := svc.SearchQueryFields(context.Background(), []string{"title"}, q, 10)
	if err != nil {
		t.Fatalf("SearchQueryFields() error = %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("expected body clause to be excluded by subset scope, got %+v", res.Results)
	}
}

func TestSearchFieldClausesCombinesDifferentQueriesAcrossFields(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["barack"] = refsForIDs(registry, namedPosting{"doc-1", 1}, namedPosting{"doc-2", 1})
	body.entries["obama"] = refsForIDs(registry, namedPosting{"doc-3", 1}, namedPosting{"doc-2", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchFieldClauses(context.Background(), []FieldQueryClause{
		MustFieldQuery("title", "barack"),
		MustFieldQuery("body", "obama"),
	}, 10)
	if err != nil {
		t.Fatalf("SearchFieldClauses() error = %v", err)
	}
	if res.TotalResultsCount != 1 || len(res.Results) != 1 || res.Results[0].ID != "doc-2" {
		t.Fatalf("expected only doc-2 after field-specific MUST clauses, got %+v", res.Results)
	}
}

func TestSearchFieldClausesSupportsFieldSpecificPhraseAndExclusion(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{
		"title": {Value: "barack"},
		"body":  {Value: "french hotel"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{
		"title": {Value: "barack"},
		"body":  {Value: "market hotel"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchFieldClauses(ctx, []FieldQueryClause{
		MustFieldQuery("title", "barack"),
		MustFieldQuery("body", `"french hotel"`),
		MustNotFieldQuery("body", "market"),
	}, 10)
	if err != nil {
		t.Fatalf("SearchFieldClauses() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("expected only doc-a after field-specific phrase and exclusion, got %+v", res.Results)
	}
}

func TestSearchDocumentsMustAcrossFieldsIntersectsByDocID(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["barack"] = refsForIDs(registry, namedPosting{"doc-1", 1}, namedPosting{"doc-2", 1})
	body.entries["obama"] = refsForIDs(registry, namedPosting{"doc-3", 1}, namedPosting{"doc-2", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchDocuments(context.Background(), "+title:barack +body:obama", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if res.TotalResultsCount != 1 || len(res.Results) != 1 || res.Results[0].ID != "doc-2" {
		t.Fatalf("expected only doc-2 after cross-field MUST intersection, got %+v", res.Results)
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

func TestSearchPhraseFieldRestrictsToOneField(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "barack obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "barack obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseField(ctx, "title", "barack obama", 10)
	if err != nil {
		t.Fatalf("SearchPhraseField() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("expected only title phrase hit, got %+v", res.Results)
	}
}

func TestSearchPhraseFieldsRestrictsToSubset(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "barack obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "barack obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseFields(ctx, []string{"title"}, "barack obama", 10)
	if err != nil {
		t.Fatalf("SearchPhraseFields() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("expected only title phrase hit, got %+v", res.Results)
	}
}

func TestSearchPhraseNearFieldRestrictsToOneField(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "barack x obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "barack x obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseNearField(ctx, "title", "barack obama", 1, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNearField() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("expected only title near-phrase hit, got %+v", res.Results)
	}
}

func TestSearchPhraseNearFieldsRestrictsToSubset(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "barack x obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "barack x obama"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseNearFields(ctx, []string{"title"}, "barack obama", 1, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNearFields() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("expected only title near-phrase hit, got %+v", res.Results)
	}
}
