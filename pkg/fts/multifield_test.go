package fts

import (
	"context"
	"strings"
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

	if err := indexDefaultDoc(context.Background(), svc, "doc-1", "alpha"); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
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

func (uppercasePipeline) Descriptor() PipelineDescriptor {
	descriptor, err := NewPipelineDescriptor("uppercase-test", testFingerprint("uppercase-test-v1"))
	if err != nil {
		panic(err)
	}
	return descriptor
}

func (uppercasePipeline) Process(text string) []string {
	words := strings.Fields(text)
	for i := range words {
		words[i] = strings.ToUpper(words[i])
	}
	return words
}

type fixedOutputPipeline struct {
	name   string
	tokens []string
}

func (pipeline *fixedOutputPipeline) Descriptor() PipelineDescriptor {
	descriptor, err := NewPipelineDescriptor(pipeline.name, testFingerprint(strings.Join(pipeline.tokens, "\x00")))
	if err != nil {
		panic(err)
	}
	return descriptor
}

func (pipeline *fixedOutputPipeline) Process(string) []string {
	return append([]string(nil), pipeline.tokens...)
}

func TestConfiguredFieldPipelineOverridesDefault(t *testing.T) {
	idx := newMemoryIndex()
	svc := NewMultiField(
		func(name string) (Index, error) { return idx, nil },
		WordKeys,
		WithFieldPipelines(map[string]Pipeline{"title": uppercasePipeline{}}),
	)

	err := svc.Index(context.Background(), Document{
		ID: "doc-1",
		Fields: map[string]Field{
			"title": {Value: "abc"},
		},
	})
	if err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	if len(idx.inserts) != 1 || idx.inserts[0].key != "ABC" {
		t.Fatalf("configured field pipeline not applied: %+v", idx.inserts)
	}
}

func TestIndexRejectsChangedConfiguredPipeline(t *testing.T) {
	first, _ := NewPipelineDescriptor("same", testFingerprint("first"))
	second, _ := NewPipelineDescriptor("same", testFingerprint("second"))
	pipeline := &customPipeline{descriptor: first}
	svc := NewMultiField(
		func(string) (Index, error) { return newMemoryIndex(), nil },
		WordKeys,
		WithFieldPipelines(map[string]Pipeline{"title": pipeline}),
	)

	if err := svc.Index(context.Background(), Document{ID: "doc-1", Fields: map[string]Field{
		"title": {Value: "alpha"},
	}}); err != nil {
		t.Fatalf("first Index() error = %v", err)
	}
	pipeline.descriptor = second
	if err := svc.Index(context.Background(), Document{ID: "doc-2", Fields: map[string]Field{
		"title": {Value: "beta"},
	}}); err == nil {
		t.Fatal("second Index() error = nil")
	}
}

func TestSearchRejectsChangedConfiguredPipeline(t *testing.T) {
	pipeline := &fixedOutputPipeline{name: "mutable", tokens: []string{"first"}}
	svc := New(newMemoryIndex(), WordKeys, WithPipeline(pipeline))
	if err := svc.Index(context.Background(), Document{ID: "doc-1", Fields: map[string]Field{
		DefaultField: {Value: "ignored"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	pipeline.tokens = []string{"second"}
	if _, err := svc.Search(context.Background(), TermQuery{Term: "ignored"}, 10); err == nil {
		t.Fatal("Search() error = nil after pipeline fingerprint changed")
	}
}

func TestPhraseUsesPhraseScoringAcrossPipelineGroups(t *testing.T) {
	single := &fixedOutputPipeline{name: "single", tokens: []string{"joined"}}
	split := &fixedOutputPipeline{name: "split", tokens: []string{"alpha", "beta"}}
	svc := NewMultiField(
		func(string) (Index, error) { return newExecPrefixPositionalMemoryIndex(), nil },
		WordKeys,
		WithPipeline(split),
		WithFieldPipelines(map[string]Pipeline{"title": single}),
		WithScorer(WeightedScorer{
			Base:             fixedScorer(1),
			QueryTypeWeights: QueryTypeWeights{Term: 2, Phrase: 5},
		}),
	)
	if err := svc.Index(context.Background(), Document{ID: "doc-1", Fields: map[string]Field{
		"title": {Value: "ignored"},
		"body":  {Value: "ignored"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.Search(WithDiagnostics(context.Background()), PhraseQuery{Phrase: "ignored"}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].Score != 10 {
		t.Fatalf("Results = %+v, want one result with phrase score 10", res.Results)
	}
	if res.Diagnostics == nil || res.Diagnostics.ProcessedTokens != 3 {
		t.Fatalf("Diagnostics = %+v, want ProcessedTokens=3", res.Diagnostics)
	}
}

func TestSearchUsesConfiguredPipelinePerField(t *testing.T) {
	factory := func(string) (Index, error) { return newExecPrefixPositionalMemoryIndex(), nil }
	svc := NewMultiField(
		factory,
		WordKeys,
		WithFieldPipelines(map[string]Pipeline{"title": uppercasePipeline{}}),
	)
	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "title-doc", Fields: map[string]Field{
		"title": {Value: "Alpha Beta"},
	}}); err != nil {
		t.Fatalf("Index(title-doc) error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "body-doc", Fields: map[string]Field{
		"body": {Value: "alpha beta"},
	}}); err != nil {
		t.Fatalf("Index(body-doc) error = %v", err)
	}

	tests := []struct {
		name  string
		query Query
	}{
		{name: "term", query: TermQuery{Term: "alpha"}},
		{name: "single-token phrase", query: PhraseQuery{Phrase: "alpha"}},
		{name: "phrase", query: PhraseQuery{Phrase: "alpha beta"}},
		{name: "prefix", query: PrefixQuery{Prefix: "al"}},
		{name: "boolean fast path", query: &BooleanQuery{Clauses: []BoolClause{
			MustClause(TermQuery{Term: "alpha"}),
			MustClause(TermQuery{Term: "beta"}),
		}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := svc.Search(ctx, test.query, 10)
			if err != nil {
				t.Fatalf("Search() error = %v", err)
			}
			if res.TotalResultsCount != 2 {
				t.Fatalf("TotalResultsCount = %d, want 2; results=%+v", res.TotalResultsCount, res.Results)
			}
		})
	}

	res, err := svc.Search(ctx, TermQuery{Field: "title", Term: "alpha"}, 10)
	if err != nil {
		t.Fatalf("Search(title) error = %v", err)
	}
	if res.TotalResultsCount != 1 || res.Results[0].ID != "title-doc" {
		t.Fatalf("title results = %+v, want title-doc", res.Results)
	}

	near, err := svc.SearchPhraseNearFields(ctx, []string{"title", "body"}, "alpha beta", 0, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNearFields() error = %v", err)
	}
	if near.TotalResultsCount != 2 {
		t.Fatalf("near TotalResultsCount = %d, want 2; results=%+v", near.TotalResultsCount, near.Results)
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

func TestSearchFieldClausesCombinesDifferentQueriesAcrossFields(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["james"] = refsForIDs(registry, namedPosting{"doc-1", 1}, namedPosting{"doc-2", 1})
	body.entries["doe"] = refsForIDs(registry, namedPosting{"doc-3", 1}, namedPosting{"doc-2", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchFieldClauses(context.Background(), []FieldQueryClause{
		MustFieldQuery("title", "james"),
		MustFieldQuery("body", "doe"),
	}, 10)
	if err != nil {
		t.Fatalf("SearchFieldClauses() error = %v", err)
	}
	if res.TotalResultsCount != 1 || len(res.Results) != 1 || res.Results[0].ID != "doc-2" {
		t.Fatalf("expected only doc-2 after field-specific MUST clauses, got %+v", res.Results)
	}
}

func TestSearchFieldClausesSupportsShouldClauses(t *testing.T) {
	title := newMemoryIndex()
	body := newMemoryIndex()
	registry := NewDocRegistry()
	title.entries["james"] = refsForIDs(registry, namedPosting{"doc-1", 1}, namedPosting{"doc-2", 1})
	body.entries["doe"] = refsForIDs(registry, namedPosting{"doc-3", 1}, namedPosting{"doc-2", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchFieldClauses(context.Background(), []FieldQueryClause{
		ShouldFieldQuery("title", "james"),
		ShouldFieldQuery("body", "doe"),
	}, 10)
	if err != nil {
		t.Fatalf("SearchFieldClauses() error = %v", err)
	}
	if res.TotalResultsCount != 3 || len(res.Results) != 3 {
		t.Fatalf("expected union of SHOULD field clauses, got %+v", res.Results)
	}
	if res.Results[0].ID != "doc-2" {
		t.Fatalf("expected doc-2 to rank first with two field matches, got %+v", res.Results)
	}
}

func TestSearchFieldClausesSupportsFieldSpecificPhraseAndExclusion(t *testing.T) {
	factory := func(name string) (Index, error) { return newPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{
		"title": {Value: "james"},
		"body":  {Value: "french hotel"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{
		"title": {Value: "james"},
		"body":  {Value: "market hotel"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchFieldClauses(ctx, []FieldQueryClause{
		MustFieldQuery("title", "james"),
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
	title.entries["james"] = refsForIDs(registry, namedPosting{"doc-1", 1}, namedPosting{"doc-2", 1})
	body.entries["doe"] = refsForIDs(registry, namedPosting{"doc-3", 1}, namedPosting{"doc-2", 1})

	svc := NewMultiFieldFromIndexes(map[string]Index{
		"title": title,
		"body":  body,
	}, WordKeys, WithDocRegistrySnapshot(registry.Snapshot()))

	res, err := svc.SearchDocuments(context.Background(), "+title:james +body:doe", 10)
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
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "james doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "james doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchDocuments(ctx, `"james doe"`, 10)
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
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "james x doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "james x doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseNear(ctx, "james doe", 1, 10)
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
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "james doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "james doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseField(ctx, "title", "james doe", 10)
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
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "james doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "james doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseFields(ctx, []string{"title"}, "james doe", 10)
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
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "james x doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "james x doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseNearField(ctx, "title", "james doe", 1, 10)
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
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{"title": {Value: "james x doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-b", Fields: map[string]Field{"body": {Value: "james x doe"}}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	res, err := svc.SearchPhraseNearFields(ctx, []string{"title"}, "james doe", 1, 10)
	if err != nil {
		t.Fatalf("SearchPhraseNearFields() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("expected only title near-phrase hit, got %+v", res.Results)
	}
}
