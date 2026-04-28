package fts

import (
	"context"
	"sort"
	"strings"
	"testing"
)

type execPrefixPositionalMemoryIndex struct {
	*positionalMemoryIndex
}

func newExecPrefixPositionalMemoryIndex() *execPrefixPositionalMemoryIndex {
	return &execPrefixPositionalMemoryIndex{positionalMemoryIndex: newPositionalMemoryIndex()}
}

func (p *execPrefixPositionalMemoryIndex) SearchPrefix(prefix string) ([]DocRef, error) {
	merged := make(map[DocID]uint32)
	for key, docs := range p.postings {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, doc := range docs {
			merged[doc.ID] += doc.Count
		}
	}

	out := make([]DocRef, 0, len(merged))
	for id, count := range merged {
		out = append(out, DocRef{ID: id, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func buildExecService(t *testing.T) *Service {
	t.Helper()
	factory := func(name string) (Index, error) { return newExecPrefixPositionalMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)
	ctx := context.Background()
	seed := map[string]map[string]string{
		"doc-a": {"title": "barack obama", "body": "speech at inauguration"},
		"doc-b": {"title": "banana split", "body": "barack said banana is tasty"},
		"doc-c": {"title": "russia", "body": "barack visited russia"},
		"doc-d": {"title": "mars rover", "body": "barack likes mars exploration"},
	}
	for id, fields := range seed {
		docFields := make(map[string]Field, len(fields))
		for name, value := range fields {
			docFields[name] = Field{Value: value}
		}
		if err := svc.Index(ctx, Document{ID: DocID(id), Fields: docFields}); err != nil {
			t.Fatalf("Index(%s) error = %v", id, err)
		}
	}
	return svc
}

func TestSearchAPITermQuery(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.Search(context.Background(), TermQuery{Term: "barack"}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 4 {
		t.Fatalf("len(results) = %d, want 4", len(res.Results))
	}
}

func TestSearchAPIFieldScopedTerm(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.Search(context.Background(), TermQuery{Field: "title", Term: "barack"}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("results = %+v, want only doc-a", res.Results)
	}
}

func TestSearchAPIPhraseQuery(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.Search(context.Background(), PhraseQuery{Phrase: "barack obama"}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("results = %+v, want only doc-a", res.Results)
	}
}

func TestSearchAPIPrefixQuery(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.Search(context.Background(), PrefixQuery{Field: "title", Prefix: "ba"}, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	ids := make(map[DocID]bool, len(res.Results))
	for _, r := range res.Results {
		ids[r.ID] = true
	}
	if !ids["doc-a"] || !ids["doc-b"] || ids["doc-c"] || ids["doc-d"] {
		t.Fatalf("unexpected prefix results: %+v", res.Results)
	}
}

func TestSearchAPIBooleanMustIntersects(t *testing.T) {
	svc := buildExecService(t)
	q := &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "barack"}),
		MustClause(TermQuery{Term: "russia"}),
	}}

	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-c" {
		t.Fatalf("results = %+v, want only doc-c", res.Results)
	}
}

func TestSearchAPIBooleanMustNotExcludes(t *testing.T) {
	svc := buildExecService(t)
	q := &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "barack"}),
		MustNotClause(TermQuery{Term: "russia"}),
	}}

	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	ids := make(map[DocID]bool, len(res.Results))
	for _, r := range res.Results {
		ids[r.ID] = true
	}
	if ids["doc-c"] || !ids["doc-a"] || !ids["doc-b"] || !ids["doc-d"] {
		t.Fatalf("unexpected results: %+v", res.Results)
	}
}

func TestSearchAPINilQueryReturnsEmpty(t *testing.T) {
	svc := buildExecService(t)

	res, err := svc.Search(context.Background(), nil, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) != 0 {
		t.Fatalf("results = %+v, want empty", res.Results)
	}
}

func TestSearchAPIParsedQuery(t *testing.T) {
	svc := buildExecService(t)
	q, err := ParseQuery(`+barack -russia "barack obama"`)
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}

	res, err := svc.Search(context.Background(), q, 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(res.Results) == 0 || res.Results[0].ID != "doc-a" {
		t.Fatalf("results = %+v, want doc-a first", res.Results)
	}
	for _, r := range res.Results {
		if r.ID == "doc-c" {
			t.Fatalf("doc-c should be excluded: %+v", res.Results)
		}
	}
}
