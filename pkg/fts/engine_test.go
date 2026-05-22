package fts

import (
	"context"
	"errors"
	"testing"
)

type memoryIndex struct {
	entries map[string][]DocRef
	inserts []struct {
		key string
		id  DocID
		ord DocOrd
	}
	searches []string
}

func newMemoryIndex() *memoryIndex {
	return &memoryIndex{entries: make(map[string][]DocRef)}
}

func (m *memoryIndex) Insert(key string, id DocID, ord ...DocOrd) error {
	assigned := DocOrd(0)
	if len(ord) > 0 {
		assigned = ord[0]
	}
	m.inserts = append(m.inserts, struct {
		key string
		id  DocID
		ord DocOrd
	}{key: key, id: id, ord: assigned})
	return nil
}

func (m *memoryIndex) Search(key string) ([]DocRef, error) {
	m.searches = append(m.searches, key)
	return m.entries[key], nil
}

type containsOnlyFilter struct {
	allowed map[string]bool
}

func (f containsOnlyFilter) Add(item []byte) bool { return true }

func (f containsOnlyFilter) Contains(item []byte) bool {
	return f.allowed[string(item)]
}

type buildableContainsFilter struct {
	containsOnlyFilter
	built bool
}

type rejectingFilter struct{}

func (rejectingFilter) Add(item []byte) bool { return false }

func (rejectingFilter) Contains(item []byte) bool { return true }

func (f *buildableContainsFilter) Build() error {
	f.built = true
	return nil
}

func TestSearchDocumentsSortAndLimit(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "a", Count: 3}, {ID: "b", Count: 1}}
	idx.entries["beta"] = []DocRef{{ID: "a", Count: 1}, {ID: "c", Count: 5}}

	svc := New(idx, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "alpha beta", 2)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	if res.TotalResultsCount != 3 {
		t.Fatalf("TotalResultsCount = %d, want 3", res.TotalResultsCount)
	}
	if len(res.Results) != 2 {
		t.Fatalf("len(Results) = %d, want 2", len(res.Results))
	}

	if res.Results[0].ID != "a" {
		t.Fatalf("results[0].ID = %q, want %q", res.Results[0].ID, "a")
	}
	if res.Results[1].ID != "c" {
		t.Fatalf("results[1].ID = %q, want %q", res.Results[1].ID, "c")
	}
}

func TestSearchDocumentsTieBreakerByID(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["token"] = []DocRef{{ID: "z", Count: 2}, {ID: "b", Count: 2}}

	svc := New(idx, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "token", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	if len(res.Results) != 2 {
		t.Fatalf("len(Results) = %d, want 2", len(res.Results))
	}

	if res.Results[0].ID != "b" || res.Results[1].ID != "z" {
		t.Fatalf("unexpected order: %+v", res.Results)
	}
}

func TestSearchDocumentsDiagnosticsDisabledByDefault(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["one"] = []DocRef{{ID: "x", Count: 1}}

	svc := New(idx, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "one", 1)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if res.Diagnostics != nil {
		t.Fatalf("expected nil diagnostics by default, got %+v", res.Diagnostics)
	}
}

func TestSearchDocumentsReturnsDiagnosticsTimings(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["one"] = []DocRef{{ID: "x", Count: 1}}

	svc := New(idx, WordKeys)

	res, err := svc.SearchDocuments(WithDiagnostics(context.Background()), "one", 1)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if res.Diagnostics == nil {
		t.Fatal("expected non-nil diagnostics")
	}

	if !res.Diagnostics.Timings.HasPreprocess() || res.Diagnostics.Timings.Preprocess <= 0 {
		t.Fatalf("expected positive preprocess timing, got %+v", res.Diagnostics.Timings)
	}
	if !res.Diagnostics.Timings.HasSearchTokens() || res.Diagnostics.Timings.SearchTokens <= 0 {
		t.Fatalf("expected positive search_tokens timing, got %+v", res.Diagnostics.Timings)
	}
	if !res.Diagnostics.Timings.HasTotal() || res.Diagnostics.Timings.Total <= 0 {
		t.Fatalf("expected positive total timing, got %+v", res.Diagnostics.Timings)
	}
}

func TestIndexDocumentUsesKeyGenerator(t *testing.T) {
	idx := newMemoryIndex()
	keyGen := func(token string) ([]string, error) {
		return []string{token, token + "-alt"}, nil
	}

	svc := New(idx, keyGen)

	err := svc.IndexDocument(context.Background(), "doc-1", "Alpha")
	if err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	if len(idx.inserts) != 2 {
		t.Fatalf("insert count = %d, want 2", len(idx.inserts))
	}
}

func TestSearchDocumentsDedupsUniqueMatchesWithinTokenClause(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-1", Count: 1}, {ID: "doc-2", Count: 1}}
	idx.entries["alpha-alt"] = []DocRef{{ID: "doc-1", Count: 2}, {ID: "doc-3", Count: 1}}

	keyGen := func(token string) ([]string, error) {
		return []string{token, token + "-alt"}, nil
	}
	svc := New(idx, keyGen)

	res, err := svc.SearchDocuments(context.Background(), "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 3 {
		t.Fatalf("len(Results) = %d, want 3", len(res.Results))
	}

	hits := map[DocID]Result{}
	for _, item := range res.Results {
		hits[item.ID] = item
	}

	if got := hits["doc-1"]; got.UniqueMatches != 1 || got.TotalMatches != 3 {
		t.Fatalf("doc-1 = %+v, want UniqueMatches=1 TotalMatches=3", got)
	}
	if got := hits["doc-2"]; got.UniqueMatches != 1 || got.TotalMatches != 1 {
		t.Fatalf("doc-2 = %+v, want UniqueMatches=1 TotalMatches=1", got)
	}
	if got := hits["doc-3"]; got.UniqueMatches != 1 || got.TotalMatches != 1 {
		t.Fatalf("doc-3 = %+v, want UniqueMatches=1 TotalMatches=1", got)
	}
}

func TestSearchDocumentsCountsSeparateTokensIndependently(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-1", Count: 1}}
	idx.entries["alpha-alt"] = []DocRef{{ID: "doc-1", Count: 1}}
	idx.entries["beta"] = []DocRef{{ID: "doc-1", Count: 2}}
	idx.entries["beta-alt"] = []DocRef{{ID: "doc-1", Count: 3}}

	keyGen := func(token string) ([]string, error) {
		return []string{token, token + "-alt"}, nil
	}
	svc := New(idx, keyGen)

	res, err := svc.SearchDocuments(context.Background(), "alpha beta", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 1 {
		t.Fatalf("len(Results) = %d, want 1", len(res.Results))
	}
	if got := res.Results[0]; got.UniqueMatches != 2 || got.TotalMatches != 7 {
		t.Fatalf("result = %+v, want UniqueMatches=2 TotalMatches=7", got)
	}
}

func TestSearchDocumentsMustTermsIntersect(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 0}, {ID: "doc-c", Count: 1, Seq: 2}}
	idx.entries["beta"] = []DocRef{{ID: "doc-b", Count: 1, Seq: 1}, {ID: "doc-c", Count: 3, Seq: 2}}

	svc := New(idx, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "+alpha +beta", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 1 {
		t.Fatalf("len(Results) = %d, want 1", len(res.Results))
	}
	if got := res.Results[0]; got.ID != "doc-c" || got.UniqueMatches != 2 || got.TotalMatches != 4 {
		t.Fatalf("result = %+v, want doc-c UniqueMatches=2 TotalMatches=4", got)
	}
}

func TestSearchDocumentsMustNotExcludesMatches(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["alpha"] = []DocRef{{ID: "doc-a", Count: 1, Seq: 0}, {ID: "doc-b", Count: 1, Seq: 1}}
	idx.entries["beta"] = []DocRef{{ID: "doc-b", Count: 1, Seq: 1}}

	svc := New(idx, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "alpha -beta", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("unexpected results: %+v", res.Results)
	}
}

func TestSearchDocumentsQuotedPhraseUsesPhraseQuery(t *testing.T) {
	svc := New(newPositionalMemoryIndex(), WordKeys)

	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "barack obama"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-b", "obama barack"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	res, err := svc.SearchDocuments(ctx, `"barack obama"`, 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-a" {
		t.Fatalf("unexpected phrase results: %+v", res.Results)
	}
}

func TestIndexDocumentReturnsErrorWhenFilterAddFails(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys, WithFilter(rejectingFilter{}))

	err := svc.IndexDocument(context.Background(), "doc-1", "Alpha")
	if err == nil {
		t.Fatal("IndexDocument() error = nil, want filter add failure")
	}
}

func TestContextCancellation(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := svc.IndexDocument(ctx, "doc-1", "text"); !errors.Is(err, context.Canceled) {
		t.Fatalf("IndexDocument() err = %v, want context canceled", err)
	}

	_, err := svc.SearchDocuments(ctx, "text", 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchDocuments() err = %v, want context canceled", err)
	}
}

func TestSearchDocumentsSkipsIndexWhenFilterMisses(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["known"] = []DocRef{{ID: "doc", Count: 1}}

	svc := New(idx, WordKeys, WithFilter(containsOnlyFilter{
		allowed: map[string]bool{"known": true},
	}))

	res, err := svc.SearchDocuments(context.Background(), "unknown", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	if res.TotalResultsCount != 0 {
		t.Fatalf("TotalResultsCount = %d, want 0", res.TotalResultsCount)
	}

	if len(idx.searches) != 0 {
		t.Fatalf("index search calls = %d, want 0", len(idx.searches))
	}
}

func TestSearchDocumentsDoesNotAutoBuildBuildableFilter(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["known"] = []DocRef{{ID: "doc", Count: 1}}

	filter := &buildableContainsFilter{
		containsOnlyFilter: containsOnlyFilter{allowed: map[string]bool{"known": true}},
	}

	svc := New(idx, WordKeys, WithFilter(filter))

	if _, err := svc.SearchDocuments(context.Background(), "known", 10); err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	if filter.built {
		t.Fatal("Build() was called during search, want explicit finalize only")
	}
}

func TestSearchUsesBufferedStaticFilterAfterManualBuild(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["known"] = []DocRef{{ID: "doc", Count: 1}}

	static := &testStaticFilter{}
	filter := NewBufferedStaticFilter(static)

	svc := New(idx, WordKeys, WithFilter(filter))

	if err := svc.IndexDocument(context.Background(), "doc-1", "known"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	resBefore, err := svc.SearchDocuments(context.Background(), "known", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() before finalize error = %v", err)
	}
	if resBefore.TotalResultsCount != 1 {
		t.Fatalf("TotalResultsCount before finalize = %d, want 1", resBefore.TotalResultsCount)
	}
	if static.builds != 0 {
		t.Fatalf("static builds before finalize = %d, want 0", static.builds)
	}

	if err := filter.Build(); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if static.builds != 1 {
		t.Fatalf("static builds after Build() = %d, want 1", static.builds)
	}

	resAfter, err := svc.SearchDocuments(context.Background(), "known", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() after Build() error = %v", err)
	}
	if resAfter.TotalResultsCount != 1 {
		t.Fatalf("TotalResultsCount after Build() = %d, want 1", resAfter.TotalResultsCount)
	}
}

func TestBuildFilterBuildsBuildableFilter(t *testing.T) {
	idx := newMemoryIndex()
	filter := &buildableContainsFilter{}

	svc := New(idx, WordKeys, WithFilter(filter))

	if err := svc.BuildFilter(); err != nil {
		t.Fatalf("BuildFilter() error = %v", err)
	}

	if !filter.built {
		t.Fatal("BuildFilter() did not call Build()")
	}
}

func TestBuildFilterSkipsNonBuildableFilter(t *testing.T) {
	idx := newMemoryIndex()
	svc := New(idx, WordKeys, WithFilter(containsOnlyFilter{allowed: map[string]bool{"known": true}}))

	if err := svc.BuildFilter(); err != nil {
		t.Fatalf("BuildFilter() error = %v", err)
	}
}
