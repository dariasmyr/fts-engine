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
	}
	searches []string
}

func newMemoryIndex() *memoryIndex {
	return &memoryIndex{entries: make(map[string][]DocRef)}
}

func (m *memoryIndex) Insert(key string, id DocID) error {
	m.inserts = append(m.inserts, struct {
		key string
		id  DocID
	}{key: key, id: id})
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

func TestSearchDocumentsReturnsTimings(t *testing.T) {
	idx := newMemoryIndex()
	idx.entries["one"] = []DocRef{{ID: "x", Count: 1}}

	svc := New(idx, WordKeys)

	res, err := svc.SearchDocuments(context.Background(), "one", 1)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	for _, key := range []string{"preprocess", "search_tokens", "total"} {
		if _, ok := res.Timings[key]; !ok {
			t.Fatalf("timings key %q missing", key)
		}
		if res.Timings[key] == "" {
			t.Fatalf("timings key %q is empty", key)
		}
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
