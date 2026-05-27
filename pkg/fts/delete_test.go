package fts

import (
	"context"
	"sort"
	"strings"
	"testing"
)

type ordAwareMemoryIndex struct {
	postings  map[string][]Posting
	positions map[string]map[DocOrd][]uint32
}

func newOrdAwareMemoryIndex() *ordAwareMemoryIndex {
	return &ordAwareMemoryIndex{
		postings:  make(map[string][]Posting),
		positions: make(map[string]map[DocOrd][]uint32),
	}
}

func (m *ordAwareMemoryIndex) Insert(key string, _ DocID, ord ...DocOrd) error {
	if len(ord) == 0 {
		return nil
	}
	m.bumpCount(key, ord[0])
	return nil
}

func (m *ordAwareMemoryIndex) InsertAt(key string, _ DocID, position uint32, ord ...DocOrd) error {
	if len(ord) == 0 {
		return nil
	}
	m.bumpCount(key, ord[0])
	if _, ok := m.positions[key]; !ok {
		m.positions[key] = make(map[DocOrd][]uint32)
	}
	ps := append(m.positions[key][ord[0]], position)
	sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
	m.positions[key][ord[0]] = ps
	return nil
}

func (m *ordAwareMemoryIndex) bumpCount(key string, ord DocOrd) {
	entries := m.postings[key]
	for i := range entries {
		if entries[i].Ord == ord {
			entries[i].Count++
			m.postings[key] = entries
			return
		}
	}
	m.postings[key] = append(entries, Posting{Ord: ord, Count: 1, Seq: uint32(ord)})
	sort.Slice(m.postings[key], func(i, j int) bool { return m.postings[key][i].Seq < m.postings[key][j].Seq })
}

func (m *ordAwareMemoryIndex) Search(key string) ([]Posting, error) {
	return append([]Posting(nil), m.postings[key]...), nil
}

func (m *ordAwareMemoryIndex) SearchPrefix(prefix string) ([]Posting, error) {
	merged := make(map[DocOrd]Posting)
	for key, docs := range m.postings {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, doc := range docs {
			ref := merged[doc.Ord]
			ref.Ord = doc.Ord
			ref.Seq = doc.Seq
			ref.Count += doc.Count
			merged[doc.Ord] = ref
		}
	}
	out := make([]Posting, 0, len(merged))
	for _, doc := range merged {
		out = append(out, doc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (m *ordAwareMemoryIndex) SearchPositional(key string) ([]PositionalPosting, error) {
	entries := m.postings[key]
	out := make([]PositionalPosting, 0, len(entries))
	for _, entry := range entries {
		out = append(out, PositionalPosting{Ord: entry.Ord, Positions: m.positions[key][entry.Ord]})
	}
	return out, nil
}

func hasResultID(res *SearchResult, id DocID) bool {
	for _, item := range res.Results {
		if item.ID == id {
			return true
		}
	}
	return false
}

func TestDeleteFiltersAllSearchPathsAndUpdatesStats(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys, WithScorer(TFIDF()))
	ctx := context.Background()
	for _, doc := range []struct {
		id      DocID
		content string
	}{
		{id: "doc-a", content: "alpha beta barack obama barge"},
		{id: "doc-b", content: "alpha beta hotel"},
		{id: "doc-c", content: "alpha gamma bar"},
	} {
		if err := svc.IndexDocument(ctx, doc.id, doc.content); err != nil {
			t.Fatalf("IndexDocument(%q) error = %v", doc.id, err)
		}
	}

	if !svc.Delete("doc-a") {
		t.Fatal("Delete(doc-a) = false, want true")
	}

	termRes, err := svc.SearchDocuments(ctx, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(alpha) error = %v", err)
	}
	if hasResultID(termRes, "doc-a") {
		t.Fatalf("deleted doc leaked into term search: %+v", termRes.Results)
	}

	prefixRes, err := svc.SearchDocuments(ctx, "bar*", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(bar*) error = %v", err)
	}
	if hasResultID(prefixRes, "doc-a") || !hasResultID(prefixRes, "doc-c") {
		t.Fatalf("unexpected prefix results after delete: %+v", prefixRes.Results)
	}

	phraseRes, err := svc.SearchDocuments(ctx, `"barack obama"`, 10)
	if err != nil {
		t.Fatalf("SearchDocuments(phrase) error = %v", err)
	}
	if len(phraseRes.Results) != 0 {
		t.Fatalf("deleted doc leaked into phrase search: %+v", phraseRes.Results)
	}

	boolFastRes, err := svc.Search(ctx, &BooleanQuery{Clauses: []BoolClause{
		MustClause(TermQuery{Term: "alpha"}),
		MustClause(TermQuery{Term: "beta"}),
	}}, 10)
	if err != nil {
		t.Fatalf("Search(bool must) error = %v", err)
	}
	if len(boolFastRes.Results) != 1 || boolFastRes.Results[0].ID != "doc-b" {
		t.Fatalf("unexpected bool fast results after delete: %+v", boolFastRes.Results)
	}

	wandRes, err := svc.Search(ctx, &BooleanQuery{Clauses: []BoolClause{
		ShouldClause(TermQuery{Term: "alpha"}),
		ShouldClause(TermQuery{Term: "beta"}),
		ShouldClause(TermQuery{Term: "gamma"}),
	}}, 2)
	if err != nil {
		t.Fatalf("Search(WAND) error = %v", err)
	}
	if hasResultID(wandRes, "doc-a") {
		t.Fatalf("deleted doc leaked into WAND results: %+v", wandRes.Results)
	}

	if got := svc.collection.TotalDocs(); got != 2 {
		t.Fatalf("TotalDocs() after delete = %d, want 2", got)
	}
	if got := svc.collection.FieldDocCount(DefaultField); got != 2 {
		t.Fatalf("FieldDocCount(default) after delete = %d, want 2", got)
	}
	if got := svc.collection.AvgDocLen(DefaultField); got != 3 {
		t.Fatalf("AvgDocLen(default) after delete = %v, want 3", got)
	}
}

func TestUpdateDocumentDoesNotLeaveStaleContent(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys)
	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "alpha oldterm"); err != nil {
		t.Fatalf("IndexDocument(doc-a) error = %v", err)
	}
	if err := svc.UpdateDocument(ctx, "doc-a", "alpha newterm"); err != nil {
		t.Fatalf("UpdateDocument(doc-a) error = %v", err)
	}

	oldRes, err := svc.SearchDocuments(ctx, "oldterm", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(oldterm) error = %v", err)
	}
	if hasResultID(oldRes, "doc-a") {
		t.Fatalf("stale content leaked after UpdateDocument: %+v", oldRes.Results)
	}

	newRes, err := svc.SearchDocuments(ctx, "newterm", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(newterm) error = %v", err)
	}
	if len(newRes.Results) != 1 || newRes.Results[0].ID != "doc-a" {
		t.Fatalf("updated content not found after UpdateDocument: %+v", newRes.Results)
	}
}

func TestUpdateDocumentReplacesMultiFieldDocument(t *testing.T) {
	factory := func(name string) (Index, error) { return newOrdAwareMemoryIndex(), nil }
	svc := NewMultiField(factory, WordKeys)
	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-a", Fields: map[string]Field{
		"title": {Value: "oldtitle"},
		"body":  {Value: "oldbody"},
	}}); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	if err := svc.Update(ctx, Document{ID: "doc-a", Fields: map[string]Field{
		"title": {Value: "newtitle"},
		"body":  {Value: "newbody"},
	}}); err != nil {
		t.Fatalf("Update(doc-a) error = %v", err)
	}

	oldTitle, err := svc.Search(ctx, TermQuery{Field: "title", Term: "oldtitle"}, 10)
	if err != nil {
		t.Fatalf("Search(title:oldtitle) error = %v", err)
	}
	if len(oldTitle.Results) != 0 {
		t.Fatalf("old title leaked after Update: %+v", oldTitle.Results)
	}

	newTitle, err := svc.Search(ctx, TermQuery{Field: "title", Term: "newtitle"}, 10)
	if err != nil {
		t.Fatalf("Search(title:newtitle) error = %v", err)
	}
	if len(newTitle.Results) != 1 || newTitle.Results[0].ID != "doc-a" {
		t.Fatalf("new title missing after Update: %+v", newTitle.Results)
	}

	oldBody, err := svc.Search(ctx, TermQuery{Field: "body", Term: "oldbody"}, 10)
	if err != nil {
		t.Fatalf("Search(body:oldbody) error = %v", err)
	}
	if len(oldBody.Results) != 0 {
		t.Fatalf("old body leaked after Update: %+v", oldBody.Results)
	}
}

func TestDeleteMarksServiceForCompactionWhenLoadFactorReached(t *testing.T) {
	svc := New(newOrdAwareMemoryIndex(), WordKeys, WithCompactionLoadFactor(0.5))
	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "alpha"); err != nil {
		t.Fatalf("IndexDocument(doc-a) error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-b", "beta"); err != nil {
		t.Fatalf("IndexDocument(doc-b) error = %v", err)
	}
	if svc.NeedsCompaction() {
		t.Fatal("NeedsCompaction() = true before deletes, want false")
	}
	if !svc.Delete("doc-a") {
		t.Fatal("Delete(doc-a) = false, want true")
	}

	stats := svc.CompactionStats()
	if got, want := stats.TotalAssignedDocs, 2; got != want {
		t.Fatalf("TotalAssignedDocs = %d, want %d", got, want)
	}
	if got, want := stats.LiveDocs, 1; got != want {
		t.Fatalf("LiveDocs = %d, want %d", got, want)
	}
	if got, want := stats.TombstonedDocs, 1; got != want {
		t.Fatalf("TombstonedDocs = %d, want %d", got, want)
	}
	if got, want := stats.TombstoneLoadFactor, 0.5; got != want {
		t.Fatalf("TombstoneLoadFactor = %v, want %v", got, want)
	}
	if !svc.NeedsCompaction() {
		t.Fatal("NeedsCompaction() = false, want true after threshold reached")
	}
}

func TestDeleteTriggersCompactionCallbackWhenEnabled(t *testing.T) {
	called := 0
	var got CompactionStats
	svc := New(newOrdAwareMemoryIndex(), WordKeys,
		WithCompactionLoadFactor(0.5),
		WithCompactionCallback(func(stats CompactionStats) {
			called++
			got = stats
		}),
	)
	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "alpha"); err != nil {
		t.Fatalf("IndexDocument(doc-a) error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-b", "beta"); err != nil {
		t.Fatalf("IndexDocument(doc-b) error = %v", err)
	}
	if !svc.Delete("doc-a") {
		t.Fatal("Delete(doc-a) = false, want true")
	}
	if called != 1 {
		t.Fatalf("compaction callback calls = %d, want 1", called)
	}
	if got.TombstoneLoadFactor != 0.5 {
		t.Fatalf("callback TombstoneLoadFactor = %v, want 0.5", got.TombstoneLoadFactor)
	}
}

func TestDeleteDoesNotTriggerCompactionCallbackWhenDisabled(t *testing.T) {
	called := 0
	svc := New(newOrdAwareMemoryIndex(), WordKeys,
		WithCompactionLoadFactor(0.5),
		WithAutoCompactionCheck(false),
		WithCompactionCallback(func(stats CompactionStats) {
			called++
		}),
	)
	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "alpha"); err != nil {
		t.Fatalf("IndexDocument(doc-a) error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-b", "beta"); err != nil {
		t.Fatalf("IndexDocument(doc-b) error = %v", err)
	}
	if !svc.Delete("doc-a") {
		t.Fatal("Delete(doc-a) = false, want true")
	}
	if called != 0 {
		t.Fatalf("compaction callback calls = %d, want 0", called)
	}
	if !svc.NeedsCompaction() {
		t.Fatal("NeedsCompaction() = false, want true even when auto-check disabled")
	}
}

func TestUpdateTriggersCompactionCallbackAfterReindex(t *testing.T) {
	called := 0
	var got CompactionStats
	svc := New(newOrdAwareMemoryIndex(), WordKeys,
		WithCompactionLoadFactor(0.3),
		WithCompactionCallback(func(stats CompactionStats) {
			called++
			got = stats
		}),
	)
	ctx := context.Background()
	if err := svc.IndexDocument(ctx, "doc-a", "alpha old"); err != nil {
		t.Fatalf("IndexDocument(doc-a) error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-b", "beta"); err != nil {
		t.Fatalf("IndexDocument(doc-b) error = %v", err)
	}
	if err := svc.UpdateDocument(ctx, "doc-a", "alpha new"); err != nil {
		t.Fatalf("UpdateDocument(doc-a) error = %v", err)
	}
	if called != 1 {
		t.Fatalf("compaction callback calls = %d, want 1", called)
	}
	if got.TotalAssignedDocs != 3 || got.LiveDocs != 2 || got.TombstonedDocs != 1 {
		t.Fatalf("callback stats = %+v, want assigned=3 live=2 tombstoned=1", got)
	}
}
