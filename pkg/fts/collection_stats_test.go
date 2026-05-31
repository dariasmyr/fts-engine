package fts

import (
	"context"
	"testing"
)

func TestCollectionStatsObservePerFieldLengths(t *testing.T) {
	svc := NewMultiField(
		func(name string) (Index, error) { return newMemoryIndex(), nil },
		WordKeys,
		WithScorer(BM25()),
	)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-1", Fields: map[string]Field{
		"title": {Value: "alpha beta"},
		"body":  {Value: "alpha beta gamma"},
	}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-2", Fields: map[string]Field{
		"title": {Value: "alpha"},
	}}); err != nil {
		t.Fatalf("Index(doc-2) error = %v", err)
	}

	if got := svc.collection.TotalDocs(); got != 2 {
		t.Fatalf("TotalDocs() = %d, want 2", got)
	}
	ord1 := svc.registry.GetOrAssign("doc-1")
	if got := svc.collection.DocLen("title", ord1); got != 2 {
		t.Fatalf("DocLen(title, doc-1) = %d, want 2", got)
	}
	if got := svc.collection.DocLen("body", ord1); got != 3 {
		t.Fatalf("DocLen(body, doc-1) = %d, want 3", got)
	}
	if got := svc.collection.FieldDocCount("title"); got != 2 {
		t.Fatalf("FieldDocCount(title) = %d, want 2", got)
	}
	if got := svc.collection.FieldDocCount("body"); got != 1 {
		t.Fatalf("FieldDocCount(body) = %d, want 1", got)
	}
	if got := svc.collection.AvgDocLen("title"); got != 1.5 {
		t.Fatalf("AvgDocLen(title) = %v, want 1.5", got)
	}
	if got := svc.collection.AvgDocLen("body"); got != 3 {
		t.Fatalf("AvgDocLen(body) = %v, want 3", got)
	}
}

func TestCollectionStatsNotObservedWithoutScorer(t *testing.T) {
	svc := New(newMemoryIndex(), WordKeys)

	if err := svc.IndexDocument(context.Background(), "doc-1", "alpha beta"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}
	if got := svc.collection.TotalDocs(); got != 0 {
		t.Fatalf("TotalDocs() without scorer = %d, want 0", got)
	}
}
