package segment_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

func TestBundleRestoreServiceRoundTrip(t *testing.T) {
	idx := slicedradix.New()
	svc := fts.New(idx, fts.WordKeys, fts.WithScorer(fts.BM25()))
	ctx := context.Background()

	if err := svc.IndexDocument(ctx, "doc-a", "alpha beta"); err != nil {
		t.Fatalf("IndexDocument(doc-a) error = %v", err)
	}
	if err := svc.IndexDocument(ctx, "doc-b", "alpha alpha"); err != nil {
		t.Fatalf("IndexDocument(doc-b) error = %v", err)
	}
	if !svc.Delete("doc-a") {
		t.Fatal("Delete(doc-a) = false, want true")
	}

	var bundleBytes bytes.Buffer
	index, _ := svc.SnapshotComponents()
	source, ok := index.(segment.Source)
	if !ok {
		t.Fatal("snapshot index does not implement segment.Source")
	}
	if err := segment.SaveBundle(&bundleBytes, source, svc.SnapshotCollectionStats(), svc.SnapshotRegistry(), svc.SnapshotTombstones()); err != nil {
		t.Fatalf("SaveBundle() error = %v", err)
	}

	loaded, err := segment.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}
	sealed, err := loaded.Fields[fts.DefaultField].Search("alpha")
	if err != nil {
		t.Fatalf("sealed Search(alpha) error = %v", err)
	}
	if len(sealed) != 1 || sealed[0].Ord != 1 || sealed[0].Count != 2 {
		t.Fatalf("sealed Search(alpha) = %+v, want only live ord=1 count=2", sealed)
	}
	restored, err := segment.RestoreService(loaded, fts.WordKeys, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("RestoreService() error = %v", err)
	}

	res, err := restored.SearchDocuments(ctx, "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(alpha) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-b" {
		t.Fatalf("Results = %+v, want only doc-b", res.Results)
	}
	if res.Results[0].Score <= 0 {
		t.Fatalf("restored score = %f, want positive", res.Results[0].Score)
	}
}

func TestMultiFieldBundleRestoreServiceRoundTrip(t *testing.T) {
	factory := func(name string) (fts.Index, error) { return slicedradix.New(), nil }
	svc := fts.NewMultiField(factory, fts.WordKeys, fts.WithScorer(fts.BM25()))
	ctx := context.Background()

	if err := svc.Index(ctx, fts.Document{ID: "doc-a", Fields: map[string]fts.Field{
		"title": {Value: "alpha title"},
		"body":  {Value: "stale body"},
	}}); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	if err := svc.Index(ctx, fts.Document{ID: "doc-b", Fields: map[string]fts.Field{
		"title": {Value: "fresh title"},
		"body":  {Value: "alpha body"},
	}}); err != nil {
		t.Fatalf("Index(doc-b) error = %v", err)
	}
	if !svc.Delete("doc-a") {
		t.Fatal("Delete(doc-a) = false, want true")
	}

	fields, _ := svc.SnapshotFields()
	sources := make(map[string]segment.Source, len(fields))
	for fieldName, index := range fields {
		source, ok := index.(segment.Source)
		if !ok {
			t.Fatalf("field %q index does not implement segment.Source", fieldName)
		}
		sources[fieldName] = source
	}

	var bundleBytes bytes.Buffer
	if err := segment.SaveMultiFieldBundle(&bundleBytes, sources, svc.SnapshotCollectionStats(), svc.SnapshotRegistry(), svc.SnapshotTombstones()); err != nil {
		t.Fatalf("SaveMultiFieldBundle() error = %v", err)
	}

	loaded, err := segment.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}
	if got := len(loaded.Fields); got != 2 {
		t.Fatalf("len(loaded.Fields) = %d, want 2", got)
	}
	titleSealed, err := loaded.Fields["title"].Search("alpha")
	if err != nil {
		t.Fatalf("sealed Search(title:alpha) error = %v", err)
	}
	if len(titleSealed) != 0 {
		t.Fatalf("sealed title postings = %+v, want tombstoned doc removed", titleSealed)
	}

	restored, err := segment.RestoreService(loaded, fts.WordKeys, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("RestoreService() error = %v", err)
	}

	titleRes, err := restored.Search(ctx, fts.TermQuery{Field: "title", Term: "alpha"}, 10)
	if err != nil {
		t.Fatalf("Search(title:alpha) error = %v", err)
	}
	if titleRes.TotalResultsCount != 0 {
		t.Fatalf("title results = %+v, want no tombstoned doc", titleRes.Results)
	}

	bodyRes, err := restored.Search(ctx, fts.TermQuery{Field: "body", Term: "alpha"}, 10)
	if err != nil {
		t.Fatalf("Search(body:alpha) error = %v", err)
	}
	if bodyRes.TotalResultsCount != 1 || len(bodyRes.Results) != 1 || bodyRes.Results[0].ID != "doc-b" {
		t.Fatalf("body results = %+v, want only doc-b", bodyRes.Results)
	}
	if bodyRes.Results[0].Score <= 0 {
		t.Fatalf("restored multi-field score = %f, want positive", bodyRes.Results[0].Score)
	}
}
