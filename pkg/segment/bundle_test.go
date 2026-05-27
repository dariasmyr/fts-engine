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
