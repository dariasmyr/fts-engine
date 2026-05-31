package ftspersist_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func TestSaveLoadSegmentRoundTripSingleField(t *testing.T) {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}

	idx, err := ftsbuiltin.BuildIndex("slicedradix")
	if err != nil {
		t.Fatalf("BuildIndex() error = %v", err)
	}
	flt, err := ftsbuiltin.BuildFilter("bloom", ftsbuiltin.FilterOptions{
		BloomExpectedItems: 1_000,
		BloomBitsPerItem:   10,
		BloomK:             7,
	})
	if err != nil {
		t.Fatalf("BuildFilter() error = %v", err)
	}

	svc := fts.New(idx, keygen.Word, fts.WithFilter(flt), fts.WithScorer(fts.BM25()))
	if err := svc.IndexDocument(context.Background(), "doc-1", "segment roundtrip"); err != nil {
		t.Fatalf("IndexDocument(doc-1) error = %v", err)
	}

	dir := filepath.Join(t.TempDir(), "segment")
	paths := ftspersist.SegmentPaths{Dir: dir}
	if err := ftspersist.SaveSegment(paths, svc, "bloom", ftspersist.SaveOptions{SyncFile: true}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}

	loaded, err := ftspersist.LoadSegment(paths, keygen.Word, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile}, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer func() {
		if err := loaded.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if loaded.FilterName != "bloom" {
		t.Fatalf("FilterName = %q, want bloom", loaded.FilterName)
	}

	res, err := loaded.Service.SearchDocuments(context.Background(), "segment", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(segment) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}

	if err := loaded.Service.IndexDocument(context.Background(), "doc-2", "should fail"); err == nil {
		t.Fatal("IndexDocument(doc-2) after segment restore error = nil, want read-only error")
	}
}

func TestSaveLoadSegmentRoundTripMultiField(t *testing.T) {
	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}

	factory := func(name string) (fts.Index, error) { return slicedradix.New(), nil }
	svc := fts.NewMultiField(factory, keygen.Word, fts.WithScorer(fts.BM25()))
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		"title": {Value: "alpha title"},
		"body":  {Value: "beta body"},
	}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := ftspersist.SaveSegment(paths, svc, "", ftspersist.SaveOptions{SyncFile: true}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}

	loadedData, err := ftspersist.LoadSegmentData(paths, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile})
	if err != nil {
		t.Fatalf("LoadSegmentData() error = %v", err)
	}

	if got, want := len(loadedData.Fields), 2; got != want {
		t.Fatalf("len(Fields) = %d, want %d", got, want)
	}
	if loadedData.Segment != nil {
		t.Fatal("Segment != nil, want nil for multi-field segment")
	}

	restored, err := ftspersist.RestoreSegmentService(loadedData, keygen.Word, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("RestoreSegmentService() error = %v", err)
	}

	res, err := restored.Search(context.Background(), fts.TermQuery{Field: "title", Term: "alpha"}, 10)
	if err != nil {
		t.Fatalf("Search(title:alpha) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("title TotalResultsCount = %d, want %d", got, want)
	}

	res, err = restored.Search(context.Background(), fts.TermQuery{Field: "body", Term: "beta"}, 10)
	if err != nil {
		t.Fatalf("Search(body:beta) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("body TotalResultsCount = %d, want %d", got, want)
	}
}
