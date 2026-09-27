package ftspersist_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func TestSaveLoadSegmentRoundTripSingleField(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
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

	svc := fts.New(idx, fts.WordKeys, fts.WithFilter(flt), fts.WithScorer(fts.BM25()))
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "segment roundtrip"}}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	dir := filepath.Join(t.TempDir(), "segment")
	paths := ftspersist.SegmentPaths{Dir: dir}
	if err := ftspersist.SaveSegment(paths, svc, "bloom", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}

	loaded, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile, Registry: registry}, fts.WithScorer(fts.BM25()))
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

	if err := loaded.Service.Index(context.Background(), fts.Document{ID: "doc-2", Fields: map[string]fts.Field{fts.DefaultField: {Value: "should fail"}}}); err == nil {
		t.Fatal("Index(doc-2) after segment restore error = nil, want read-only error")
	}
}

func TestSaveLoadSegmentRoundTripMultiField(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}

	factory := func(name string) (fts.Index, error) { return slicedradix.New(), nil }
	svc := fts.NewMultiField(factory, fts.WordKeys, fts.WithScorer(fts.BM25()))
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		"title": {Value: "alpha title"},
		"body":  {Value: "beta body"},
	}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := ftspersist.SaveSegment(paths, svc, "", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}

	restored, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile, Registry: registry}, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer restored.Close()

	res, err := restored.Service.Search(context.Background(), fts.TermQuery{Field: "title", Term: "alpha"}, 10)
	if err != nil {
		t.Fatalf("Search(title:alpha) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("title TotalResultsCount = %d, want %d", got, want)
	}

	res, err = restored.Service.Search(context.Background(), fts.TermQuery{Field: "body", Term: "beta"}, 10)
	if err != nil {
		t.Fatalf("Search(body:beta) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("body TotalResultsCount = %d, want %d", got, want)
	}
}

func TestSaveLoadSegmentRoundTripMmap(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}

	idx, err := ftsbuiltin.BuildIndex("slicedradix")
	if err != nil {
		t.Fatalf("BuildIndex() error = %v", err)
	}
	svc := fts.New(idx, fts.WordKeys, fts.WithScorer(fts.BM25()))
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "segment mmap roundtrip"}}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := ftspersist.SaveSegment(paths, svc, "", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}

	loaded, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessMmap, Registry: registry}, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}

	res, err := loaded.Service.SearchDocuments(context.Background(), "mmap", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(mmap) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}

	if err := loaded.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := loaded.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestSegmentAnalyzerFingerprintGate(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	pipeline := textproc.ObservabilityPipeline()
	svc := fts.New(slicedradix.New(), fts.WordKeys, fts.WithPipeline(pipeline))
	if err := svc.Index(context.Background(), fts.Document{
		ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "io.EOF"}},
	}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := ftspersist.SaveSegment(paths, svc, "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}
	descriptor := pipeline.Descriptor()
	loaded, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{
		Access: ftspersist.AccessFile, ExpectedAnalyzerFingerprint: descriptor.Fingerprint, Registry: registry,
	}, fts.WithPipeline(pipeline))
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer func() {
		if err := loaded.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()
	wrong := textproc.DefaultEnglishPipeline().Descriptor().Fingerprint
	if _, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{
		Access: ftspersist.AccessFile, ExpectedAnalyzerFingerprint: wrong, Registry: registry,
	}); err == nil {
		t.Fatal("LoadSegment(wrong analyzer) error = nil, want mismatch")
	}
}
