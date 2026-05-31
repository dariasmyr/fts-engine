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

func TestSaveLoadSnapshotRoundTripSingleField(t *testing.T) {
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
	if err := svc.IndexDocument(context.Background(), "doc-1", "snapshot roundtrip"); err != nil {
		t.Fatalf("IndexDocument(doc-1) error = %v", err)
	}

	dir := t.TempDir()
	paths := ftspersist.SnapshotPaths{
		IndexPath:  filepath.Join(dir, "default.index.fidx"),
		FilterPath: filepath.Join(dir, "default.filter.fidx"),
	}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "bloom", ftspersist.SaveOptions{SyncFile: true}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded, err := ftspersist.LoadSnapshot(paths, keygen.Word, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	defer func() {
		if err := loaded.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if loaded.IndexName != "slicedradix" {
		t.Fatalf("IndexName = %q, want slicedradix", loaded.IndexName)
	}
	if loaded.FilterName != "bloom" {
		t.Fatalf("FilterName = %q, want bloom", loaded.FilterName)
	}

	res, err := loaded.Service.SearchDocuments(context.Background(), "snapshot", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(snapshot) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}

	if err := loaded.Service.IndexDocument(context.Background(), "doc-2", "snapshot stays writable"); err != nil {
		t.Fatalf("IndexDocument(doc-2) after restore error = %v", err)
	}

	res, err = loaded.Service.SearchDocuments(context.Background(), "writable", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(writable) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount after restore write = %d, want %d", got, want)
	}
}

func TestLoadSnapshotDataAllowsExplicitRestore(t *testing.T) {
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
	if err := svc.IndexDocument(context.Background(), "doc-1", "explicit restore path"); err != nil {
		t.Fatalf("IndexDocument(doc-1) error = %v", err)
	}

	dir := t.TempDir()
	paths := ftspersist.SnapshotPaths{
		IndexPath:  filepath.Join(dir, "default.index.fidx"),
		FilterPath: filepath.Join(dir, "default.filter.fidx"),
	}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "bloom", ftspersist.SaveOptions{SyncFile: true}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded, err := ftspersist.LoadSnapshotData(paths)
	if err != nil {
		t.Fatalf("LoadSnapshotData() error = %v", err)
	}

	if loaded.Index == nil {
		t.Fatal("LoadSnapshotData() returned nil Index")
	}
	if loaded.Filter == nil {
		t.Fatal("LoadSnapshotData() returned nil Filter")
	}

	restored := fts.New(
		loaded.Index,
		keygen.Word,
		fts.WithFilter(loaded.Filter),
		fts.WithScorer(fts.BM25()),
		fts.WithCollectionStatsSnapshot(loaded.CollectionStats),
		fts.WithDocRegistrySnapshot(loaded.Registry),
		fts.WithTombstonesSnapshot(loaded.Tombstones),
	)

	res, err := restored.SearchDocuments(context.Background(), "explicit", 10)
	if err != nil {
		t.Fatalf("SearchDocuments(explicit) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}
}

func TestSaveLoadSnapshotRoundTripMultiField(t *testing.T) {
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

	dir := t.TempDir()
	paths := ftspersist.SnapshotPaths{IndexPath: filepath.Join(dir, "default.index.fidx")}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "", ftspersist.SaveOptions{SyncFile: true}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded, err := ftspersist.LoadSnapshot(paths, keygen.Word, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}

	if loaded.IndexName != "" {
		t.Fatalf("IndexName = %q, want empty for multi-field snapshot", loaded.IndexName)
	}
	if got, want := len(loaded.FieldIndexNames), 2; got != want {
		t.Fatalf("len(FieldIndexNames) = %d, want %d", got, want)
	}

	res, err := loaded.Service.Search(context.Background(), fts.TermQuery{Field: "title", Term: "alpha"}, 10)
	if err != nil {
		t.Fatalf("Search(title:alpha) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("title TotalResultsCount = %d, want %d", got, want)
	}
}
