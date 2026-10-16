package ftspersist_test

import (
	"context"
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/v2/pkg/textproc"
)

func TestSaveLoadSnapshotRoundTripSingleField(t *testing.T) {
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
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "snapshot roundtrip"}}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	dir := t.TempDir()
	paths := ftspersist.SnapshotPaths{
		IndexPath:  filepath.Join(dir, "default.index.fidx"),
		FilterPath: filepath.Join(dir, "default.filter.fidx"),
	}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "bloom", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	if _, err := os.Stat(paths.FilterPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("separate filter snapshot exists after bundled save: %v", err)
	}
	if err := os.WriteFile(paths.FilterPath, []byte("stale legacy filter"), 0o600); err != nil {
		t.Fatalf("WriteFile(stale filter) error = %v", err)
	}

	loaded, err := ftspersist.LoadSnapshot(paths, fts.WordKeys, ftspersist.SnapshotLoadOptions{Registry: registry}, fts.WithScorer(fts.BM25()))
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

	res, err := loaded.Service.SearchText(context.Background(), "snapshot", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText(snapshot) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}

	if err := loaded.Service.Index(context.Background(), fts.Document{ID: "doc-2", Fields: map[string]fts.Field{fts.DefaultField: {Value: "snapshot stays writable"}}}); err != nil {
		t.Fatalf("Index(doc-2) after restore error = %v", err)
	}

	res, err = loaded.Service.SearchText(context.Background(), "writable", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText(writable) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount after restore write = %d, want %d", got, want)
	}
}

func TestSaveSnapshotRejectsSameIndexAndFilterPathBeforeWrite(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}
	svc := fts.New(slicedradix.New(), fts.WordKeys)
	path := filepath.Join(t.TempDir(), "snapshot.fidx")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := ftspersist.SaveSnapshot(
		ftspersist.SnapshotPaths{IndexPath: path, FilterPath: path},
		svc,
		"slicedradix",
		"",
		ftspersist.SaveOptions{Registry: registry},
	)
	if err == nil {
		t.Fatal("SaveSnapshot() error = nil")
	}
	payload, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(payload) != "existing" {
		t.Fatalf("snapshot path changed on validation error: %q", payload)
	}
}

func TestLoadSnapshotDataHonorsMaxBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized.fidx")
	if err := os.WriteFile(path, []byte("too large"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ftspersist.LoadSnapshotData(
		ftspersist.SnapshotPaths{IndexPath: path},
		ftspersist.SnapshotLoadOptions{Registry: fts.NewSnapshotRegistry(), MaxBytes: 4},
	)
	if err == nil {
		t.Fatal("LoadSnapshotData() error = nil")
	}
}

func TestSaveLoadSnapshotRoundTripEmptyIndex(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}
	idx, err := ftsbuiltin.BuildIndex("slicedradix")
	if err != nil {
		t.Fatalf("BuildIndex() error = %v", err)
	}
	svc := fts.New(idx, fts.WordKeys)

	paths := ftspersist.SnapshotPaths{IndexPath: filepath.Join(t.TempDir(), "empty.index.fidx")}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	loaded, err := ftspersist.LoadSnapshot(paths, fts.WordKeys, ftspersist.SnapshotLoadOptions{Registry: registry})
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	defer func() {
		if err := loaded.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	res, err := loaded.Service.SearchText(context.Background(), "missing", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText() error = %v", err)
	}
	if res.TotalResultsCount != 0 {
		t.Fatalf("TotalResultsCount = %d, want 0", res.TotalResultsCount)
	}
}

func TestSaveLoadSnapshotRoundTripEmptyMultiField(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}
	svc := fts.NewMultiField(func(string) (fts.MutableIndex, error) {
		return slicedradix.New(), nil
	}, fts.WordKeys)

	paths := ftspersist.SnapshotPaths{IndexPath: filepath.Join(t.TempDir(), "empty-multi.index.fidx")}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	loaded, err := ftspersist.LoadSnapshot(paths, fts.WordKeys, ftspersist.SnapshotLoadOptions{
		Registry: registry,
		IndexFactory: func(string) (fts.MutableIndex, error) {
			return slicedradix.New(), nil
		},
	})
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}
	defer loaded.Close()
	if !loaded.Service.IsMultiField() {
		t.Fatal("restored service is not multi-field")
	}
	if fields := loaded.Service.Fields(); len(fields) != 0 {
		t.Fatalf("Fields() = %v, want empty", fields)
	}
	if err := loaded.Service.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		"title": {Value: "writable"},
	}}); err != nil {
		t.Fatalf("Index() after restore error = %v", err)
	}
}

func TestLoadSnapshotDataAllowsExplicitRestore(t *testing.T) {
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
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "explicit restore path"}}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	dir := t.TempDir()
	paths := ftspersist.SnapshotPaths{
		IndexPath:  filepath.Join(dir, "default.index.fidx"),
		FilterPath: filepath.Join(dir, "default.filter.fidx"),
	}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "bloom", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded, err := ftspersist.LoadSnapshotData(paths, ftspersist.SnapshotLoadOptions{Registry: registry})
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
		fts.WordKeys,
		fts.WithFilter(loaded.Filter),
		fts.WithScorer(fts.BM25()),
		fts.WithCollectionStatsSnapshot(loaded.CollectionStats),
		fts.WithDocRegistrySnapshot(loaded.Registry),
		fts.WithTombstonesSnapshot(loaded.Tombstones),
	)

	res, err := restored.SearchText(context.Background(), "explicit", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText(explicit) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}
}

func TestSaveLoadSnapshotRoundTripMultiField(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}

	factory := func(name string) (fts.MutableIndex, error) { return slicedradix.New(), nil }
	svc := fts.NewMultiField(factory, fts.WordKeys, fts.WithScorer(fts.BM25()))
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		"title": {Value: "alpha title"},
		"body":  {Value: "beta body"},
	}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	dir := t.TempDir()
	paths := ftspersist.SnapshotPaths{IndexPath: filepath.Join(dir, "default.index.fidx")}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded, err := ftspersist.LoadSnapshot(paths, fts.WordKeys, ftspersist.SnapshotLoadOptions{Registry: registry}, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("LoadSnapshot() error = %v", err)
	}

	if loaded.IndexName != "" {
		t.Fatalf("IndexName = %q, want empty for multi-field snapshot", loaded.IndexName)
	}
	if got, want := len(loaded.FieldIndexNames), 2; got != want {
		t.Fatalf("len(FieldIndexNames) = %d, want %d", got, want)
	}

	res, err := loaded.Service.SearchQuery(context.Background(), fts.TermQuery{Field: "title", Term: "alpha"}, fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("Search(title:alpha) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("title TotalResultsCount = %d, want %d", got, want)
	}
}

func TestSnapshotValidatesPerFieldPipelines(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}
	english := textproc.DefaultEnglishPipeline()
	russian := textproc.DefaultRussianPipeline()
	factory := func(string) (fts.MutableIndex, error) { return slicedradix.New(), nil }
	svc := fts.NewMultiField(factory, fts.WordKeys,
		fts.WithPipeline(english),
		fts.WithFieldPipelines(map[string]fts.Pipeline{"body": russian}),
	)
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		"title": {Value: "running"},
		"body":  {Value: "машины"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	paths := ftspersist.SnapshotPaths{IndexPath: filepath.Join(t.TempDir(), "multi.index.fidx")}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}
	load := ftspersist.SnapshotLoadOptions{Registry: registry}
	loaded, err := ftspersist.LoadSnapshot(paths, fts.WordKeys, load,
		fts.WithPipeline(english),
		fts.WithFieldPipelines(map[string]fts.Pipeline{"body": russian}),
	)
	if err != nil {
		t.Fatalf("LoadSnapshot(matching pipelines) error = %v", err)
	}
	res, err := loaded.Service.SearchQuery(context.Background(), fts.TermQuery{Field: "body", Term: "машина"}, fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("Search(body:машина) error = %v", err)
	}
	if res.TotalResultsCount != 1 || res.Results[0].ID != "doc-1" {
		t.Fatalf("body results = %+v, want doc-1", res.Results)
	}
	if _, err := ftspersist.LoadSnapshot(paths, fts.WordKeys, load, fts.WithPipeline(english)); err == nil {
		t.Fatal("LoadSnapshot(missing field pipeline) error = nil")
	}
	changedEnglish, err := textproc.NewNamedPipeline("english", textproc.AlnumTokenizer{})
	if err != nil {
		t.Fatalf("NewNamedPipeline() error = %v", err)
	}
	if _, err := ftspersist.LoadSnapshot(paths, fts.WordKeys, load,
		fts.WithPipeline(changedEnglish),
		fts.WithFieldPipelines(map[string]fts.Pipeline{"body": russian}),
	); err == nil {
		t.Fatal("LoadSnapshot(mismatched pipeline configuration) error = nil")
	}
}

func TestSnapshotRejectsPipelineConfigurationChanges(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}
	original, err := textproc.NewNamedPipeline("persisted",
		textproc.AlnumTokenizer{},
		textproc.LowercaseFilter{},
		textproc.MinLengthOrNumericFilter{MinLength: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	svc := fts.New(slicedradix.New(), fts.WordKeys, fts.WithPipeline(original))
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		fts.DefaultField: {Value: "Pipeline Configuration"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	paths := ftspersist.SnapshotPaths{IndexPath: filepath.Join(t.TempDir(), "index.fidx")}
	if err := ftspersist.SaveSnapshot(paths, svc, "slicedradix", "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	tests := []struct {
		name      string
		tokenizer textproc.Tokenizer
		filters   []textproc.Filter
	}{
		{name: "tokenizer", tokenizer: textproc.ObservabilityTokenizer{}, filters: []textproc.Filter{textproc.LowercaseFilter{}, textproc.MinLengthOrNumericFilter{MinLength: 2}}},
		{name: "filter order", tokenizer: textproc.AlnumTokenizer{}, filters: []textproc.Filter{textproc.MinLengthOrNumericFilter{MinLength: 2}, textproc.LowercaseFilter{}}},
		{name: "filter parameter", tokenizer: textproc.AlnumTokenizer{}, filters: []textproc.Filter{textproc.LowercaseFilter{}, textproc.MinLengthOrNumericFilter{MinLength: 3}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pipeline, err := textproc.NewNamedPipeline("persisted", test.tokenizer, test.filters...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ftspersist.LoadSnapshot(paths, fts.WordKeys,
				ftspersist.SnapshotLoadOptions{Registry: registry},
				fts.WithPipeline(pipeline),
			); err == nil {
				t.Fatal("LoadSnapshot() error = nil")
			}
		})
	}
}

func TestLoadSnapshotDataDoesNotClassifyCurrentCorruptionAsLegacy(t *testing.T) {
	descriptor, err := fts.NewPipelineDescriptor("default", fts.SHA256Fingerprint([]byte("default")))
	if err != nil {
		t.Fatalf("NewPipelineDescriptor() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "current-corrupt.index.fidx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	fixture := struct {
		Version   uint16
		Kind      string
		IndexName string
		Pipelines map[string]fts.PipelineDescriptor
	}{
		Version:   5,
		Kind:      "multi",
		IndexName: "unknown",
		Pipelines: map[string]fts.PipelineDescriptor{fts.DefaultField: descriptor},
	}
	if err := gob.NewEncoder(file).Encode(fixture); err != nil {
		_ = file.Close()
		t.Fatalf("Encode() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, err = ftspersist.LoadSnapshotData(
		ftspersist.SnapshotPaths{IndexPath: path},
		ftspersist.SnapshotLoadOptions{Registry: fts.NewSnapshotRegistry()},
	)
	if err == nil {
		t.Fatal("LoadSnapshotData() error = nil")
	}
	if errors.Is(err, fts.ErrPersistenceRebuildRequired) {
		t.Fatalf("LoadSnapshotData() error = %v, must not require rebuild for current corruption", err)
	}
}

func TestLoadSnapshotDataChecksLegacyIndexBeforeFilter(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "legacy.index.fidx")
	file, err := os.Create(indexPath)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	fixture := struct {
		Version uint16
		Fields  []struct {
			FieldName string
			IndexName string
		}
	}{
		Version: 5,
		Fields: []struct {
			FieldName string
			IndexName string
		}{{FieldName: "title", IndexName: "slicedradix"}},
	}
	if err := gob.NewEncoder(file).Encode(fixture); err != nil {
		_ = file.Close()
		t.Fatalf("Encode() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	filterPath := filepath.Join(dir, "corrupt.filter.fidx")
	if err := os.WriteFile(filterPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err = ftspersist.LoadSnapshotData(
		ftspersist.SnapshotPaths{IndexPath: indexPath, FilterPath: filterPath},
		ftspersist.SnapshotLoadOptions{Registry: fts.NewSnapshotRegistry()},
	)
	if err == nil || !errors.Is(err, fts.ErrPersistenceRebuildRequired) {
		t.Fatalf("LoadSnapshotData() error = %v, want ErrPersistenceRebuildRequired", err)
	}
}
