package ftspersist_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/v2/pkg/textproc"
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
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-2", Fields: map[string]fts.Field{fts.DefaultField: {Value: "segment candidate"}}}); err != nil {
		t.Fatalf("Index(doc-2) error = %v", err)
	}

	dir := filepath.Join(t.TempDir(), "segment")
	paths := ftspersist.SegmentPaths{Dir: dir}
	if err := ftspersist.SaveSegment(context.Background(), paths, svc, "bloom", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
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

	res, err := loaded.Service.SearchText(context.Background(), "segment", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText(segment) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 2; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}
	candidateResult, err := loaded.Service.SearchText(context.Background(), "segment", fts.SearchOptions{
		CandidateIDs: []fts.DocID{"doc-2"},
		MaxResults:   10,
	})
	if err != nil {
		t.Fatalf("SearchText(segment, candidates) error = %v", err)
	}
	if len(candidateResult.Results) != 1 || candidateResult.Results[0].ID != "doc-2" {
		t.Fatalf("candidate results = %+v, want only doc-2", candidateResult.Results)
	}

	if err := loaded.Service.Index(context.Background(), fts.Document{ID: "doc-3", Fields: map[string]fts.Field{fts.DefaultField: {Value: "should fail"}}}); err == nil {
		t.Fatal("Index(doc-3) after segment restore error = nil, want read-only error")
	}
}

func TestSaveLoadSegmentRoundTripEmptyIndex(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}
	idx, err := ftsbuiltin.BuildIndex("slicedradix")
	if err != nil {
		t.Fatalf("BuildIndex() error = %v", err)
	}
	svc := fts.New(idx, fts.WordKeys)

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "empty-segment")}
	if err := ftspersist.SaveSegment(context.Background(), paths, svc, "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}
	loaded, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
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

func TestSaveLoadSegmentRoundTripEmptyMultiField(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}
	svc := fts.NewMultiField(func(string) (fts.MutableIndex, error) {
		return slicedradix.New(), nil
	}, fts.WordKeys)

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "empty-multi-segment")}
	if err := ftspersist.SaveSegment(context.Background(), paths, svc, "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}
	loaded, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer loaded.Close()
	if !loaded.Service.IsMultiField() {
		t.Fatal("restored service is not multi-field")
	}
	if fields := loaded.Service.Fields(); len(fields) != 0 {
		t.Fatalf("Fields() = %v, want empty", fields)
	}
}

func TestSaveLoadSegmentPreservesMultiFieldModeWithDefaultFieldOnly(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}
	svc := fts.NewMultiField(func(string) (fts.MutableIndex, error) {
		return slicedradix.New(), nil
	}, fts.WordKeys)
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		fts.DefaultField: {Value: "default only"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "default-only-multi-segment")}
	if err := ftspersist.SaveSegment(context.Background(), paths, svc, "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}
	loaded, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Registry: registry})
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer loaded.Close()
	if !loaded.Service.IsMultiField() {
		t.Fatal("restored service is not multi-field")
	}
}

func TestSaveLoadSegmentRoundTripMultiField(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	if err := ftsbuiltin.RegisterSnapshotCodecs(registry); err != nil {
		t.Fatalf("RegisterSnapshotCodecs() error = %v", err)
	}

	factory := func(name string) (fts.MutableIndex, error) { return slicedradix.New(), nil }
	english := textproc.DefaultEnglishPipeline()
	russian := textproc.DefaultRussianPipeline()
	svc := fts.NewMultiField(factory, fts.WordKeys,
		fts.WithScorer(fts.BM25()),
		fts.WithPipeline(english),
		fts.WithFieldPipelines(map[string]fts.Pipeline{"body": russian}),
	)
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		"title": {Value: "running title"},
		"body":  {Value: "машины"},
	}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := ftspersist.SaveSegment(context.Background(), paths, svc, "", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}

	restored, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile, Registry: registry},
		fts.WithScorer(fts.BM25()),
		fts.WithPipeline(english),
		fts.WithFieldPipelines(map[string]fts.Pipeline{"body": russian}),
	)
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer restored.Close()

	res, err := restored.Service.SearchQuery(context.Background(), fts.TermQuery{Field: "title", Term: "run"}, fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("Search(title:run) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("title TotalResultsCount = %d, want %d", got, want)
	}

	res, err = restored.Service.SearchQuery(context.Background(), fts.TermQuery{Field: "body", Term: "машина"}, fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("Search(body:машина) error = %v", err)
	}
	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("body TotalResultsCount = %d, want %d", got, want)
	}

	if _, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessFile, Registry: registry},
		fts.WithPipeline(english),
	); err == nil {
		t.Fatal("LoadSegment() without matching body pipeline error = nil")
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
	if err := ftspersist.SaveSegment(context.Background(), paths, svc, "", ftspersist.SaveOptions{SyncFile: true, Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}

	loaded, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{Access: ftspersist.AccessMmap, Registry: registry}, fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}

	res, err := loaded.Service.SearchText(context.Background(), "mmap", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText(mmap) error = %v", err)
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

func TestSegmentPipelineFingerprintGate(t *testing.T) {
	registry := fts.NewSnapshotRegistry()
	pipeline := textproc.ObservabilityPipeline()
	svc := fts.New(slicedradix.New(), fts.WordKeys, fts.WithPipeline(pipeline))
	if err := svc.Index(context.Background(), fts.Document{
		ID: "doc-1", Fields: map[string]fts.Field{fts.DefaultField: {Value: "io.EOF"}},
	}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	paths := ftspersist.SegmentPaths{Dir: filepath.Join(t.TempDir(), "segment")}
	if err := ftspersist.SaveSegment(context.Background(), paths, svc, "", ftspersist.SaveOptions{Registry: registry}); err != nil {
		t.Fatalf("SaveSegment() error = %v", err)
	}
	loaded, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{
		Access: ftspersist.AccessFile, Registry: registry,
	}, fts.WithPipeline(pipeline))
	if err != nil {
		t.Fatalf("LoadSegment() error = %v", err)
	}
	defer func() {
		if err := loaded.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()
	if _, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{
		Access: ftspersist.AccessFile, Registry: registry,
	}, fts.WithPipeline(textproc.DefaultEnglishPipeline())); err == nil {
		t.Fatal("LoadSegment(wrong pipeline) error = nil, want mismatch")
	}
	changed, err := textproc.NewNamedPipeline("observability",
		textproc.ObservabilityTokenizer{},
		textproc.LowercaseFilter{},
		textproc.MinLengthOrNumericFilter{MinLength: 3},
		textproc.UniqueFilter{},
	)
	if err != nil {
		t.Fatalf("NewNamedPipeline() error = %v", err)
	}
	if _, err := ftspersist.LoadSegment(paths, fts.WordKeys, ftspersist.SegmentLoadOptions{
		Access: ftspersist.AccessFile, Registry: registry,
	}, fts.WithPipeline(changed)); err == nil {
		t.Fatal("LoadSegment(changed configuration) error = nil, want mismatch")
	}
}
