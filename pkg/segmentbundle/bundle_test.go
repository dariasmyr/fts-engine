package segmentbundle_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/segment"
	"github.com/dariasmyr/fts-engine/pkg/segmentbundle"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func TestBundleRestoreServiceRoundTrip(t *testing.T) {
	idx := slicedradix.New()
	pipeline := textproc.NewNamedPipeline("bundle-test", 1, textproc.AlnumTokenizer{}, textproc.LowercaseFilter{})
	svc := fts.New(idx, fts.WordKeys, fts.WithPipeline(pipeline), fts.WithScorer(fts.BM25()))
	ctx := context.Background()

	if err := svc.Index(ctx, fts.Document{ID: "doc-a", Fields: map[string]fts.Field{fts.DefaultField: {Value: "alpha beta"}}}); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	if err := svc.Index(ctx, fts.Document{ID: "doc-b", Fields: map[string]fts.Field{fts.DefaultField: {Value: "alpha alpha"}}}); err != nil {
		t.Fatalf("Index(doc-b) error = %v", err)
	}
	if !svc.Delete("doc-a") {
		t.Fatal("Delete(doc-a) = false, want true")
	}
	descriptors, ok := svc.AnalyzerDescriptors()
	if !ok {
		t.Fatal("AnalyzerDescriptors() = false, want true")
	}

	var bundleBytes bytes.Buffer
	index, _ := svc.SnapshotComponents()
	source, ok := index.(segment.Source)
	if !ok {
		t.Fatal("snapshot index does not implement segment.Source")
	}
	keyGenerator := fts.NewKeyGeneratorDescriptor("word-keys", 1)
	if err := segmentbundle.SaveBundle(&bundleBytes, source, svc.SnapshotCollectionStats(), svc.SnapshotRegistry(), svc.SnapshotTombstones(), segmentbundle.BundleSaveOptions{Analyzers: descriptors, KeyGenerator: &keyGenerator}); err != nil {
		t.Fatalf("SaveBundle() error = %v", err)
	}

	loaded, err := segmentbundle.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
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
	if _, err := segmentbundle.RestoreService(loaded, fts.WordKeys); err == nil {
		t.Fatal("RestoreService() without pipeline error = nil, want analyzer descriptor error")
	}
	restored, err := segmentbundle.RestoreService(loaded, fts.WordKeys, fts.WithPipeline(pipeline), fts.WithScorer(fts.BM25()))
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

func TestSaveServiceStoresAnalyzerIdentity(t *testing.T) {
	pipeline := textproc.ObservabilityPipeline()
	svc := fts.New(slicedradix.New(), fts.WordKeys, fts.WithPipeline(pipeline))
	if err := svc.Index(context.Background(), fts.Document{
		ID: "doc-1",
		Fields: map[string]fts.Field{
			fts.DefaultField: {Value: "analyzer identity"},
		},
	}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	var bundleBytes bytes.Buffer
	if err := segmentbundle.SaveService(&bundleBytes, svc); err != nil {
		t.Fatalf("SaveService() error = %v", err)
	}
	loaded, err := segmentbundle.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}
	if loaded.Analyzers[fts.DefaultField].Fingerprint == "" {
		t.Fatalf("Analyzers = %+v, want fingerprint", loaded.Analyzers)
	}
	if _, err := segmentbundle.RestoreService(loaded, fts.WordKeys); err == nil {
		t.Fatal("RestoreService() without pipeline error = nil, want analyzer descriptor error")
	}
	if _, err := segmentbundle.RestoreService(loaded, fts.WordKeys, fts.WithPipeline(textproc.DefaultEnglishPipeline())); err == nil {
		t.Fatal("RestoreService() with mismatched pipeline error = nil, want analyzer mismatch")
	}
	if _, err := segmentbundle.RestoreService(loaded, fts.WordKeys, fts.WithPipeline(pipeline)); err != nil {
		t.Fatalf("RestoreService() with matching pipeline error = %v", err)
	}
}

func TestMultiFieldBundleRestoreServiceRoundTrip(t *testing.T) {
	factory := func(name string) (fts.Index, error) { return slicedradix.New(), nil }
	pipeline := textproc.ObservabilityPipeline()
	svc := fts.NewMultiField(factory, fts.WordKeys, fts.WithPipeline(pipeline), fts.WithScorer(fts.BM25()))
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
	descriptors, ok := svc.AnalyzerDescriptors()
	if !ok {
		t.Fatal("AnalyzerDescriptors() = false, want true")
	}

	fields, _ := svc.SnapshotFields()
	sources := make(map[string]segment.Source, len(fields))
	for fieldName, index := range fields {
		source, ok := index.(segment.Source)
		if !ok {
			t.Fatalf("field %q does not implement segment.Source", fieldName)
		}
		sources[fieldName] = source
	}

	var bundleBytes bytes.Buffer
	keyGenerator := fts.NewKeyGeneratorDescriptor("word-keys", 1)
	if err := segmentbundle.SaveMultiFieldBundle(&bundleBytes, sources, svc.SnapshotCollectionStats(), svc.SnapshotRegistry(), svc.SnapshotTombstones(), segmentbundle.BundleSaveOptions{Analyzers: descriptors, KeyGenerator: &keyGenerator}); err != nil {
		t.Fatalf("SaveMultiFieldBundle() error = %v", err)
	}

	loaded, err := segmentbundle.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
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

	restored, err := segmentbundle.RestoreService(loaded, fts.WordKeys, fts.WithPipeline(pipeline), fts.WithScorer(fts.BM25()))
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

func TestMultiFieldBundleStoresPerFieldAnalyzerIdentity(t *testing.T) {
	factory := func(name string) (fts.Index, error) { return slicedradix.New(), nil }
	english := textproc.DefaultEnglishPipeline()
	russian := textproc.DefaultRussianPipeline()
	svc := fts.NewMultiField(factory, fts.WordKeys, fts.WithPipeline(english))
	ctx := context.Background()
	if err := svc.Index(ctx, fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		"title": {Value: "Hello", Pipeline: english},
		"body":  {Value: "Привет", Pipeline: russian},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	descriptors, ok := svc.AnalyzerDescriptors()
	if !ok || descriptors["title"].Fingerprint == descriptors["body"].Fingerprint {
		t.Fatalf("AnalyzerDescriptors() = %+v, want distinct field descriptors", descriptors)
	}
	var bundleBytes bytes.Buffer
	if err := segmentbundle.SaveService(&bundleBytes, svc); err != nil {
		t.Fatalf("SaveService() error = %v", err)
	}
	bundle, err := segmentbundle.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}

	if _, err := segmentbundle.RestoreService(bundle, fts.WordKeys, fts.WithPipeline(english)); err == nil {
		t.Fatal("RestoreService() without field pipeline error = nil, want mismatch")
	}
	if _, err := segmentbundle.RestoreService(bundle, fts.WordKeys,
		fts.WithPipeline(english),
		fts.WithFieldPipelines(map[string]fts.Pipeline{"body": russian}),
	); err != nil {
		t.Fatalf("RestoreService() with field pipelines error = %v", err)
	}
}

func TestBundleRejectsMismatchedKeyGenerator(t *testing.T) {
	keyGenerator := func(token string) ([]string, error) { return []string{"prefix:" + token}, nil }
	keyDescriptor := fts.NewKeyGeneratorDescriptor("prefix-keys", 1)
	svc := fts.New(slicedradix.New(), keyGenerator, fts.WithKeyGeneratorDescriptor(keyDescriptor))
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		fts.DefaultField: {Value: "alpha"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	var bundleBytes bytes.Buffer
	if err := segmentbundle.SaveService(&bundleBytes, svc); err != nil {
		t.Fatalf("SaveService() error = %v", err)
	}
	bundle, err := segmentbundle.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}

	if _, err := segmentbundle.RestoreService(bundle, fts.WordKeys); err == nil {
		t.Fatal("RestoreService() with mismatched key generator error = nil")
	}
}
