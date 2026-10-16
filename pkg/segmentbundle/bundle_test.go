package segmentbundle_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
	"github.com/dariasmyr/fts-engine/v2/pkg/segmentbundle"
	"github.com/dariasmyr/fts-engine/v2/pkg/textproc"
)

func TestBundleRestoreServiceRoundTrip(t *testing.T) {
	idx := slicedradix.New()
	pipeline, err := textproc.NewNamedPipeline("bundle-test", textproc.AlnumTokenizer{}, textproc.LowercaseFilter{})
	if err != nil {
		t.Fatalf("NewNamedPipeline() error = %v", err)
	}
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
	descriptors, err := svc.PipelineDescriptors()
	if err != nil {
		t.Fatalf("PipelineDescriptors() error = %v", err)
	}

	var bundleBytes bytes.Buffer
	index, _ := svc.SnapshotComponents()
	source, ok := index.(posting.Exporter)
	if !ok {
		t.Fatal("snapshot index does not implement posting.Exporter")
	}
	keyGenerator := fts.NewKeyGeneratorDescriptor("word-keys", 1)
	if err := segmentbundle.SaveBundle(&bundleBytes, source, svc.SnapshotCollectionStats(), svc.SnapshotRegistry(), svc.SnapshotTombstones(), segmentbundle.BundleSaveOptions{Pipelines: descriptors, KeyGenerator: &keyGenerator}); err != nil {
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
		t.Fatal("RestoreService() without pipeline error = nil, want pipeline descriptor error")
	}
	restored, err := segmentbundle.RestoreService(loaded, fts.WordKeys, fts.WithPipeline(pipeline), fts.WithScorer(fts.BM25()))
	if err != nil {
		t.Fatalf("RestoreService() error = %v", err)
	}

	res, err := restored.SearchText(ctx, "alpha", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText(alpha) error = %v", err)
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

func TestBundleRestoreServiceRoundTripEmptyIndex(t *testing.T) {
	svc := fts.New(slicedradix.New(), fts.WordKeys)

	var bundleBytes bytes.Buffer
	if err := segmentbundle.SaveService(&bundleBytes, svc); err != nil {
		t.Fatalf("SaveService() error = %v", err)
	}
	loaded, err := segmentbundle.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}
	restored, err := segmentbundle.RestoreService(loaded, fts.WordKeys)
	if err != nil {
		t.Fatalf("RestoreService() error = %v", err)
	}

	res, err := restored.SearchText(context.Background(), "missing", fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText() error = %v", err)
	}
	if res.TotalResultsCount != 0 {
		t.Fatalf("TotalResultsCount = %d, want 0", res.TotalResultsCount)
	}
}

func TestBundleRestoreServiceRoundTripEmptyMultiField(t *testing.T) {
	svc := fts.NewMultiField(func(string) (fts.MutableIndex, error) {
		return slicedradix.New(), nil
	}, fts.WordKeys)

	var bundleBytes bytes.Buffer
	if err := segmentbundle.SaveService(&bundleBytes, svc); err != nil {
		t.Fatalf("SaveService() error = %v", err)
	}
	loaded, err := segmentbundle.LoadBundle(bytes.NewReader(bundleBytes.Bytes()))
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}
	restored, err := segmentbundle.RestoreService(loaded, fts.WordKeys)
	if err != nil {
		t.Fatalf("RestoreService() error = %v", err)
	}
	if !restored.IsMultiField() {
		t.Fatal("restored service is not multi-field")
	}
	if fields := restored.Fields(); len(fields) != 0 {
		t.Fatalf("Fields() = %v, want empty", fields)
	}
}

func TestBundleRestoreServicePreservesMultiFieldModeWithDefaultFieldOnly(t *testing.T) {
	svc := fts.NewMultiField(func(string) (fts.MutableIndex, error) {
		return slicedradix.New(), nil
	}, fts.WordKeys)
	if err := svc.Index(context.Background(), fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		fts.DefaultField: {Value: "default only"},
	}}); err != nil {
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
	restored, err := segmentbundle.RestoreService(loaded, fts.WordKeys)
	if err != nil {
		t.Fatalf("RestoreService() error = %v", err)
	}
	if !restored.IsMultiField() {
		t.Fatal("restored service is not multi-field")
	}
}

func TestSaveServiceStoresPipelineIdentity(t *testing.T) {
	pipeline := textproc.ObservabilityPipeline()
	svc := fts.New(slicedradix.New(), fts.WordKeys, fts.WithPipeline(pipeline))
	if err := svc.Index(context.Background(), fts.Document{
		ID: "doc-1",
		Fields: map[string]fts.Field{
			fts.DefaultField: {Value: "pipeline identity"},
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
	if loaded.Pipelines[fts.DefaultField].Fingerprint == "" {
		t.Fatalf("Pipelines = %+v, want fingerprint", loaded.Pipelines)
	}
	if _, err := segmentbundle.RestoreService(loaded, fts.WordKeys); err == nil {
		t.Fatal("RestoreService() without pipeline error = nil, want pipeline descriptor error")
	}
	if _, err := segmentbundle.RestoreService(loaded, fts.WordKeys, fts.WithPipeline(textproc.DefaultEnglishPipeline())); err == nil {
		t.Fatal("RestoreService() with mismatched pipeline error = nil, want pipeline mismatch")
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
	if _, err := segmentbundle.RestoreService(loaded, fts.WordKeys, fts.WithPipeline(changed)); err == nil {
		t.Fatal("RestoreService() with changed configuration error = nil")
	}
	if _, err := segmentbundle.RestoreService(loaded, fts.WordKeys, fts.WithPipeline(pipeline)); err != nil {
		t.Fatalf("RestoreService() with matching pipeline error = %v", err)
	}
}

func TestMultiFieldBundleRestoreServiceRoundTrip(t *testing.T) {
	factory := func(name string) (fts.MutableIndex, error) { return slicedradix.New(), nil }
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
	descriptors, err := svc.PipelineDescriptors()
	if err != nil {
		t.Fatalf("PipelineDescriptors() error = %v", err)
	}

	fields, _ := svc.SnapshotFields()
	sources := make(map[string]posting.Exporter, len(fields))
	for fieldName, index := range fields {
		source, ok := index.(posting.Exporter)
		if !ok {
			t.Fatalf("field %q does not implement posting.Exporter", fieldName)
		}
		sources[fieldName] = source
	}

	var bundleBytes bytes.Buffer
	keyGenerator := fts.NewKeyGeneratorDescriptor("word-keys", 1)
	if err := segmentbundle.SaveMultiFieldBundle(&bundleBytes, sources, svc.SnapshotCollectionStats(), svc.SnapshotRegistry(), svc.SnapshotTombstones(), segmentbundle.BundleSaveOptions{Pipelines: descriptors, KeyGenerator: &keyGenerator}); err != nil {
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

	titleRes, err := restored.SearchQuery(ctx, fts.TermQuery{Field: "title", Term: "alpha"}, fts.SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("Search(title:alpha) error = %v", err)
	}
	if titleRes.TotalResultsCount != 0 {
		t.Fatalf("title results = %+v, want no tombstoned doc", titleRes.Results)
	}

	bodyRes, err := restored.SearchQuery(ctx, fts.TermQuery{Field: "body", Term: "alpha"}, fts.SearchOptions{MaxResults: 10})
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

func TestMultiFieldBundleStoresPerFieldPipelineIdentity(t *testing.T) {
	factory := func(name string) (fts.MutableIndex, error) { return slicedradix.New(), nil }
	english := textproc.DefaultEnglishPipeline()
	russian := textproc.DefaultRussianPipeline()
	svc := fts.NewMultiField(factory, fts.WordKeys,
		fts.WithPipeline(english),
		fts.WithFieldPipelines(map[string]fts.Pipeline{"body": russian}),
	)
	ctx := context.Background()
	if err := svc.Index(ctx, fts.Document{ID: "doc-1", Fields: map[string]fts.Field{
		"title": {Value: "running"},
		"body":  {Value: "машины"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	descriptors, err := svc.PipelineDescriptors()
	if err != nil || descriptors["title"].Fingerprint == descriptors["body"].Fingerprint {
		t.Fatalf("PipelineDescriptors() = %+v, want distinct field descriptors", descriptors)
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
	restored, err := segmentbundle.RestoreService(bundle, fts.WordKeys,
		fts.WithPipeline(english),
		fts.WithFieldPipelines(map[string]fts.Pipeline{"body": russian}),
	)
	if err != nil {
		t.Fatalf("RestoreService() with field pipelines error = %v", err)
	}
	for _, query := range []fts.TermQuery{
		{Field: "title", Term: "run"},
		{Field: "body", Term: "машина"},
	} {
		res, err := restored.SearchQuery(ctx, query, fts.SearchOptions{MaxResults: 10})
		if err != nil {
			t.Fatalf("Search(%s:%s) error = %v", query.Field, query.Term, err)
		}
		if res.TotalResultsCount != 1 || res.Results[0].ID != "doc-1" {
			t.Fatalf("Search(%s:%s) results = %+v, want doc-1", query.Field, query.Term, res.Results)
		}
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
