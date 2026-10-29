package fts

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

type snapshotIndex struct {
	data map[string][]Posting
}

func newSnapshotIndex() *snapshotIndex {
	return &snapshotIndex{data: make(map[string][]Posting)}
}

func (m *snapshotIndex) Insert(key string, ord DocOrd) error {
	rows := m.data[key]
	for i := range rows {
		if rows[i].Ord == ord {
			rows[i].Count++
			m.data[key] = rows
			return nil
		}
	}
	m.data[key] = append(rows, Posting{Ord: ord, Count: 1})
	return nil
}

func (m *snapshotIndex) Search(key string) ([]Posting, error) {
	return append([]Posting(nil), m.data[key]...), nil
}

func (m *snapshotIndex) Serialize(w io.Writer) error {
	return gob.NewEncoder(w).Encode(m.data)
}

func loadSnapshotIndex(r io.Reader) (Index, error) {
	out := newSnapshotIndex()
	if err := gob.NewDecoder(r).Decode(&out.data); err != nil {
		return nil, err
	}
	return out, nil
}

type snapshotFilter struct {
	set map[string]bool
}

func newSnapshotFilter() *snapshotFilter {
	return &snapshotFilter{set: make(map[string]bool)}
}

func (f *snapshotFilter) Add(item []byte) bool {
	f.set[string(item)] = true
	return true
}

func (f *snapshotFilter) Contains(item []byte) bool {
	return f.set[string(item)]
}

func (f *snapshotFilter) Serialize(w io.Writer) error {
	return gob.NewEncoder(w).Encode(f.set)
}

func loadSnapshotFilter(r io.Reader) (Filter, error) {
	out := newSnapshotFilter()
	if err := gob.NewDecoder(r).Decode(&out.set); err != nil {
		return nil, err
	}
	return out, nil
}

func TestSaveLoadSplitSnapshotsRoundTrip(t *testing.T) {
	indexCodecName := fmt.Sprintf("test-index-%s", t.Name())
	if err := RegisterIndexSnapshotCodec(indexCodecName,
		func(index Index, w io.Writer) error {
			return index.(Serializable).Serialize(w)
		},
		loadSnapshotIndex,
	); err != nil {
		t.Fatalf("RegisterIndexSnapshotCodec() error = %v", err)
	}

	filterCodecName := fmt.Sprintf("test-filter-%s", t.Name())
	if err := RegisterFilterSnapshotCodec(filterCodecName,
		func(filter Filter, w io.Writer) error {
			return filter.(Serializable).Serialize(w)
		},
		loadSnapshotFilter,
	); err != nil {
		t.Fatalf("RegisterFilterSnapshotCodec() error = %v", err)
	}

	idx := newSnapshotIndex()
	f := newSnapshotFilter()
	svc := New(idx, WordKeys, WithFilter(f))

	if err := indexDefaultDoc(context.Background(), svc, "doc-1", "alpha beta"); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	index, searchFilter := svc.SnapshotComponents()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()

	var indexSnap bytes.Buffer
	if err := SaveIndexSnapshotWithState(&indexSnap, indexCodecName, index, nil, registry, tombstones); err != nil {
		t.Fatalf("SaveIndexSnapshotWithState() error = %v", err)
	}

	var filterSnap bytes.Buffer
	if err := SaveFilterSnapshot(&filterSnap, filterCodecName, searchFilter); err != nil {
		t.Fatalf("SaveFilterSnapshot() error = %v", err)
	}

	loadedIndex, err := LoadIndexSnapshot(bytes.NewReader(indexSnap.Bytes()))
	if err != nil {
		t.Fatalf("LoadIndexSnapshot() error = %v", err)
	}

	loadedFilter, err := LoadFilterSnapshot(bytes.NewReader(filterSnap.Bytes()))
	if err != nil {
		t.Fatalf("LoadFilterSnapshot() error = %v", err)
	}

	reloaded := New(loadedIndex.Index, WordKeys,
		WithDocRegistrySnapshot(loadedIndex.Registry),
		WithTombstonesSnapshot(loadedIndex.Tombstones),
		WithFilter(loadedFilter.Filter),
	)

	res, err := reloaded.SearchText(context.Background(), "alpha", SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText() error = %v", err)
	}

	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}
}

func TestSaveLoadIndexSnapshotWithCollectionStatsRoundTrip(t *testing.T) {
	indexCodecName := fmt.Sprintf("test-index-stats-%s", t.Name())
	if err := RegisterIndexSnapshotCodec(indexCodecName,
		func(index Index, w io.Writer) error { return index.(Serializable).Serialize(w) },
		loadSnapshotIndex,
	); err != nil {
		t.Fatalf("RegisterIndexSnapshotCodec() error = %v", err)
	}

	svc := New(newSnapshotIndex(), WordKeys, WithScorer(BM25()))
	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "rosa barge"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	if err := indexDefaultDoc(ctx, svc, "doc-b", "barge barge"); err != nil {
		t.Fatalf("Index(doc-b) error = %v", err)
	}

	index, _ := svc.SnapshotComponents()
	stats := svc.SnapshotCollectionStats()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()

	var snap bytes.Buffer
	if err := SaveIndexSnapshotWithState(&snap, indexCodecName, index, stats, registry, tombstones); err != nil {
		t.Fatalf("SaveIndexSnapshotWithState() error = %v", err)
	}

	loaded, err := LoadIndexSnapshot(bytes.NewReader(snap.Bytes()))
	if err != nil {
		t.Fatalf("LoadIndexSnapshot() error = %v", err)
	}
	if loaded.CollectionStats == nil {
		t.Fatal("LoadIndexSnapshot() returned nil collection stats")
	}
	if got := loaded.CollectionStats.DocLen[DefaultField]["doc-a"]; got != 2 {
		t.Fatalf("loaded doc length = %d, want 2", got)
	}

	restored := New(loaded.Index, WordKeys,
		WithScorer(BM25()),
		WithDocRegistrySnapshot(loaded.Registry),
		WithTombstonesSnapshot(loaded.Tombstones),
		WithCollectionStatsSnapshot(loaded.CollectionStats),
	)
	res, err := restored.SearchText(ctx, "rosa barge", SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText() after restore error = %v", err)
	}
	if len(res.Results) < 2 || res.Results[0].ID != "doc-a" {
		t.Fatalf("expected doc-a to rank first after restore, got %+v", res.Results)
	}
	if res.Results[0].Score <= 0 {
		t.Fatalf("expected positive score after restore, got %+v", res.Results[0])
	}
}

func TestSaveIndexSnapshotUnknownCodec(t *testing.T) {
	var snap bytes.Buffer
	err := SaveIndexSnapshotWithState(&snap, "unknown", newSnapshotIndex(), nil, nil, nil)
	if err == nil {
		t.Fatal("SaveIndexSnapshotWithState() error = nil, want non-nil")
	}
}

func TestLoadIndexSnapshotRejectsInvalidPipelineMetadata(t *testing.T) {
	valid, _ := NewPipelineDescriptor("default", testFingerprint("default"))
	tests := []struct {
		name                string
		pipelines           map[string]PipelineDescriptor
		wantRebuildRequired bool
	}{
		{name: "missing", pipelines: nil},
		{name: "malformed", pipelines: map[string]PipelineDescriptor{DefaultField: {Name: valid.Name, Fingerprint: "invalid"}}},
		{name: "legacy", pipelines: map[string]PipelineDescriptor{DefaultField: {Name: valid.Name, Fingerprint: legacyTestFingerprint(valid.Name, 1)}}, wantRebuildRequired: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var payload bytes.Buffer
			if err := gob.NewEncoder(&payload).Encode(indexEnvelope{
				Version: indexSnapshotVersion, Kind: snapshotKindSingle, IndexName: "unused", Pipelines: test.pipelines,
			}); err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			_, err := testSnapshotRegistry.LoadIndexSnapshot(&payload)
			if err == nil {
				t.Fatal("LoadIndexSnapshot() error = nil")
			}
			if got := errors.Is(err, ErrPersistenceRebuildRequired); got != test.wantRebuildRequired {
				t.Fatalf("errors.Is(ErrPersistenceRebuildRequired) = %t, want %t; error=%v", got, test.wantRebuildRequired, err)
			}
		})
	}
}

func TestLoadIndexSnapshotRejectsLegacyVersion(t *testing.T) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(legacyIndexEnvelopeFixture{
		Version: indexSnapshotVersion - 1,
		Analyzers: map[string]legacyAnalyzerDescriptorFixture{
			DefaultField: {Name: "default", Fingerprint: legacyTestFingerprint("default", 1), FingerprintScheme: 1},
		},
	}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if _, err := testSnapshotRegistry.LoadIndexSnapshot(&payload); err == nil || !errors.Is(err, ErrPersistenceRebuildRequired) || !strings.Contains(err.Error(), "legacy version 4 uses analyzer metadata") {
		t.Fatalf("LoadIndexSnapshot() error = %v, want legacy analyzer rebuild guidance", err)
	}
}

func TestLoadMultiIndexSnapshotAllowsNoFields(t *testing.T) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(multiIndexEnvelope{Version: multiIndexSnapshotVersion, Kind: snapshotKindMulti}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	loaded, err := testSnapshotRegistry.LoadMultiIndexSnapshot(&payload)
	if err != nil {
		t.Fatalf("LoadMultiIndexSnapshot() error = %v", err)
	}
	if len(loaded.Fields) != 0 {
		t.Fatalf("len(Fields) = %d, want 0", len(loaded.Fields))
	}
}

func TestLoadMultiIndexSnapshotRejectsLegacyPipeline(t *testing.T) {
	legacy := PipelineDescriptor{Name: "default", Fingerprint: legacyTestFingerprint("default", 1)}
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(multiIndexEnvelope{
		Version:   multiIndexSnapshotVersion,
		Kind:      snapshotKindMulti,
		Fields:    []multiIndexField{{FieldName: DefaultField, IndexName: "unused"}},
		Pipelines: map[string]PipelineDescriptor{DefaultField: legacy},
	}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if _, err := testSnapshotRegistry.LoadMultiIndexSnapshot(&payload); err == nil || !errors.Is(err, ErrPersistenceRebuildRequired) {
		t.Fatalf("LoadMultiIndexSnapshot() error = %v", err)
	}
}

func TestLoadMultiIndexSnapshotRejectsLegacyVersion(t *testing.T) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(legacyMultiIndexEnvelopeFixture{
		Version: multiIndexSnapshotVersion - 1,
		Analyzers: map[string]legacyAnalyzerDescriptorFixture{
			DefaultField: {Name: "default", Fingerprint: legacyTestFingerprint("default", 1), FingerprintScheme: 1},
		},
	}); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if _, err := testSnapshotRegistry.LoadMultiIndexSnapshot(&payload); err == nil || !errors.Is(err, ErrPersistenceRebuildRequired) || !strings.Contains(err.Error(), "legacy version 5 uses analyzer metadata") {
		t.Fatalf("LoadMultiIndexSnapshot() error = %v, want legacy analyzer rebuild guidance", err)
	}
}

func TestLoadIndexSnapshotVersionClassification(t *testing.T) {
	tests := []struct {
		name                string
		version             uint16
		wantRebuildRequired bool
	}{
		{name: "older legacy", version: 1, wantRebuildRequired: true},
		{name: "zero", version: 0},
		{name: "future", version: indexSnapshotVersion + 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var payload bytes.Buffer
			if err := gob.NewEncoder(&payload).Encode(indexEnvelope{Version: test.version}); err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			_, err := testSnapshotRegistry.LoadIndexSnapshot(&payload)
			if err == nil {
				t.Fatal("LoadIndexSnapshot() error = nil")
			}
			if got := errors.Is(err, ErrPersistenceRebuildRequired); got != test.wantRebuildRequired {
				t.Fatalf("errors.Is(ErrPersistenceRebuildRequired) = %t, want %t; error=%v", got, test.wantRebuildRequired, err)
			}
		})
	}
}

type legacyAnalyzerDescriptorFixture struct {
	Name              string
	Fingerprint       string
	FingerprintScheme uint32
}

type legacyIndexEnvelopeFixture struct {
	Version   uint16
	Analyzers map[string]legacyAnalyzerDescriptorFixture
}

type legacyMultiIndexEnvelopeFixture struct {
	Version   uint16
	Analyzers map[string]legacyAnalyzerDescriptorFixture
}

func TestSaveIndexSnapshotWritesPayload(t *testing.T) {
	indexCodecName := fmt.Sprintf("test-index-%s", t.Name())
	if err := RegisterIndexSnapshotCodec(indexCodecName,
		func(index Index, w io.Writer) error { return index.(Serializable).Serialize(w) },
		loadSnapshotIndex,
	); err != nil {
		t.Fatalf("RegisterIndexSnapshotCodec() error = %v", err)
	}

	svc := New(newSnapshotIndex(), WordKeys)
	if err := indexDefaultDoc(context.Background(), svc, "doc-1", "alpha"); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}

	index, _ := svc.SnapshotComponents()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()

	var out bytes.Buffer
	if err := SaveIndexSnapshotWithState(&out, indexCodecName, index, nil, registry, tombstones); err != nil {
		t.Fatalf("SaveIndexSnapshotWithState() error = %v", err)
	}
	if out.Len() == 0 {
		t.Fatal("SaveIndexSnapshotWithState() wrote empty payload")
	}
}

func TestSaveLoadIndexSnapshotPreservesRegistryAndTombstones(t *testing.T) {
	indexCodecName := fmt.Sprintf("test-index-delete-%s", t.Name())
	if err := RegisterIndexSnapshotCodec(indexCodecName,
		func(index Index, w io.Writer) error { return index.(Serializable).Serialize(w) },
		loadSnapshotIndex,
	); err != nil {
		t.Fatalf("RegisterIndexSnapshotCodec() error = %v", err)
	}

	svc := New(newSnapshotIndex(), WordKeys)
	ctx := context.Background()
	if err := indexDefaultDoc(ctx, svc, "doc-a", "alpha stale"); err != nil {
		t.Fatalf("Index(doc-a) error = %v", err)
	}
	if err := indexDefaultDoc(ctx, svc, "doc-b", "alpha live"); err != nil {
		t.Fatalf("Index(doc-b) error = %v", err)
	}
	if !svc.Delete("doc-a") {
		t.Fatal("Delete(doc-a) = false, want true")
	}

	index, _ := svc.SnapshotComponents()
	var snap bytes.Buffer
	if err := SaveIndexSnapshotWithState(&snap, indexCodecName, index, svc.SnapshotCollectionStats(), svc.SnapshotRegistry(), svc.SnapshotTombstones()); err != nil {
		t.Fatalf("SaveIndexSnapshotWithState() error = %v", err)
	}

	loaded, err := LoadIndexSnapshot(bytes.NewReader(snap.Bytes()))
	if err != nil {
		t.Fatalf("LoadIndexSnapshot() error = %v", err)
	}
	if len(loaded.Registry) == 0 {
		t.Fatal("LoadIndexSnapshot() returned empty registry snapshot")
	}
	if len(loaded.Tombstones) == 0 {
		t.Fatal("LoadIndexSnapshot() returned empty tombstones snapshot")
	}

	restored := New(loaded.Index, WordKeys,
		WithDocRegistrySnapshot(loaded.Registry),
		WithTombstonesSnapshot(loaded.Tombstones),
		WithCollectionStatsSnapshot(loaded.CollectionStats),
	)

	res, err := restored.SearchText(ctx, "alpha", SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText(alpha) after restore error = %v", err)
	}
	if len(res.Results) != 1 || res.Results[0].ID != "doc-b" {
		t.Fatalf("expected only live doc after restore, got %+v", res.Results)
	}

	stale, err := restored.SearchText(ctx, "stale", SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("SearchText(stale) after restore error = %v", err)
	}
	if len(stale.Results) != 0 {
		t.Fatalf("deleted document leaked after restore: %+v", stale.Results)
	}
}

func TestSaveLoadMultiIndexSnapshotRoundTrip(t *testing.T) {
	codecName := fmt.Sprintf("test-multi-index-%s", t.Name())
	if err := RegisterIndexSnapshotCodec(codecName,
		func(index Index, w io.Writer) error { return index.(Serializable).Serialize(w) },
		loadSnapshotIndex,
	); err != nil {
		t.Fatalf("RegisterIndexSnapshotCodec() error = %v", err)
	}

	svc := NewMultiField(
		func(name string) (MutableIndex, error) { return newSnapshotIndex(), nil },
		WordKeys,
	)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-1", Fields: map[string]Field{
		"title": {Value: "rosa barge"},
		"body":  {Value: "french canal"},
	}}); err != nil {
		t.Fatalf("Index() error = %v", err)
	}

	indexes, _ := svc.SnapshotFields()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()
	if got := len(indexes); got != 2 {
		t.Fatalf("len(SnapshotFields()) = %d, want 2", got)
	}

	codecs := map[string]string{"body": codecName, "title": codecName}

	var snap bytes.Buffer
	if err := SaveMultiIndexSnapshotWithState(&snap, codecs, indexes, nil, registry, tombstones); err != nil {
		t.Fatalf("SaveMultiIndexSnapshotWithState() error = %v", err)
	}
	if snap.Len() == 0 {
		t.Fatal("SaveMultiIndexSnapshotWithState() wrote empty payload")
	}

	loaded, err := LoadMultiIndexSnapshot(bytes.NewReader(snap.Bytes()))
	if err != nil {
		t.Fatalf("LoadMultiIndexSnapshot() error = %v", err)
	}
	if got := len(loaded.Fields); got != 2 {
		t.Fatalf("len(loaded.Fields) = %d, want 2", got)
	}

	restoredIndexes := make(map[string]Index, len(loaded.Fields))
	for fieldName, snapshot := range loaded.Fields {
		if snapshot.IndexName != codecName {
			t.Fatalf("loaded codec for field %q = %q, want %q", fieldName, snapshot.IndexName, codecName)
		}
		restoredIndexes[fieldName] = snapshot.Index
	}

	restored := NewMultiFieldFromIndexes(restoredIndexes, WordKeys,
		WithDocRegistrySnapshot(loaded.Registry),
		WithTombstonesSnapshot(loaded.Tombstones),
	)

	titleRes, err := restored.SearchQuery(ctx, TermQuery{Field: "title", Term: "rosa"}, SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("Search(title:rosa) error = %v", err)
	}
	if titleRes.TotalResultsCount != 1 || len(titleRes.Results) != 1 || titleRes.Results[0].ID != "doc-1" {
		t.Fatalf("restored title search = %+v, want doc-1", titleRes.Results)
	}

	bodyRes, err := restored.SearchQuery(ctx, TermQuery{Field: "body", Term: "french"}, SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("Search(body:french) error = %v", err)
	}
	if bodyRes.TotalResultsCount != 1 || len(bodyRes.Results) != 1 || bodyRes.Results[0].ID != "doc-1" {
		t.Fatalf("restored body search = %+v, want doc-1", bodyRes.Results)
	}
}

func TestSaveLoadMultiIndexSnapshotWithCollectionStatsRoundTrip(t *testing.T) {
	codecName := fmt.Sprintf("test-multi-index-stats-%s", t.Name())
	if err := RegisterIndexSnapshotCodec(codecName,
		func(index Index, w io.Writer) error { return index.(Serializable).Serialize(w) },
		loadSnapshotIndex,
	); err != nil {
		t.Fatalf("RegisterIndexSnapshotCodec() error = %v", err)
	}

	svc := NewMultiField(
		func(name string) (MutableIndex, error) { return newSnapshotIndex(), nil },
		WordKeys,
		WithScorer(BM25()),
	)

	ctx := context.Background()
	if err := svc.Index(ctx, Document{ID: "doc-1", Fields: map[string]Field{
		"title": {Value: "rosa barge"},
		"body":  {Value: "french canal"},
	}}); err != nil {
		t.Fatalf("Index(doc-1) error = %v", err)
	}
	if err := svc.Index(ctx, Document{ID: "doc-2", Fields: map[string]Field{
		"title": {Value: "barge"},
	}}); err != nil {
		t.Fatalf("Index(doc-2) error = %v", err)
	}

	indexes, _ := svc.SnapshotFields()
	stats := svc.SnapshotCollectionStats()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()
	codecs := map[string]string{"body": codecName, "title": codecName}

	var snap bytes.Buffer
	if err := SaveMultiIndexSnapshotWithState(&snap, codecs, indexes, stats, registry, tombstones); err != nil {
		t.Fatalf("SaveMultiIndexSnapshotWithState() error = %v", err)
	}

	loaded, err := LoadMultiIndexSnapshot(bytes.NewReader(snap.Bytes()))
	if err != nil {
		t.Fatalf("LoadMultiIndexSnapshot() error = %v", err)
	}
	if loaded.CollectionStats == nil {
		t.Fatal("LoadMultiIndexSnapshot() returned nil collection stats")
	}
	if got := loaded.CollectionStats.DocLen["title"]["doc-1"]; got != 2 {
		t.Fatalf("loaded title doc length = %d, want 2", got)
	}
	if got := len(loaded.CollectionStats.DocLen["body"]); got != 1 {
		t.Fatalf("loaded body doc count = %d, want 1", got)
	}

	restoredIndexes := make(map[string]Index, len(loaded.Fields))
	for fieldName, snapshot := range loaded.Fields {
		restoredIndexes[fieldName] = snapshot.Index
	}

	restored := NewMultiFieldFromIndexes(restoredIndexes, WordKeys,
		WithScorer(BM25()),
		WithDocRegistrySnapshot(loaded.Registry),
		WithTombstonesSnapshot(loaded.Tombstones),
		WithCollectionStatsSnapshot(loaded.CollectionStats),
	)
	res, err := restored.SearchQuery(ctx, TermQuery{Field: "title", Term: "rosa barge"}, SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatalf("Search(title:rosa barge) error = %v", err)
	}
	if len(res.Results) < 2 || res.Results[0].ID != "doc-1" {
		t.Fatalf("expected doc-1 to rank first after multi-field restore, got %+v", res.Results)
	}
	if res.Results[0].Score <= 0 {
		t.Fatalf("expected positive score after multi-field restore, got %+v", res.Results[0])
	}
}

func TestSaveMultiIndexSnapshotUnknownCodec(t *testing.T) {
	err := SaveMultiIndexSnapshotWithState(&bytes.Buffer{}, map[string]string{"title": "unknown"}, map[string]Index{"title": newSnapshotIndex()}, nil, nil, nil)
	if err == nil {
		t.Fatal("SaveMultiIndexSnapshotWithState() error = nil, want non-nil")
	}
}

func TestLoadMultiIndexSnapshotRejectsEmptyFieldName(t *testing.T) {
	var snap bytes.Buffer
	if err := gob.NewEncoder(&snap).Encode(multiIndexEnvelope{
		Version: multiIndexSnapshotVersion,
		Kind:    snapshotKindMulti,
		Fields:  []multiIndexField{{FieldName: "", IndexName: "codec", Payload: []byte("payload")}},
	}); err != nil {
		t.Fatalf("encode multiIndexEnvelope: %v", err)
	}

	if _, err := LoadMultiIndexSnapshot(bytes.NewReader(snap.Bytes())); err == nil {
		t.Fatal("LoadMultiIndexSnapshot() error = nil, want non-nil")
	}
}
