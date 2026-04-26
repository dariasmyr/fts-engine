package fts

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"testing"
)

type snapshotIndex struct {
	data map[string][]DocRef
}

func newSnapshotIndex() *snapshotIndex {
	return &snapshotIndex{data: make(map[string][]DocRef)}
}

func (m *snapshotIndex) Insert(key string, id DocID) error {
	rows := m.data[key]
	for i := range rows {
		if rows[i].ID == id {
			rows[i].Count++
			m.data[key] = rows
			return nil
		}
	}
	m.data[key] = append(rows, DocRef{ID: id, Count: 1})
	return nil
}

func (m *snapshotIndex) Search(key string) ([]DocRef, error) {
	return append([]DocRef(nil), m.data[key]...), nil
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

	if err := svc.IndexDocument(context.Background(), "doc-1", "alpha beta"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	index, searchFilter := svc.SnapshotComponents()

	var indexSnap bytes.Buffer
	if err := SaveIndexSnapshot(&indexSnap, indexCodecName, index); err != nil {
		t.Fatalf("SaveIndexSnapshot() error = %v", err)
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

	reloaded := New(loadedIndex.Index, WordKeys, WithFilter(loadedFilter.Filter))

	res, err := reloaded.SearchDocuments(context.Background(), "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	if got, want := res.TotalResultsCount, 1; got != want {
		t.Fatalf("TotalResultsCount = %d, want %d", got, want)
	}
}

func TestSaveIndexSnapshotUnknownCodec(t *testing.T) {
	var snap bytes.Buffer
	err := SaveIndexSnapshot(&snap, "unknown", newSnapshotIndex())
	if err == nil {
		t.Fatal("SaveIndexSnapshot() error = nil, want non-nil")
	}
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
	if err := svc.IndexDocument(context.Background(), "doc-1", "alpha"); err != nil {
		t.Fatalf("IndexDocument() error = %v", err)
	}

	index, _ := svc.SnapshotComponents()

	var out bytes.Buffer
	if err := SaveIndexSnapshot(&out, indexCodecName, index); err != nil {
		t.Fatalf("SaveIndexSnapshot() error = %v", err)
	}
	if out.Len() == 0 {
		t.Fatal("SaveIndexSnapshot() wrote empty payload")
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
		func(name string) (Index, error) { return newSnapshotIndex(), nil },
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
	if got := len(indexes); got != 2 {
		t.Fatalf("len(SnapshotFields()) = %d, want 2", got)
	}

	codecs := map[string]string{"body": codecName, "title": codecName}

	var snap bytes.Buffer
	if err := SaveMultiIndexSnapshot(&snap, codecs, indexes); err != nil {
		t.Fatalf("SaveMultiIndexSnapshot() error = %v", err)
	}
	if snap.Len() == 0 {
		t.Fatal("SaveMultiIndexSnapshot() wrote empty payload")
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

	restored := NewMultiFieldFromIndexes(restoredIndexes, WordKeys)

	titleRes, err := restored.Search(ctx, TermQuery{Field: "title", Term: "rosa"}, 10)
	if err != nil {
		t.Fatalf("Search(title:rosa) error = %v", err)
	}
	if titleRes.TotalResultsCount != 1 || len(titleRes.Results) != 1 || titleRes.Results[0].ID != "doc-1" {
		t.Fatalf("restored title search = %+v, want doc-1", titleRes.Results)
	}

	bodyRes, err := restored.Search(ctx, TermQuery{Field: "body", Term: "french"}, 10)
	if err != nil {
		t.Fatalf("Search(body:french) error = %v", err)
	}
	if bodyRes.TotalResultsCount != 1 || len(bodyRes.Results) != 1 || bodyRes.Results[0].ID != "doc-1" {
		t.Fatalf("restored body search = %+v, want doc-1", bodyRes.Results)
	}
}

func TestSaveMultiIndexSnapshotUnknownCodec(t *testing.T) {
	err := SaveMultiIndexSnapshot(&bytes.Buffer{}, map[string]string{"title": "unknown"}, map[string]Index{"title": newSnapshotIndex()})
	if err == nil {
		t.Fatal("SaveMultiIndexSnapshot() error = nil, want non-nil")
	}
}

func TestLoadMultiIndexSnapshotRejectsEmptyFieldName(t *testing.T) {
	var snap bytes.Buffer
	if err := gob.NewEncoder(&snap).Encode(multiIndexEnvelope{
		Version: multiIndexSnapshotVersion,
		Fields:  []multiIndexField{{FieldName: "", IndexName: "codec", Payload: []byte("payload")}},
	}); err != nil {
		t.Fatalf("encode multiIndexEnvelope: %v", err)
	}

	if _, err := LoadMultiIndexSnapshot(bytes.NewReader(snap.Bytes())); err == nil {
		t.Fatal("LoadMultiIndexSnapshot() error = nil, want non-nil")
	}
}
