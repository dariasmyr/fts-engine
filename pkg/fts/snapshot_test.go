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

func TestSaveLoadSegmentSnapshotRoundTrip(t *testing.T) {
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

	var snap bytes.Buffer
	if err := svc.SaveSnapshot(&snap, indexCodecName, filterCodecName); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	reloaded, err := NewFromSnapshot(bytes.NewReader(snap.Bytes()), WordKeys)
	if err != nil {
		t.Fatalf("NewFromSnapshot() error = %v", err)
	}

	res, err := reloaded.SearchDocuments(context.Background(), "alpha", 10)
	if err != nil {
		t.Fatalf("SearchDocuments() error = %v", err)
	}

	if res.TotalResultsCount != 1 {
		t.Fatalf("TotalResultsCount = %d, want 1", res.TotalResultsCount)
	}
}

func TestSaveSegmentSnapshotUnknownCodec(t *testing.T) {
	var snap bytes.Buffer
	err := SaveSegmentSnapshot(&snap, "unknown", newSnapshotIndex(), "", nil)
	if err == nil {
		t.Fatal("SaveSegmentSnapshot() error = nil, want non-nil")
	}
}

func TestSaveSnapshotBufferedWritesPayload(t *testing.T) {
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

	var out bytes.Buffer
	if err := svc.SaveSnapshotBuffered(&out, indexCodecName, ""); err != nil {
		t.Fatalf("SaveSnapshotBuffered() error = %v", err)
	}
	if out.Len() == 0 {
		t.Fatal("SaveSnapshotBuffered() wrote empty payload")
	}
}

func TestSaveSnapshotBufferedAsync(t *testing.T) {
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

	var out bytes.Buffer
	if err := <-svc.SaveSnapshotBufferedAsync(&out, indexCodecName, ""); err != nil {
		t.Fatalf("SaveSnapshotBufferedAsync() error = %v", err)
	}
	if out.Len() == 0 {
		t.Fatal("SaveSnapshotBufferedAsync() wrote empty payload")
	}
}
