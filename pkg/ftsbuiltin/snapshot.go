package ftsbuiltin

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/dariasmyr/fts-engine/pkg/filter"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/hamtpointered"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/index/trigram"
)

const snapshotVersion uint16 = 1

type snapshotEnvelope struct {
	Version       uint16
	IndexName     string
	IndexPayload  []byte
	FilterName    string
	FilterPayload []byte
}

type indexEnvelope struct {
	Version      uint16
	IndexName    string
	IndexPayload []byte
}

type filterEnvelope struct {
	Version       uint16
	FilterName    string
	FilterPayload []byte
}

type LoadedIndexSnapshot struct {
	IndexName string
	Index     fts.Index
}

type LoadedFilterSnapshot struct {
	FilterName string
	Filter     fts.Filter
}

func SaveServiceSnapshot(w io.Writer, svc *fts.Service, indexName string, filterName string) error {
	if svc == nil {
		return fmt.Errorf("ftsbuiltin: save snapshot: nil service")
	}

	index, searchFilter := svc.SnapshotComponents()
	return saveSegmentSnapshot(w, indexName, index, filterName, searchFilter)
}

func saveSegmentSnapshot(w io.Writer, indexName string, index fts.Index, filterName string, searchFilter fts.Filter) error {
	if w == nil {
		return fmt.Errorf("ftsbuiltin: save snapshot: nil writer")
	}
	if index == nil {
		return fmt.Errorf("ftsbuiltin: save snapshot: nil index")
	}
	if indexName == "" {
		return fmt.Errorf("ftsbuiltin: save snapshot: empty index name")
	}

	var indexPayload bytes.Buffer
	if err := serializeIndex(indexName, index, &indexPayload); err != nil {
		return fmt.Errorf("ftsbuiltin: save snapshot: encode index %q: %w", indexName, err)
	}

	envelope := snapshotEnvelope{
		Version:      snapshotVersion,
		IndexName:    indexName,
		IndexPayload: indexPayload.Bytes(),
	}

	if searchFilter != nil {
		if filterName == "" {
			return fmt.Errorf("ftsbuiltin: save snapshot: empty filter name")
		}

		var filterPayload bytes.Buffer
		if err := serializeFilter(filterName, searchFilter, &filterPayload); err != nil {
			return fmt.Errorf("ftsbuiltin: save snapshot: encode filter %q: %w", filterName, err)
		}

		envelope.FilterName = filterName
		envelope.FilterPayload = filterPayload.Bytes()
	}

	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("ftsbuiltin: save snapshot: encode envelope: %w", err)
	}

	return nil
}

func LoadSegmentSnapshot(r io.Reader) (*fts.LoadedSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("ftsbuiltin: load snapshot: nil reader")
	}

	var envelope snapshotEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("ftsbuiltin: load snapshot: decode envelope: %w", err)
	}

	if envelope.Version != snapshotVersion {
		return nil, fmt.Errorf("ftsbuiltin: load snapshot: unsupported version %d", envelope.Version)
	}
	if envelope.IndexName == "" {
		return nil, fmt.Errorf("ftsbuiltin: load snapshot: empty index name")
	}

	index, err := loadIndex(envelope.IndexName, bytes.NewReader(envelope.IndexPayload))
	if err != nil {
		return nil, fmt.Errorf("ftsbuiltin: load snapshot: decode index %q: %w", envelope.IndexName, err)
	}

	loaded := &fts.LoadedSnapshot{
		IndexName: envelope.IndexName,
		Index:     index,
	}

	if envelope.FilterName != "" {
		searchFilter, loadErr := loadFilter(envelope.FilterName, bytes.NewReader(envelope.FilterPayload))
		if loadErr != nil {
			return nil, fmt.Errorf("ftsbuiltin: load snapshot: decode filter %q: %w", envelope.FilterName, loadErr)
		}

		loaded.FilterName = envelope.FilterName
		loaded.Filter = searchFilter
	}

	return loaded, nil
}

func SaveIndexSnapshot(w io.Writer, indexName string, index fts.Index) error {
	if w == nil {
		return fmt.Errorf("ftsbuiltin: save index snapshot: nil writer")
	}
	if index == nil {
		return fmt.Errorf("ftsbuiltin: save index snapshot: nil index")
	}
	if indexName == "" {
		return fmt.Errorf("ftsbuiltin: save index snapshot: empty index name")
	}

	var indexPayload bytes.Buffer
	if err := serializeIndex(indexName, index, &indexPayload); err != nil {
		return fmt.Errorf("ftsbuiltin: save index snapshot: encode index %q: %w", indexName, err)
	}

	envelope := indexEnvelope{
		Version:      snapshotVersion,
		IndexName:    indexName,
		IndexPayload: indexPayload.Bytes(),
	}

	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("ftsbuiltin: save index snapshot: encode envelope: %w", err)
	}

	return nil
}

func LoadIndexSnapshot(r io.Reader) (*LoadedIndexSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("ftsbuiltin: load index snapshot: nil reader")
	}

	var envelope indexEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("ftsbuiltin: load index snapshot: decode envelope: %w", err)
	}

	if envelope.Version != snapshotVersion {
		return nil, fmt.Errorf("ftsbuiltin: load index snapshot: unsupported version %d", envelope.Version)
	}
	if envelope.IndexName == "" {
		return nil, fmt.Errorf("ftsbuiltin: load index snapshot: empty index name")
	}

	index, err := loadIndex(envelope.IndexName, bytes.NewReader(envelope.IndexPayload))
	if err != nil {
		return nil, fmt.Errorf("ftsbuiltin: load index snapshot: decode index %q: %w", envelope.IndexName, err)
	}

	return &LoadedIndexSnapshot{IndexName: envelope.IndexName, Index: index}, nil
}

func SaveFilterSnapshot(w io.Writer, filterName string, searchFilter fts.Filter) error {
	if w == nil {
		return fmt.Errorf("ftsbuiltin: save filter snapshot: nil writer")
	}
	if searchFilter == nil {
		return fmt.Errorf("ftsbuiltin: save filter snapshot: nil filter")
	}
	if filterName == "" {
		return fmt.Errorf("ftsbuiltin: save filter snapshot: empty filter name")
	}

	var filterPayload bytes.Buffer
	if err := serializeFilter(filterName, searchFilter, &filterPayload); err != nil {
		return fmt.Errorf("ftsbuiltin: save filter snapshot: encode filter %q: %w", filterName, err)
	}

	envelope := filterEnvelope{
		Version:       snapshotVersion,
		FilterName:    filterName,
		FilterPayload: filterPayload.Bytes(),
	}

	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("ftsbuiltin: save filter snapshot: encode envelope: %w", err)
	}

	return nil
}

func LoadFilterSnapshot(r io.Reader) (*LoadedFilterSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("ftsbuiltin: load filter snapshot: nil reader")
	}

	var envelope filterEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("ftsbuiltin: load filter snapshot: decode envelope: %w", err)
	}

	if envelope.Version != snapshotVersion {
		return nil, fmt.Errorf("ftsbuiltin: load filter snapshot: unsupported version %d", envelope.Version)
	}
	if envelope.FilterName == "" {
		return nil, fmt.Errorf("ftsbuiltin: load filter snapshot: empty filter name")
	}

	searchFilter, err := loadFilter(envelope.FilterName, bytes.NewReader(envelope.FilterPayload))
	if err != nil {
		return nil, fmt.Errorf("ftsbuiltin: load filter snapshot: decode filter %q: %w", envelope.FilterName, err)
	}

	return &LoadedFilterSnapshot{FilterName: envelope.FilterName, Filter: searchFilter}, nil
}

func serializeIndex(name string, index fts.Index, w io.Writer) error {
	serializable, ok := index.(fts.Serializable)
	if !ok {
		return fmt.Errorf("index %q does not support serialization", name)
	}

	switch name {
	case "radix", "slicedradix", "hamt", "hamtpointered", "trigram":
		return serializable.Serialize(w)
	default:
		return fmt.Errorf("unknown index codec %q", name)
	}
}

func serializeFilter(name string, value fts.Filter, w io.Writer) error {
	serializable, ok := value.(fts.Serializable)
	if !ok {
		return fmt.Errorf("filter %q does not support serialization", name)
	}

	switch name {
	case "bloom", "cuckoo", "ribbon":
		return serializable.Serialize(w)
	default:
		return fmt.Errorf("unknown filter codec %q", name)
	}
}

func loadIndex(name string, r io.Reader) (fts.Index, error) {
	switch name {
	case "radix":
		return radix.Load(r)
	case "slicedradix":
		return slicedradix.Load(r)
	case "hamt":
		return hamt.Load(r)
	case "hamtpointered":
		return hamtpointered.Load(r)
	case "trigram":
		return trigram.Load(r)
	default:
		return nil, fmt.Errorf("unknown index codec %q", name)
	}
}

func loadFilter(name string, r io.Reader) (fts.Filter, error) {
	switch name {
	case "bloom":
		return filter.LoadBloomFilter(r)
	case "cuckoo":
		return filter.LoadCuckooFilter(r)
	case "ribbon":
		rf, err := filter.LoadRibbonFilter(r)
		if err != nil {
			return nil, err
		}

		wrapped := fts.NewBufferedStaticFilter(rf)
		if err = wrapped.Build(); err != nil {
			return nil, err
		}

		return wrapped, nil
	default:
		return nil, fmt.Errorf("unknown filter codec %q", name)
	}
}
