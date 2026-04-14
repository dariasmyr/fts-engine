package fts

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sync"
)

const snapshotVersion uint16 = 1

type IndexSnapshotSaver func(index Index, w io.Writer) error
type IndexSnapshotLoader func(r io.Reader) (Index, error)

type FilterSnapshotSaver func(filter Filter, w io.Writer) error
type FilterSnapshotLoader func(r io.Reader) (Filter, error)

type LoadedIndexSnapshot struct {
	IndexName string
	Index     Index
}

type LoadedFilterSnapshot struct {
	FilterName string
	Filter     Filter
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

var (
	snapshotRegistryMu   sync.RWMutex
	indexSnapshotCodecs  = make(map[string]indexSnapshotCodec)
	filterSnapshotCodecs = make(map[string]filterSnapshotCodec)
)

type indexSnapshotCodec struct {
	save IndexSnapshotSaver
	load IndexSnapshotLoader
}

type filterSnapshotCodec struct {
	save FilterSnapshotSaver
	load FilterSnapshotLoader
}

func RegisterIndexSnapshotCodec(name string, saver IndexSnapshotSaver, loader IndexSnapshotLoader) error {
	if name == "" {
		return fmt.Errorf("fts: register index snapshot codec: empty name")
	}
	if saver == nil {
		return fmt.Errorf("fts: register index snapshot codec: nil saver")
	}
	if loader == nil {
		return fmt.Errorf("fts: register index snapshot codec: nil loader")
	}

	snapshotRegistryMu.Lock()
	defer snapshotRegistryMu.Unlock()

	if _, exists := indexSnapshotCodecs[name]; exists {
		return fmt.Errorf("fts: register index snapshot codec: duplicate name %q", name)
	}

	indexSnapshotCodecs[name] = indexSnapshotCodec{save: saver, load: loader}
	return nil
}

func RegisterFilterSnapshotCodec(name string, saver FilterSnapshotSaver, loader FilterSnapshotLoader) error {
	if name == "" {
		return fmt.Errorf("fts: register filter snapshot codec: empty name")
	}
	if saver == nil {
		return fmt.Errorf("fts: register filter snapshot codec: nil saver")
	}
	if loader == nil {
		return fmt.Errorf("fts: register filter snapshot codec: nil loader")
	}

	snapshotRegistryMu.Lock()
	defer snapshotRegistryMu.Unlock()

	if _, exists := filterSnapshotCodecs[name]; exists {
		return fmt.Errorf("fts: register filter snapshot codec: duplicate name %q", name)
	}

	filterSnapshotCodecs[name] = filterSnapshotCodec{save: saver, load: loader}
	return nil
}

func SaveIndexSnapshot(w io.Writer, indexName string, index Index) error {
	if w == nil {
		return fmt.Errorf("fts: save index snapshot: nil writer")
	}
	if index == nil {
		return fmt.Errorf("fts: save index snapshot: nil index")
	}
	if indexName == "" {
		return fmt.Errorf("fts: save index snapshot: empty index name")
	}

	indexCodec, ok := indexCodecByName(indexName)
	if !ok {
		return fmt.Errorf("fts: save index snapshot: unknown index codec %q", indexName)
	}

	var indexPayload bytes.Buffer
	if err := indexCodec.save(index, &indexPayload); err != nil {
		return fmt.Errorf("fts: save index snapshot: encode index %q: %w", indexName, err)
	}

	envelope := indexEnvelope{
		Version:      snapshotVersion,
		IndexName:    indexName,
		IndexPayload: indexPayload.Bytes(),
	}

	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("fts: save index snapshot: encode envelope: %w", err)
	}

	return nil
}

func LoadIndexSnapshot(r io.Reader) (*LoadedIndexSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("fts: load index snapshot: nil reader")
	}

	var envelope indexEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("fts: load index snapshot: decode envelope: %w", err)
	}

	if envelope.Version != snapshotVersion {
		return nil, fmt.Errorf("fts: load index snapshot: unsupported version %d", envelope.Version)
	}
	if envelope.IndexName == "" {
		return nil, fmt.Errorf("fts: load index snapshot: empty index name")
	}

	indexCodec, ok := indexCodecByName(envelope.IndexName)
	if !ok {
		return nil, fmt.Errorf("fts: load index snapshot: unknown index codec %q", envelope.IndexName)
	}

	index, err := indexCodec.load(bytes.NewReader(envelope.IndexPayload))
	if err != nil {
		return nil, fmt.Errorf("fts: load index snapshot: decode index %q: %w", envelope.IndexName, err)
	}

	return &LoadedIndexSnapshot{IndexName: envelope.IndexName, Index: index}, nil
}

func SaveFilterSnapshot(w io.Writer, filterName string, filter Filter) error {
	if w == nil {
		return fmt.Errorf("fts: save filter snapshot: nil writer")
	}
	if filter == nil {
		return fmt.Errorf("fts: save filter snapshot: nil filter")
	}
	if filterName == "" {
		return fmt.Errorf("fts: save filter snapshot: empty filter name")
	}

	if buildable, ok := filter.(BuildableFilter); ok {
		if err := buildable.Build(); err != nil {
			return fmt.Errorf("fts: save filter snapshot: build filter %q: %w", filterName, err)
		}
	}

	filterCodec, ok := filterCodecByName(filterName)
	if !ok {
		return fmt.Errorf("fts: save filter snapshot: unknown filter codec %q", filterName)
	}

	var filterPayload bytes.Buffer
	if err := filterCodec.save(filter, &filterPayload); err != nil {
		return fmt.Errorf("fts: save filter snapshot: encode filter %q: %w", filterName, err)
	}

	envelope := filterEnvelope{
		Version:       snapshotVersion,
		FilterName:    filterName,
		FilterPayload: filterPayload.Bytes(),
	}

	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("fts: save filter snapshot: encode envelope: %w", err)
	}

	return nil
}

func LoadFilterSnapshot(r io.Reader) (*LoadedFilterSnapshot, error) {
	if r == nil {
		return nil, fmt.Errorf("fts: load filter snapshot: nil reader")
	}

	var envelope filterEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("fts: load filter snapshot: decode envelope: %w", err)
	}

	if envelope.Version != snapshotVersion {
		return nil, fmt.Errorf("fts: load filter snapshot: unsupported version %d", envelope.Version)
	}
	if envelope.FilterName == "" {
		return nil, fmt.Errorf("fts: load filter snapshot: empty filter name")
	}

	filterCodec, ok := filterCodecByName(envelope.FilterName)
	if !ok {
		return nil, fmt.Errorf("fts: load filter snapshot: unknown filter codec %q", envelope.FilterName)
	}

	filter, err := filterCodec.load(bytes.NewReader(envelope.FilterPayload))
	if err != nil {
		return nil, fmt.Errorf("fts: load filter snapshot: decode filter %q: %w", envelope.FilterName, err)
	}

	return &LoadedFilterSnapshot{FilterName: envelope.FilterName, Filter: filter}, nil
}

func indexCodecByName(name string) (indexSnapshotCodec, bool) {
	snapshotRegistryMu.RLock()
	codec, ok := indexSnapshotCodecs[name]
	snapshotRegistryMu.RUnlock()
	return codec, ok
}

func filterCodecByName(name string) (filterSnapshotCodec, bool) {
	snapshotRegistryMu.RLock()
	codec, ok := filterSnapshotCodecs[name]
	snapshotRegistryMu.RUnlock()
	return codec, ok
}
