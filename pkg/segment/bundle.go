package segment

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sort"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

const (
	bundleVersionV1 uint16 = 1
	bundleVersion   uint16 = 2
)

type LoadedBundle struct {
	Segment         *Reader
	Fields          map[string]*Reader
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
}

type bundleField struct {
	FieldName string
	Payload   []byte
}

type bundleEnvelope struct {
	Version         uint16
	SegmentPayload  []byte
	Fields          []bundleField
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
}

func SaveBundle(w io.Writer, source Source, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64) error {
	return SaveMultiFieldBundle(w, map[string]Source{fts.DefaultField: source}, stats, registry, tombstones)
}

func SaveMultiFieldBundle(w io.Writer, fields map[string]Source, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64) error {
	if w == nil {
		return fmt.Errorf("segment: save bundle: nil writer")
	}
	if len(fields) == 0 {
		return fmt.Errorf("segment: save bundle: no fields")
	}

	fieldNames := make([]string, 0, len(fields))
	for fieldName := range fields {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)

	bundleFields := make([]bundleField, 0, len(fields))
	for _, fieldName := range fieldNames {
		if fieldName == "" {
			return fmt.Errorf("segment: save bundle: empty field name")
		}
		source := fields[fieldName]
		if source == nil {
			return fmt.Errorf("segment: save bundle: nil source for field %q", fieldName)
		}
		segmentBytes, err := BuildFromSourceWithTombstones(source, tombstones)
		if err != nil {
			return fmt.Errorf("segment: save bundle: build segment for field %q: %w", fieldName, err)
		}
		bundleFields = append(bundleFields, bundleField{FieldName: fieldName, Payload: segmentBytes})
	}

	envelope := bundleEnvelope{
		Version:         bundleVersion,
		Fields:          bundleFields,
		CollectionStats: stats,
		Registry:        append([]fts.DocID(nil), registry...),
		Tombstones:      append([]uint64(nil), tombstones...),
	}
	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("segment: save bundle: encode envelope: %w", err)
	}
	return nil
}

func LoadBundle(r io.Reader) (*LoadedBundle, error) {
	if r == nil {
		return nil, fmt.Errorf("segment: load bundle: nil reader")
	}

	var envelope bundleEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("segment: load bundle: decode envelope: %w", err)
	}
	if envelope.Version != bundleVersionV1 && envelope.Version != bundleVersion {
		return nil, fmt.Errorf("segment: load bundle: unsupported version %d", envelope.Version)
	}

	fields, err := loadBundleReaders(envelope)
	if err != nil {
		return nil, err
	}
	single := fields[fts.DefaultField]

	return &LoadedBundle{
		Segment:         single,
		Fields:          fields,
		CollectionStats: envelope.CollectionStats,
		Registry:        append([]fts.DocID(nil), envelope.Registry...),
		Tombstones:      append([]uint64(nil), envelope.Tombstones...),
	}, nil
}

func RestoreService(bundle *LoadedBundle, keyGen fts.KeyGenerator, opts ...fts.Option) (*fts.Service, error) {
	if bundle == nil {
		return nil, fmt.Errorf("segment: restore service: nil bundle")
	}
	if len(bundle.Fields) == 0 {
		if bundle.Segment == nil {
			return nil, fmt.Errorf("segment: restore service: nil segment reader")
		}
		bundle.Fields = map[string]*Reader{fts.DefaultField: bundle.Segment}
	}

	builtOpts := append([]fts.Option(nil), opts...)
	if len(bundle.Registry) > 0 {
		builtOpts = append(builtOpts, fts.WithDocRegistrySnapshot(bundle.Registry))
	}
	if len(bundle.Tombstones) > 0 {
		builtOpts = append(builtOpts, fts.WithTombstonesSnapshot(bundle.Tombstones))
	}
	if bundle.CollectionStats != nil {
		builtOpts = append(builtOpts, fts.WithCollectionStatsSnapshot(bundle.CollectionStats))
	}

	if len(bundle.Fields) == 1 {
		if reader := bundle.Fields[fts.DefaultField]; reader != nil {
			return fts.New(reader, keyGen, builtOpts...), nil
		}
	}

	indexes := make(map[string]fts.Index, len(bundle.Fields))
	for fieldName, reader := range bundle.Fields {
		if reader == nil {
			return nil, fmt.Errorf("segment: restore service: nil reader for field %q", fieldName)
		}
		indexes[fieldName] = reader
	}
	return fts.NewMultiFieldFromIndexes(indexes, keyGen, builtOpts...), nil
}

func SaveBundleToBytes(source Source, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64) ([]byte, error) {
	var buf bytes.Buffer
	if err := SaveBundle(&buf, source, stats, registry, tombstones); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func loadBundleReaders(envelope bundleEnvelope) (map[string]*Reader, error) {
	if len(envelope.Fields) == 0 {
		if len(envelope.SegmentPayload) == 0 {
			return nil, fmt.Errorf("segment: load bundle: empty segment payload")
		}
		reader, err := Open(envelope.SegmentPayload)
		if err != nil {
			return nil, fmt.Errorf("segment: load bundle: open segment payload: %w", err)
		}
		return map[string]*Reader{fts.DefaultField: reader}, nil
	}

	fields := make(map[string]*Reader, len(envelope.Fields))
	for _, field := range envelope.Fields {
		if field.FieldName == "" {
			return nil, fmt.Errorf("segment: load bundle: empty field name")
		}
		reader, err := Open(field.Payload)
		if err != nil {
			return nil, fmt.Errorf("segment: load bundle: open segment for field %q: %w", field.FieldName, err)
		}
		fields[field.FieldName] = reader
	}
	return fields, nil
}
