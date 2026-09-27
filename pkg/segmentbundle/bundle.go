// Package segmentbundle provides the stream/blob persistence API for sealed
// FTS segments.
//
// Use this package when a complete sealed index should be encoded as one
// io.Writer/io.Reader-backed object. For filesystem persistence with manifests,
// separate field files, filters, and mmap support, use pkg/ftspersist.
package segmentbundle

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sort"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

const bundleVersion uint16 = 3

// BundleSaveOptions controls metadata written into a new bundle.
type BundleSaveOptions struct {
	Analyzers    map[string]fts.AnalyzerDescriptor
	KeyGenerator *fts.KeyGeneratorDescriptor
}

// LoadedBundle is a decoded sealed segment bundle.
type LoadedBundle struct {
	Segment         *segment.Reader
	Fields          map[string]*segment.Reader
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Analyzers       map[string]fts.AnalyzerDescriptor
	KeyGenerator    *fts.KeyGeneratorDescriptor
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
	Analyzers       map[string]fts.AnalyzerDescriptor
	KeyGenerator    *fts.KeyGeneratorDescriptor
}

// SaveBundle writes a single-field sealed segment bundle to w.
func SaveBundle(w io.Writer, source segment.Source, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64, opts BundleSaveOptions) error {
	return SaveMultiFieldBundle(w, map[string]segment.Source{fts.DefaultField: source}, stats, registry, tombstones, opts)
}

// SaveMultiFieldBundle writes a multi-field sealed segment bundle to w.
func SaveMultiFieldBundle(w io.Writer, fields map[string]segment.Source, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64, opts BundleSaveOptions) error {
	if w == nil {
		return fmt.Errorf("segmentbundle: save bundle: nil writer")
	}
	if len(fields) == 0 {
		return fmt.Errorf("segmentbundle: save bundle: no fields")
	}
	if len(opts.Analyzers) != len(fields) {
		return fmt.Errorf("segmentbundle: save bundle: analyzer descriptor is required for every field")
	}
	if opts.KeyGenerator == nil || opts.KeyGenerator.Fingerprint == "" {
		return fmt.Errorf("segmentbundle: save bundle: key generator descriptor is required")
	}
	for fieldName := range fields {
		descriptor, ok := opts.Analyzers[fieldName]
		if !ok || descriptor.Fingerprint == "" {
			return fmt.Errorf("segmentbundle: save bundle: analyzer descriptor is required for field %q", fieldName)
		}
	}

	fieldNames := make([]string, 0, len(fields))
	for fieldName := range fields {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)

	bundleFields := make([]bundleField, 0, len(fields))
	for _, fieldName := range fieldNames {
		if fieldName == "" {
			return fmt.Errorf("segmentbundle: save bundle: empty field name")
		}
		source := fields[fieldName]
		if source == nil {
			return fmt.Errorf("segmentbundle: save bundle: nil source for field %q", fieldName)
		}
		segmentBytes, err := segment.BuildFromSourceWithTombstones(source, tombstones)
		if err != nil {
			return fmt.Errorf("segmentbundle: save bundle: build segment for field %q: %w", fieldName, err)
		}
		bundleFields = append(bundleFields, bundleField{FieldName: fieldName, Payload: segmentBytes})
	}

	envelope := bundleEnvelope{
		Version:         bundleVersion,
		Fields:          bundleFields,
		CollectionStats: stats,
		Registry:        append([]fts.DocID(nil), registry...),
		Tombstones:      append([]uint64(nil), tombstones...),
		Analyzers:       cloneAnalyzerDescriptors(opts.Analyzers),
		KeyGenerator:    cloneKeyGeneratorDescriptor(opts.KeyGenerator),
	}
	if err := gob.NewEncoder(w).Encode(envelope); err != nil {
		return fmt.Errorf("segmentbundle: save bundle: encode envelope: %w", err)
	}
	return nil
}

// LoadBundle decodes a sealed segment bundle from r.
func LoadBundle(r io.Reader) (*LoadedBundle, error) {
	if r == nil {
		return nil, fmt.Errorf("segmentbundle: load bundle: nil reader")
	}

	var envelope bundleEnvelope
	if err := gob.NewDecoder(r).Decode(&envelope); err != nil {
		return nil, fmt.Errorf("segmentbundle: load bundle: decode envelope: %w", err)
	}
	if envelope.Version != bundleVersion {
		return nil, fmt.Errorf("segmentbundle: load bundle: unsupported version %d", envelope.Version)
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
		Analyzers:       cloneAnalyzerDescriptors(envelope.Analyzers),
		KeyGenerator:    cloneKeyGeneratorDescriptor(envelope.KeyGenerator),
	}, nil
}

// SaveService writes a complete sealed bundle from an fts.Service.
// The service must use a described pipeline so the bundle can validate its
// analyzer identity when restored.
func SaveService(w io.Writer, service *fts.Service) error {
	if service == nil {
		return fmt.Errorf("segmentbundle: save service: nil service")
	}
	descriptors, ok := service.AnalyzerDescriptors()
	if !ok {
		return fmt.Errorf("segmentbundle: save service: analyzer descriptor is unavailable for every field")
	}
	keyGenerator, ok := service.KeyGeneratorDescriptor()
	if !ok {
		return fmt.Errorf("segmentbundle: save service: key generator descriptor is unavailable")
	}

	fields, _ := service.SnapshotFields()
	if len(fields) == 0 {
		return fmt.Errorf("segmentbundle: save service: no fields")
	}
	sources := make(map[string]segment.Source, len(fields))
	for fieldName, index := range fields {
		source, ok := index.(segment.Source)
		if !ok {
			return fmt.Errorf("segmentbundle: save service: field %q does not support segment export", fieldName)
		}
		sources[fieldName] = source
	}

	return SaveMultiFieldBundle(
		w,
		sources,
		service.SnapshotCollectionStats(),
		service.SnapshotRegistry(),
		service.SnapshotTombstones(),
		BundleSaveOptions{Analyzers: descriptors, KeyGenerator: &keyGenerator},
	)
}

// RestoreService restores an fts.Service from a decoded sealed segment bundle.
func RestoreService(bundle *LoadedBundle, keyGen fts.KeyGenerator, opts ...fts.Option) (*fts.Service, error) {
	if bundle == nil {
		return nil, fmt.Errorf("segmentbundle: restore service: nil bundle")
	}
	if len(bundle.Fields) == 0 {
		if bundle.Segment == nil {
			return nil, fmt.Errorf("segmentbundle: restore service: nil segment reader")
		}
		bundle.Fields = map[string]*segment.Reader{fts.DefaultField: bundle.Segment}
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
	builtOpts = append(builtOpts, fts.WithAnalyzerDescriptors(bundle.Analyzers))

	var service *fts.Service
	if len(bundle.Fields) == 1 {
		if reader := bundle.Fields[fts.DefaultField]; reader != nil {
			service = fts.New(reader, keyGen, builtOpts...)
		}
	}

	if service == nil {
		indexes := make(map[string]fts.Index, len(bundle.Fields))
		for fieldName, reader := range bundle.Fields {
			if reader == nil {
				return nil, fmt.Errorf("segmentbundle: restore service: nil reader for field %q", fieldName)
			}
			indexes[fieldName] = reader
		}
		service = fts.NewMultiFieldFromIndexes(indexes, keyGen, builtOpts...)
	}

	if len(bundle.Analyzers) != len(bundle.Fields) {
		return nil, fmt.Errorf("segmentbundle: restore service: analyzer descriptor is required for every field")
	}
	if bundle.KeyGenerator == nil {
		return nil, fmt.Errorf("segmentbundle: restore service: key generator descriptor is unavailable")
	}
	restoredKeyGenerator, ok := service.KeyGeneratorDescriptor()
	if !ok || restoredKeyGenerator.Fingerprint != bundle.KeyGenerator.Fingerprint {
		return nil, fmt.Errorf("segmentbundle: restore service: key generator fingerprint mismatch")
	}
	if err := service.ValidateAnalyzerDescriptors(bundle.Analyzers); err != nil {
		return nil, fmt.Errorf("segmentbundle: restore service: %w", err)
	}
	return service, nil
}

// SaveBundleToBytes writes a single-field sealed segment bundle to memory.
func SaveBundleToBytes(source segment.Source, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64, opts BundleSaveOptions) ([]byte, error) {
	var buf bytes.Buffer
	if err := SaveBundle(&buf, source, stats, registry, tombstones, opts); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func cloneAnalyzerDescriptors(descriptors map[string]fts.AnalyzerDescriptor) map[string]fts.AnalyzerDescriptor {
	if descriptors == nil {
		return nil
	}
	cloned := make(map[string]fts.AnalyzerDescriptor, len(descriptors))
	for fieldName, descriptor := range descriptors {
		cloned[fieldName] = descriptor
	}
	return cloned
}

func cloneKeyGeneratorDescriptor(descriptor *fts.KeyGeneratorDescriptor) *fts.KeyGeneratorDescriptor {
	if descriptor == nil {
		return nil
	}
	cloned := *descriptor
	return &cloned
}

func loadBundleReaders(envelope bundleEnvelope) (map[string]*segment.Reader, error) {
	if len(envelope.Fields) == 0 {
		if len(envelope.SegmentPayload) == 0 {
			return nil, fmt.Errorf("segmentbundle: load bundle: empty segment payload")
		}
		reader, err := segment.Open(envelope.SegmentPayload)
		if err != nil {
			return nil, fmt.Errorf("segmentbundle: load bundle: open segment payload: %w", err)
		}
		return map[string]*segment.Reader{fts.DefaultField: reader}, nil
	}

	fields := make(map[string]*segment.Reader, len(envelope.Fields))
	for _, field := range envelope.Fields {
		if field.FieldName == "" {
			return nil, fmt.Errorf("segmentbundle: load bundle: empty field name")
		}
		reader, err := segment.Open(field.Payload)
		if err != nil {
			return nil, fmt.Errorf("segmentbundle: load bundle: open segment for field %q: %w", field.FieldName, err)
		}
		fields[field.FieldName] = reader
	}
	return fields, nil
}
