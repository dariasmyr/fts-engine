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
	"slices"

	"github.com/dariasmyr/fts-engine/v2/internal/sealedstate"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

const bundleVersion uint16 = 6

// BundleSaveOptions controls metadata written into a new bundle.
type BundleSaveOptions struct {
	Pipelines    map[string]fts.PipelineDescriptor
	KeyGenerator *fts.KeyGeneratorDescriptor
}

// LoadedBundle is a decoded sealed segment bundle.
type LoadedBundle struct {
	Fields          map[string]*ftse.Reader
	MultiField      bool
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Pipelines       map[string]fts.PipelineDescriptor
	KeyGenerator    *fts.KeyGeneratorDescriptor
}

type bundleField struct {
	FieldName string
	Payload   []byte
}

type bundleEnvelope struct {
	Version         uint16
	MultiField      bool
	SegmentPayload  []byte
	Fields          []bundleField
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Pipelines       map[string]fts.PipelineDescriptor
	KeyGenerator    *fts.KeyGeneratorDescriptor
}

// SaveBundle writes a single-field sealed segment bundle to w.
func SaveBundle(w io.Writer, source posting.Exporter, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64, opts BundleSaveOptions) error {
	return saveBundle(w, map[string]posting.Exporter{fts.DefaultField: source}, false, stats, registry, tombstones, opts)
}

// SaveMultiFieldBundle writes a multi-field sealed segment bundle to w.
func SaveMultiFieldBundle(w io.Writer, fields map[string]posting.Exporter, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64, opts BundleSaveOptions) error {
	return saveBundle(w, fields, true, stats, registry, tombstones, opts)
}

func saveBundle(w io.Writer, fields map[string]posting.Exporter, multiField bool, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64, opts BundleSaveOptions) error {
	if w == nil {
		return fmt.Errorf("segmentbundle: save bundle: nil writer")
	}
	if len(fields) == 0 && !multiField {
		return fmt.Errorf("segmentbundle: save bundle: no fields")
	}
	fieldNames := make([]string, 0, len(fields))
	for fieldName := range fields {
		fieldNames = append(fieldNames, fieldName)
	}
	if err := fts.ValidatePipelineFields(fieldNames, opts.Pipelines); err != nil {
		return fmt.Errorf("segmentbundle: save bundle: %w", err)
	}
	if opts.KeyGenerator == nil || opts.KeyGenerator.Fingerprint == "" {
		return fmt.Errorf("segmentbundle: save bundle: key generator descriptor is required")
	}
	slices.Sort(fieldNames)

	bundleFields := make([]bundleField, 0, len(fields))
	for _, fieldName := range fieldNames {
		if fieldName == "" {
			return fmt.Errorf("segmentbundle: save bundle: empty field name")
		}
		source := fields[fieldName]
		if source == nil {
			return fmt.Errorf("segmentbundle: save bundle: nil source for field %q", fieldName)
		}
		segmentBytes, err := ftse.BuildFromSourceWithTombstones(source, tombstones)
		if err != nil {
			return fmt.Errorf("segmentbundle: save bundle: build segment for field %q: %w", fieldName, err)
		}
		bundleFields = append(bundleFields, bundleField{FieldName: fieldName, Payload: segmentBytes})
	}

	envelope := bundleEnvelope{
		Version:         bundleVersion,
		MultiField:      multiField,
		Fields:          bundleFields,
		CollectionStats: stats,
		Registry:        append([]fts.DocID(nil), registry...),
		Tombstones:      append([]uint64(nil), tombstones...),
		Pipelines:       clonePipelineDescriptors(opts.Pipelines),
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
		if envelope.Version > 0 && envelope.Version < bundleVersion {
			if envelope.Version == bundleVersion-1 {
				return nil, fmt.Errorf("segmentbundle: load bundle: legacy version %d uses analyzer metadata: %w", envelope.Version, fts.ErrPersistenceRebuildRequired)
			}
			return nil, fmt.Errorf("segmentbundle: load bundle: legacy version %d: %w", envelope.Version, fts.ErrPersistenceRebuildRequired)
		}
		return nil, fmt.Errorf("segmentbundle: load bundle: unsupported version %d", envelope.Version)
	}
	fieldNames := make([]string, 0, len(envelope.Fields))
	if len(envelope.Fields) == 0 && len(envelope.SegmentPayload) > 0 {
		fieldNames = append(fieldNames, fts.DefaultField)
	} else {
		for _, field := range envelope.Fields {
			fieldNames = append(fieldNames, field.FieldName)
		}
	}
	if err := fts.ValidatePipelineFields(fieldNames, envelope.Pipelines); err != nil {
		return nil, fmt.Errorf("segmentbundle: load bundle: %w", err)
	}

	fields, err := loadBundleReaders(envelope)
	if err != nil {
		return nil, err
	}
	return &LoadedBundle{
		Fields:          fields,
		MultiField:      envelope.MultiField,
		CollectionStats: envelope.CollectionStats,
		Registry:        append([]fts.DocID(nil), envelope.Registry...),
		Tombstones:      append([]uint64(nil), envelope.Tombstones...),
		Pipelines:       clonePipelineDescriptors(envelope.Pipelines),
		KeyGenerator:    cloneKeyGeneratorDescriptor(envelope.KeyGenerator),
	}, nil
}

// SaveService writes a complete sealed bundle from an fts.Service.
// The service must use described pipelines so the bundle can validate their
// identities when restored.
func SaveService(w io.Writer, service *fts.Service) error {
	if service == nil {
		return fmt.Errorf("segmentbundle: save service: nil service")
	}
	descriptors, err := service.PipelineDescriptors()
	if err != nil {
		return fmt.Errorf("segmentbundle: save service: %w", err)
	}
	keyGenerator, ok := service.KeyGeneratorDescriptor()
	if !ok {
		return fmt.Errorf("segmentbundle: save service: key generator descriptor is unavailable")
	}

	fields, _ := service.SnapshotFields()
	if len(fields) == 0 && !service.IsMultiField() {
		return fmt.Errorf("segmentbundle: save service: no fields")
	}
	sources := make(map[string]posting.Exporter, len(fields))
	for fieldName, index := range fields {
		source, ok := index.(posting.Exporter)
		if !ok {
			return fmt.Errorf("segmentbundle: save service: field %q does not support segment export", fieldName)
		}
		sources[fieldName] = source
	}

	return saveBundle(
		w,
		sources,
		service.IsMultiField(),
		service.SnapshotCollectionStats(),
		service.SnapshotRegistry(),
		service.SnapshotTombstones(),
		BundleSaveOptions{Pipelines: descriptors, KeyGenerator: &keyGenerator},
	)
}

// RestoreService restores an fts.Service from a decoded sealed segment bundle.
func RestoreService(bundle *LoadedBundle, keyGen fts.KeyGenerator, opts ...*fts.Option) (*fts.Service, error) {
	if bundle == nil {
		return nil, fmt.Errorf("segmentbundle: restore service: nil bundle")
	}
	indexes := make(map[string]fts.Index, len(bundle.Fields))
	for fieldName, reader := range bundle.Fields {
		indexes[fieldName] = reader
	}
	service, err := sealedstate.Restore(sealedstate.State{
		Fields:          indexes,
		MultiField:      bundle.MultiField,
		CollectionStats: bundle.CollectionStats,
		Registry:        bundle.Registry,
		Tombstones:      bundle.Tombstones,
		Pipelines:       bundle.Pipelines,
		KeyGenerator:    bundle.KeyGenerator,
	}, keyGen, opts...)
	if err != nil {
		return nil, fmt.Errorf("segmentbundle: restore service: %w", err)
	}
	return service, nil
}

// SaveBundleToBytes writes a single-field sealed segment bundle to memory.
func SaveBundleToBytes(source posting.Exporter, stats *fts.CollectionStatsSnapshot, registry []fts.DocID, tombstones []uint64, opts BundleSaveOptions) ([]byte, error) {
	var buf bytes.Buffer
	if err := SaveBundle(&buf, source, stats, registry, tombstones, opts); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func clonePipelineDescriptors(descriptors map[string]fts.PipelineDescriptor) map[string]fts.PipelineDescriptor {
	if descriptors == nil {
		return nil
	}
	cloned := make(map[string]fts.PipelineDescriptor, len(descriptors))
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

func loadBundleReaders(envelope bundleEnvelope) (map[string]*ftse.Reader, error) {
	if len(envelope.Fields) == 0 {
		if envelope.MultiField {
			return map[string]*ftse.Reader{}, nil
		}
		if len(envelope.SegmentPayload) == 0 {
			return nil, fmt.Errorf("segmentbundle: load bundle: empty segment payload")
		}
		reader, err := ftse.Open(envelope.SegmentPayload)
		if err != nil {
			return nil, fmt.Errorf("segmentbundle: load bundle: open segment payload: %w", err)
		}
		return map[string]*ftse.Reader{fts.DefaultField: reader}, nil
	}

	fields := make(map[string]*ftse.Reader, len(envelope.Fields))
	for _, field := range envelope.Fields {
		if field.FieldName == "" {
			return nil, fmt.Errorf("segmentbundle: load bundle: empty field name")
		}
		if _, exists := fields[field.FieldName]; exists {
			return nil, fmt.Errorf("segmentbundle: load bundle: duplicate field %q", field.FieldName)
		}
		reader, err := ftse.Open(field.Payload)
		if err != nil {
			return nil, fmt.Errorf("segmentbundle: load bundle: open segment for field %q: %w", field.FieldName, err)
		}
		fields[field.FieldName] = reader
	}
	return fields, nil
}
