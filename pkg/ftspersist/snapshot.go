package ftspersist

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

type SaveOptions struct {
	BufferSize     int
	FlushThreshold int
	SyncFile       bool
	Registry       *fts.SnapshotRegistry
}

type SnapshotPaths struct {
	// IndexPath stores the complete atomic snapshot bundle.
	IndexPath string
	// FilterPath is read only for compatibility with legacy split snapshots.
	// New snapshots embed the filter in IndexPath.
	FilterPath string
}

// SnapshotLoadOptions controls snapshot codec resolution during load.
type SnapshotLoadOptions struct {
	Registry *fts.SnapshotRegistry
	// MaxBytes limits the snapshot bundle read from IndexPath. Values <= 0 use
	// the package default.
	MaxBytes int64
	// IndexFactory is required to restore an empty multi-field snapshot because
	// the snapshot contains no field index from which to derive future indexes.
	IndexFactory fts.IndexFactory
}

const (
	snapshotBundleVersion   uint16 = 1
	defaultMaxSnapshotBytes        = int64(2 << 30)
)

var snapshotBundleMagic = []byte{'F', 'T', 'S', 'P', 'B', '1', 0}

type snapshotBundle struct {
	Version       uint16
	IndexPayload  []byte
	FilterPayload []byte
}

type LoadedSnapshot struct {
	Index           fts.Index
	IndexName       string
	MultiField      bool
	IndexFactory    fts.IndexFactory
	Fields          map[string]fts.Index
	FieldIndexNames map[string]string
	Filter          fts.Filter
	FilterName      string
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Pipelines       map[string]fts.PipelineDescriptor
	KeyGenerator    *fts.KeyGeneratorDescriptor
	Close           func() error
}

type LoadedService struct {
	Service         *fts.Service
	Close           func() error
	IndexName       string
	FieldIndexNames map[string]string
	FilterName      string
}

// SaveSnapshot atomically writes one self-contained snapshot bundle. The
// service must not be mutated during the call; searches may continue.
func SaveSnapshot(paths SnapshotPaths, svc *fts.Service, indexName string, filterName string, opts SaveOptions) error {
	if svc == nil {
		return fmt.Errorf("ftspersist: save snapshot: nil service")
	}
	if paths.IndexPath == "" {
		return fmt.Errorf("ftspersist: save snapshot: empty index path")
	}
	if indexName == "" {
		return fmt.Errorf("ftspersist: save snapshot: empty index name")
	}
	if opts.Registry == nil {
		return fmt.Errorf("ftspersist: save snapshot: nil snapshot registry")
	}
	if paths.FilterPath != "" && samePath(paths.IndexPath, paths.FilterPath) {
		return fmt.Errorf("ftspersist: save snapshot: index and filter paths must differ")
	}
	keyGenerator, ok := svc.KeyGeneratorDescriptor()
	if !ok {
		return fmt.Errorf("ftspersist: save snapshot: key generator descriptor is unavailable")
	}
	pipelines, err := svc.PipelineDescriptors()
	if err != nil {
		return fmt.Errorf("ftspersist: save snapshot: %w", err)
	}

	fields, searchFilter := svc.SnapshotFields()
	stats := svc.SnapshotCollectionStats()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()
	var indexPayload bytes.Buffer
	if svc.IsMultiField() {
		fieldCodecs := make(map[string]string, len(fields))
		for fieldName := range fields {
			fieldCodecs[fieldName] = indexName
		}
		if err := opts.Registry.SaveMultiIndexSnapshotWithState(&indexPayload, fieldCodecs, fields, stats, registry, tombstones, pipelines, &keyGenerator); err != nil {
			return fmt.Errorf("ftspersist: save snapshot index: %w", err)
		}
	} else {
		index, _ := svc.SnapshotComponents()
		if index == nil {
			for _, fieldIndex := range fields {
				index = fieldIndex
				break
			}
		}
		if index == nil {
			return fmt.Errorf("ftspersist: save snapshot: nil index")
		}
		if err := opts.Registry.SaveIndexSnapshotWithState(&indexPayload, indexName, index, stats, registry, tombstones, pipelines, &keyGenerator); err != nil {
			return fmt.Errorf("ftspersist: save snapshot index: %w", err)
		}
	}

	var filterPayload bytes.Buffer
	if searchFilter != nil && filterName != "" {
		if err := opts.Registry.SaveFilterSnapshot(&filterPayload, filterName, searchFilter); err != nil {
			return fmt.Errorf("ftspersist: save snapshot filter: %w", err)
		}
	}

	bundle := snapshotBundle{
		Version:       snapshotBundleVersion,
		IndexPayload:  indexPayload.Bytes(),
		FilterPayload: filterPayload.Bytes(),
	}
	if err := saveAtomicWithOptions(paths.IndexPath, opts, func(w io.Writer) error {
		if _, err := w.Write(snapshotBundleMagic); err != nil {
			return err
		}
		return gob.NewEncoder(w).Encode(bundle)
	}); err != nil {
		return fmt.Errorf("ftspersist: save snapshot bundle: %w", err)
	}
	return nil
}

func LoadSnapshotData(paths SnapshotPaths, opts SnapshotLoadOptions) (*LoadedSnapshot, error) {
	if paths.IndexPath == "" {
		return nil, fmt.Errorf("ftspersist: load snapshot: empty index path")
	}
	if opts.Registry == nil {
		return nil, fmt.Errorf("ftspersist: load snapshot: nil snapshot registry")
	}

	indexPayload, filterPayload, isBundle, err := loadSnapshotPayload(paths.IndexPath, opts.MaxBytes)
	if err != nil {
		return nil, fmt.Errorf("ftspersist: load snapshot index %q: %w", paths.IndexPath, err)
	}

	type snapshotHeader struct {
		Version   uint16
		Kind      string
		IndexName string
		Fields    []struct {
			FieldName string
		}
	}
	var header snapshotHeader
	if err := gob.NewDecoder(bytes.NewReader(indexPayload)).Decode(&header); err != nil {
		return nil, fmt.Errorf("ftspersist: decode snapshot header: %w", err)
	}
	kind := header.Kind
	if kind == "" {
		switch {
		case len(header.Fields) > 0:
			kind = "multi"
		case header.IndexName != "":
			kind = "single"
		default:
			return nil, fmt.Errorf("ftspersist: load snapshot: cannot determine snapshot kind")
		}
	}

	loaded := &LoadedSnapshot{IndexFactory: opts.IndexFactory, Close: onceClose(nil)}
	switch kind {
	case "multi":
		if header.IndexName != "" {
			return nil, fmt.Errorf("ftspersist: load snapshot: multi-field header contains single-field index name %q", header.IndexName)
		}
		loadedMulti, err := opts.Registry.LoadMultiIndexSnapshot(bytes.NewReader(indexPayload))
		if err != nil {
			return nil, err
		}
		indexes := make(map[string]fts.Index, len(loadedMulti.Fields))
		fieldIndexNames := make(map[string]string, len(loadedMulti.Fields))
		for fieldName, field := range loadedMulti.Fields {
			indexes[fieldName] = field.Index
			fieldIndexNames[fieldName] = field.IndexName
		}
		loaded.Fields = indexes
		loaded.FieldIndexNames = fieldIndexNames
		loaded.CollectionStats = loadedMulti.CollectionStats
		loaded.Registry = append([]fts.DocID(nil), loadedMulti.Registry...)
		loaded.Tombstones = append([]uint64(nil), loadedMulti.Tombstones...)
		loaded.Pipelines = loadedMulti.Pipelines
		loaded.KeyGenerator = loadedMulti.KeyGenerator
		loaded.MultiField = true
	case "single":
		if len(header.Fields) > 0 {
			return nil, fmt.Errorf("ftspersist: load snapshot: single-field header contains %d multi-field entries", len(header.Fields))
		}
		loadedIndex, err := opts.Registry.LoadIndexSnapshot(bytes.NewReader(indexPayload))
		if err != nil {
			return nil, err
		}
		loaded.Index = loadedIndex.Index
		loaded.IndexName = loadedIndex.IndexName
		loaded.CollectionStats = loadedIndex.CollectionStats
		loaded.Registry = append([]fts.DocID(nil), loadedIndex.Registry...)
		loaded.Tombstones = append([]uint64(nil), loadedIndex.Tombstones...)
		loaded.Pipelines = loadedIndex.Pipelines
		loaded.KeyGenerator = loadedIndex.KeyGenerator
	default:
		return nil, fmt.Errorf("ftspersist: load snapshot: unsupported snapshot kind %q", kind)
	}

	if len(filterPayload) > 0 {
		loadedFilter, err := opts.Registry.LoadFilterSnapshot(bytes.NewReader(filterPayload))
		if err != nil {
			return nil, fmt.Errorf("ftspersist: decode bundled snapshot filter: %w", err)
		}
		loaded.FilterName = loadedFilter.FilterName
		loaded.Filter = loadedFilter.Filter
	} else if !isBundle && paths.FilterPath != "" {
		filterFile, err := os.Open(paths.FilterPath)
		if err != nil {
			return nil, fmt.Errorf("ftspersist: load snapshot filter %q: %w", paths.FilterPath, err)
		}
		loadedFilter, loadErr := opts.Registry.LoadFilterSnapshot(filterFile)
		closeErr := filterFile.Close()
		if loadErr != nil || closeErr != nil {
			return nil, errors.Join(
				wrapError(loadErr, "ftspersist: decode snapshot filter %q", paths.FilterPath),
				wrapError(closeErr, "ftspersist: close snapshot filter %q", paths.FilterPath),
			)
		}
		loaded.FilterName = loadedFilter.FilterName
		loaded.Filter = loadedFilter.Filter
	}
	return loaded, nil
}

func samePath(left, right string) bool {
	leftInfo, leftStatErr := os.Stat(left)
	rightInfo, rightStatErr := os.Stat(right)
	if leftStatErr == nil && rightStatErr == nil && os.SameFile(leftInfo, rightInfo) {
		return true
	}
	leftPath, leftErr := filepath.Abs(left)
	rightPath, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return filepath.Clean(left) == filepath.Clean(right)
	}
	return filepath.Clean(leftPath) == filepath.Clean(rightPath)
}

func loadSnapshotPayload(path string, configuredMax int64) ([]byte, []byte, bool, error) {
	maxBytes := configuredMax
	if maxBytes <= 0 {
		maxBytes = defaultMaxSnapshotBytes
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, false, err
	}
	if info.Size() > maxBytes {
		return nil, nil, false, fmt.Errorf("file size %d exceeds limit %d", info.Size(), maxBytes)
	}

	magic := make([]byte, len(snapshotBundleMagic))
	if _, err := io.ReadFull(file, magic); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, nil, false, err
		}
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
			return nil, nil, false, seekErr
		}
		payload, readErr := io.ReadAll(io.LimitReader(file, maxBytes+1))
		return payload, nil, false, readErr
	}
	if bytes.Equal(magic, snapshotBundleMagic) {
		var bundle snapshotBundle
		if err := gob.NewDecoder(io.LimitReader(file, maxBytes-int64(len(magic)))).Decode(&bundle); err != nil {
			return nil, nil, false, fmt.Errorf("decode snapshot bundle: %w", err)
		}
		if bundle.Version != snapshotBundleVersion {
			return nil, nil, false, fmt.Errorf("unsupported snapshot bundle version %d", bundle.Version)
		}
		return bundle.IndexPayload, bundle.FilterPayload, true, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, false, err
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, nil, false, err
	}
	if int64(len(payload)) > maxBytes {
		return nil, nil, false, fmt.Errorf("file exceeds limit %d", maxBytes)
	}
	return payload, nil, false, nil
}

func RestoreSnapshotService(snapshot *LoadedSnapshot, keyGen fts.KeyGenerator, opts ...*fts.Option) (*fts.Service, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("ftspersist: restore snapshot service: nil snapshot")
	}
	if snapshot.KeyGenerator == nil {
		return nil, fmt.Errorf("ftspersist: restore snapshot service: key generator descriptor is unavailable")
	}
	fieldNames := []string{fts.DefaultField}
	if snapshot.MultiField {
		fieldNames = make([]string, 0, len(snapshot.Fields))
		for fieldName := range snapshot.Fields {
			fieldNames = append(fieldNames, fieldName)
		}
	}
	if err := fts.ValidatePipelineFields(fieldNames, snapshot.Pipelines); err != nil {
		return nil, fmt.Errorf("ftspersist: restore snapshot service: %w", err)
	}

	builtOpts := append([]*fts.Option(nil), opts...)
	builtOpts = append(builtOpts, fts.WithPipelineDescriptors(snapshot.Pipelines))
	if snapshot.Filter != nil {
		builtOpts = append(builtOpts, fts.WithFilter(snapshot.Filter))
	}
	if len(snapshot.Registry) > 0 {
		builtOpts = append(builtOpts, fts.WithDocRegistrySnapshot(snapshot.Registry))
	}
	if len(snapshot.Tombstones) > 0 {
		builtOpts = append(builtOpts, fts.WithTombstonesSnapshot(snapshot.Tombstones))
	}
	if snapshot.CollectionStats != nil {
		builtOpts = append(builtOpts, fts.WithCollectionStatsSnapshot(snapshot.CollectionStats))
	}

	if snapshot.MultiField {
		var service *fts.Service
		if len(snapshot.Fields) == 0 {
			if snapshot.IndexFactory == nil {
				return nil, fmt.Errorf("ftspersist: restore snapshot service: empty multi-field snapshot requires an index factory")
			}
			service = fts.NewMultiField(snapshot.IndexFactory, keyGen, builtOpts...)
		} else {
			service = fts.NewMultiFieldFromIndexes(snapshot.Fields, keyGen, builtOpts...)
		}
		if err := validateSnapshotKeyGenerator(service, snapshot.KeyGenerator); err != nil {
			return nil, err
		}
		if err := service.ValidatePipelineDescriptors(snapshot.Pipelines); err != nil {
			return nil, fmt.Errorf("ftspersist: restore snapshot service: %w", err)
		}
		return service, nil
	}
	if snapshot.Index == nil {
		return nil, fmt.Errorf("ftspersist: restore snapshot service: nil index")
	}
	service := fts.New(snapshot.Index, keyGen, builtOpts...)
	if err := validateSnapshotKeyGenerator(service, snapshot.KeyGenerator); err != nil {
		return nil, err
	}
	if err := service.ValidatePipelineDescriptors(snapshot.Pipelines); err != nil {
		return nil, fmt.Errorf("ftspersist: restore snapshot service: %w", err)
	}
	return service, nil
}

func validateSnapshotKeyGenerator(service *fts.Service, expected *fts.KeyGeneratorDescriptor) error {
	got, ok := service.KeyGeneratorDescriptor()
	if !ok || got.Fingerprint != expected.Fingerprint {
		return fmt.Errorf("ftspersist: restore snapshot service: key generator fingerprint mismatch")
	}
	return nil
}

func LoadSnapshot(paths SnapshotPaths, keyGen fts.KeyGenerator, loadOpts SnapshotLoadOptions, opts ...*fts.Option) (*LoadedService, error) {
	loadedSnapshot, err := LoadSnapshotData(paths, loadOpts)
	if err != nil {
		return nil, err
	}

	service, err := RestoreSnapshotService(loadedSnapshot, keyGen, opts...)
	if err != nil {
		if loadedSnapshot.Close != nil {
			_ = loadedSnapshot.Close()
		}
		return nil, err
	}

	return &LoadedService{
		Service:         service,
		Close:           loadedSnapshot.Close,
		IndexName:       loadedSnapshot.IndexName,
		FieldIndexNames: loadedSnapshot.FieldIndexNames,
		FilterName:      loadedSnapshot.FilterName,
	}, nil
}
