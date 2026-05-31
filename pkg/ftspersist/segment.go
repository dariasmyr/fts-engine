package ftspersist

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"

	internalpersist "github.com/dariasmyr/fts-engine/internal/services/fts/persist"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

type AccessMode string

const (
	AccessFile AccessMode = "file"
	AccessMmap AccessMode = "mmap"
)

type SegmentPaths struct {
	Dir string
}

type SegmentLoadOptions struct {
	Access AccessMode
}

type LoadedSegment struct {
	Segment         *segment.Reader
	Fields          map[string]*segment.Reader
	Filter          fts.Filter
	FilterName      string
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Close           func() error
}

type segmentManifest struct {
	Version         uint16
	Fields          []segmentFieldMeta
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Filter          *filterMeta
}

type segmentFieldMeta struct {
	FieldName string
	FileName  string
}

type filterMeta struct {
	FilterName string
	FileName   string
}

const (
	segmentManifestVersion uint16 = 1
	segmentManifestFile           = "manifest.gob"
	segmentFilterFile             = "filter.fidx"
)

func SaveSegment(paths SegmentPaths, svc *fts.Service, filterName string, opts SaveOptions) error {
	if svc == nil {
		return fmt.Errorf("ftspersist: save segment: nil service")
	}
	if paths.Dir == "" {
		return fmt.Errorf("ftspersist: save segment: empty dir")
	}

	fields, searchFilter := svc.SnapshotFields()
	if len(fields) == 0 {
		return fmt.Errorf("ftspersist: save segment: no fields")
	}

	fieldNames := make([]string, 0, len(fields))
	for fieldName := range fields {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)

	persistOpts := internalpersist.SaveOptions(opts)
	manifest := segmentManifest{
		Version:         segmentManifestVersion,
		Fields:          make([]segmentFieldMeta, 0, len(fieldNames)),
		CollectionStats: svc.SnapshotCollectionStats(),
		Registry:        append([]fts.DocID(nil), svc.SnapshotRegistry()...),
		Tombstones:      append([]uint64(nil), svc.SnapshotTombstones()...),
	}

	for _, fieldName := range fieldNames {
		fieldIndex := fields[fieldName]
		source, ok := fieldIndex.(segment.Source)
		if !ok {
			return fmt.Errorf("ftspersist: save segment: field %q does not support segment export", fieldName)
		}
		fileName := segmentFieldFileName(fieldName)
		path := filepath.Join(paths.Dir, fileName)
		if err := internalpersist.SaveAtomicWithOptions(path, persistOpts, func(w io.Writer) error {
			data, err := segment.BuildFromSourceWithTombstones(source, manifest.Tombstones)
			if err != nil {
				return err
			}
			_, err = w.Write(data)
			return err
		}); err != nil {
			return fmt.Errorf("ftspersist: save segment field %q: %w", fieldName, err)
		}
		manifest.Fields = append(manifest.Fields, segmentFieldMeta{FieldName: fieldName, FileName: fileName})
	}

	if searchFilter != nil && filterName != "" {
		manifest.Filter = &filterMeta{FilterName: filterName, FileName: segmentFilterFile}
		filterPath := filepath.Join(paths.Dir, segmentFilterFile)
		if err := internalpersist.SaveAtomicWithOptions(filterPath, persistOpts, func(w io.Writer) error {
			return fts.SaveFilterSnapshot(w, filterName, searchFilter)
		}); err != nil {
			return fmt.Errorf("ftspersist: save segment filter: %w", err)
		}
	}

	manifestPath := filepath.Join(paths.Dir, segmentManifestFile)
	if err := internalpersist.SaveAtomicWithOptions(manifestPath, persistOpts, func(w io.Writer) error {
		return gob.NewEncoder(w).Encode(manifest)
	}); err != nil {
		return fmt.Errorf("ftspersist: save segment manifest: %w", err)
	}

	return nil
}

func LoadSegmentData(paths SegmentPaths, load SegmentLoadOptions) (*LoadedSegment, error) {
	if paths.Dir == "" {
		return nil, fmt.Errorf("ftspersist: load segment: empty dir")
	}
	if load.Access == "" {
		load.Access = AccessFile
	}
	if load.Access != AccessFile {
		return nil, fmt.Errorf("ftspersist: load segment: unsupported access mode %q", load.Access)
	}

	manifestPath := filepath.Join(paths.Dir, segmentManifestFile)
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("ftspersist: load segment manifest %q: %w", manifestPath, err)
	}
	var manifest segmentManifest
	if err := gob.NewDecoder(bytes.NewReader(manifestBytes)).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("ftspersist: decode segment manifest: %w", err)
	}
	if manifest.Version != segmentManifestVersion {
		return nil, fmt.Errorf("ftspersist: load segment: unsupported manifest version %d", manifest.Version)
	}
	if len(manifest.Fields) == 0 {
		return nil, fmt.Errorf("ftspersist: load segment: manifest has no fields")
	}

	loaded := &LoadedSegment{
		Fields:          make(map[string]*segment.Reader, len(manifest.Fields)),
		CollectionStats: manifest.CollectionStats,
		Registry:        append([]fts.DocID(nil), manifest.Registry...),
		Tombstones:      append([]uint64(nil), manifest.Tombstones...),
		Close:           func() error { return nil },
	}

	for _, field := range manifest.Fields {
		if field.FieldName == "" {
			return nil, fmt.Errorf("ftspersist: load segment: empty field name in manifest")
		}
		if field.FileName == "" {
			return nil, fmt.Errorf("ftspersist: load segment: empty file name for field %q", field.FieldName)
		}
		segmentPath := filepath.Join(paths.Dir, field.FileName)
		data, err := os.ReadFile(segmentPath)
		if err != nil {
			return nil, fmt.Errorf("ftspersist: load segment field %q from %q: %w", field.FieldName, segmentPath, err)
		}
		reader, err := segment.Open(data)
		if err != nil {
			return nil, fmt.Errorf("ftspersist: open segment field %q: %w", field.FieldName, err)
		}
		loaded.Fields[field.FieldName] = reader
	}
	loaded.Segment = loaded.Fields[fts.DefaultField]

	if manifest.Filter != nil {
		filterPath := filepath.Join(paths.Dir, manifest.Filter.FileName)
		filterFile, err := os.Open(filterPath)
		if err != nil {
			return nil, fmt.Errorf("ftspersist: load segment filter %q: %w", filterPath, err)
		}
		loadedFilter, err := fts.LoadFilterSnapshot(filterFile)
		_ = filterFile.Close()
		if err != nil {
			return nil, fmt.Errorf("ftspersist: decode segment filter %q: %w", filterPath, err)
		}
		loaded.FilterName = loadedFilter.FilterName
		loaded.Filter = loadedFilter.Filter
	}

	return loaded, nil
}

func RestoreSegmentService(loaded *LoadedSegment, keyGen fts.KeyGenerator, opts ...fts.Option) (*fts.Service, error) {
	if loaded == nil {
		return nil, fmt.Errorf("ftspersist: restore segment service: nil segment")
	}
	if len(loaded.Fields) == 0 && loaded.Segment == nil {
		return nil, fmt.Errorf("ftspersist: restore segment service: no segment readers")
	}

	builtOpts := append([]fts.Option(nil), opts...)
	if loaded.Filter != nil {
		builtOpts = append(builtOpts, fts.WithFilter(loaded.Filter))
	}
	if len(loaded.Registry) > 0 {
		builtOpts = append(builtOpts, fts.WithDocRegistrySnapshot(loaded.Registry))
	}
	if len(loaded.Tombstones) > 0 {
		builtOpts = append(builtOpts, fts.WithTombstonesSnapshot(loaded.Tombstones))
	}
	if loaded.CollectionStats != nil {
		builtOpts = append(builtOpts, fts.WithCollectionStatsSnapshot(loaded.CollectionStats))
	}

	if len(loaded.Fields) == 1 {
		if reader := loaded.Fields[fts.DefaultField]; reader != nil {
			return fts.New(reader, keyGen, builtOpts...), nil
		}
	}
	if loaded.Segment != nil && len(loaded.Fields) == 0 {
		return fts.New(loaded.Segment, keyGen, builtOpts...), nil
	}

	indexes := make(map[string]fts.Index, len(loaded.Fields))
	for fieldName, reader := range loaded.Fields {
		if reader == nil {
			return nil, fmt.Errorf("ftspersist: restore segment service: nil reader for field %q", fieldName)
		}
		indexes[fieldName] = reader
	}
	return fts.NewMultiFieldFromIndexes(indexes, keyGen, builtOpts...), nil
}

func LoadSegment(paths SegmentPaths, keyGen fts.KeyGenerator, load SegmentLoadOptions, opts ...fts.Option) (*LoadedService, error) {
	loadedSegment, err := LoadSegmentData(paths, load)
	if err != nil {
		return nil, err
	}
	service, err := RestoreSegmentService(loadedSegment, keyGen, opts...)
	if err != nil {
		if loadedSegment.Close != nil {
			_ = loadedSegment.Close()
		}
		return nil, err
	}
	return &LoadedService{Service: service, Close: loadedSegment.Close, FilterName: loadedSegment.FilterName}, nil
}

func segmentFieldFileName(fieldName string) string {
	if fieldName == "" || fieldName == fts.DefaultField {
		return "_default.segment.fidx"
	}
	return url.PathEscape(fieldName) + ".segment.fidx"
}
