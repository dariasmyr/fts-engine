package ftspersist

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

type SaveOptions struct {
	BufferSize     int
	FlushThreshold int
	SyncFile       bool
}

type SnapshotPaths struct {
	IndexPath  string
	FilterPath string
}

type LoadedSnapshot struct {
	Index           fts.Index
	IndexName       string
	Fields          map[string]fts.Index
	FieldIndexNames map[string]string
	Filter          fts.Filter
	FilterName      string
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Close           func() error
}

type LoadedService struct {
	Service         *fts.Service
	Close           func() error
	IndexName       string
	FieldIndexNames map[string]string
	FilterName      string
}

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

	fields, searchFilter := svc.SnapshotFields()
	stats := svc.SnapshotCollectionStats()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()
	if len(fields) > 1 {
		fieldCodecs := make(map[string]string, len(fields))
		for fieldName := range fields {
			fieldCodecs[fieldName] = indexName
		}
		if err := saveAtomicWithOptions(paths.IndexPath, opts, func(w io.Writer) error {
			return fts.SaveMultiIndexSnapshotWithState(w, fieldCodecs, fields, stats, registry, tombstones)
		}); err != nil {
			return fmt.Errorf("ftspersist: save snapshot: %w", err)
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
		if err := saveAtomicWithOptions(paths.IndexPath, opts, func(w io.Writer) error {
			return fts.SaveIndexSnapshotWithState(w, indexName, index, stats, registry, tombstones)
		}); err != nil {
			return fmt.Errorf("ftspersist: save snapshot: %w", err)
		}
	}

	if searchFilter != nil && filterName != "" {
		if paths.FilterPath == "" {
			return fmt.Errorf("ftspersist: save snapshot: empty filter path")
		}
		if err := saveAtomicWithOptions(paths.FilterPath, opts, func(w io.Writer) error {
			return fts.SaveFilterSnapshot(w, filterName, searchFilter)
		}); err != nil {
			return fmt.Errorf("ftspersist: save snapshot filter: %w", err)
		}
	} else if paths.FilterPath != "" {
		if err := os.Remove(paths.FilterPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("ftspersist: remove stale filter snapshot: %w", err)
		}
	}

	return nil
}

func LoadSnapshotData(paths SnapshotPaths) (*LoadedSnapshot, error) {
	if paths.IndexPath == "" {
		return nil, fmt.Errorf("ftspersist: load snapshot: empty index path")
	}

	indexPayload, err := os.ReadFile(paths.IndexPath)
	if err != nil {
		return nil, fmt.Errorf("ftspersist: load snapshot index %q: %w", paths.IndexPath, err)
	}

	loaded := &LoadedSnapshot{Close: func() error { return nil }}
	if paths.FilterPath != "" {
		filterFile, err := os.Open(paths.FilterPath)
		if err != nil {
			return nil, fmt.Errorf("ftspersist: load snapshot filter %q: %w", paths.FilterPath, err)
		}
		loadedFilter, err := fts.LoadFilterSnapshot(filterFile)
		_ = filterFile.Close()
		if err != nil {
			return nil, fmt.Errorf("ftspersist: decode snapshot filter %q: %w", paths.FilterPath, err)
		}
		loaded.FilterName = loadedFilter.FilterName
		loaded.Filter = loadedFilter.Filter
	}

	loadedMulti, multiErr := fts.LoadMultiIndexSnapshot(bytes.NewReader(indexPayload))
	if multiErr == nil && len(loadedMulti.Fields) > 0 {
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
		return loaded, nil
	}

	loadedIndex, singleErr := fts.LoadIndexSnapshot(bytes.NewReader(indexPayload))
	if singleErr != nil {
		return nil, fmt.Errorf("ftspersist: load snapshot: decode as multi-field: %v; decode as single-field: %w", multiErr, singleErr)
	}
	loaded.Index = loadedIndex.Index
	loaded.IndexName = loadedIndex.IndexName
	loaded.CollectionStats = loadedIndex.CollectionStats
	loaded.Registry = append([]fts.DocID(nil), loadedIndex.Registry...)
	loaded.Tombstones = append([]uint64(nil), loadedIndex.Tombstones...)
	return loaded, nil
}

func RestoreSnapshotService(snapshot *LoadedSnapshot, keyGen fts.KeyGenerator, opts ...fts.Option) (*fts.Service, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("ftspersist: restore snapshot service: nil snapshot")
	}

	builtOpts := append([]fts.Option(nil), opts...)
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

	if len(snapshot.Fields) > 0 {
		return fts.NewMultiFieldFromIndexes(snapshot.Fields, keyGen, builtOpts...), nil
	}
	if snapshot.Index == nil {
		return nil, fmt.Errorf("ftspersist: restore snapshot service: nil index")
	}
	return fts.New(snapshot.Index, keyGen, builtOpts...), nil
}

func LoadSnapshot(paths SnapshotPaths, keyGen fts.KeyGenerator, opts ...fts.Option) (*LoadedService, error) {
	loadedSnapshot, err := LoadSnapshotData(paths)
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
