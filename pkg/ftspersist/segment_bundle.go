package ftspersist

import (
	"errors"
	"fmt"
	"io"
	"os"

	internalpersist "github.com/dariasmyr/fts-engine/internal/services/fts/persist"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

var ErrSegmentBundleUnsupported = errors.New("segment bundle unsupported")

type SegmentBundlePaths struct {
	BundlePath string
	FilterPath string
}

func SaveSegmentBundle(paths SegmentBundlePaths, svc *fts.Service, filterName string, opts SaveOptions) error {
	if svc == nil {
		return fmt.Errorf("ftspersist: save segment bundle: nil service")
	}
	if paths.BundlePath == "" {
		return ErrSegmentBundleUnsupported
	}

	index, searchFilter := svc.SnapshotComponents()
	fields, _ := svc.SnapshotFields()
	sources := make(map[string]segment.Source, len(fields))
	for fieldName, fieldIndex := range fields {
		source, ok := fieldIndex.(segment.Source)
		if !ok {
			return ErrSegmentBundleUnsupported
		}
		sources[fieldName] = source
	}
	if len(sources) == 0 && index != nil {
		source, ok := index.(segment.Source)
		if !ok {
			return ErrSegmentBundleUnsupported
		}
		sources[fts.DefaultField] = source
	}

	persistOpts := internalpersist.SaveOptions(opts)
	stats := svc.SnapshotCollectionStats()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()

	if err := internalpersist.SaveAtomicWithOptions(paths.BundlePath, persistOpts, func(w io.Writer) error {
		return segment.SaveMultiFieldBundle(w, sources, stats, registry, tombstones)
	}); err != nil {
		return fmt.Errorf("ftspersist: save segment bundle: %w", err)
	}

	if searchFilter != nil && filterName != "" {
		if paths.FilterPath == "" {
			return fmt.Errorf("ftspersist: save segment bundle: empty filter path")
		}
		if err := internalpersist.SaveAtomicWithOptions(paths.FilterPath, persistOpts, func(w io.Writer) error {
			return fts.SaveFilterSnapshot(w, filterName, searchFilter)
		}); err != nil {
			return fmt.Errorf("ftspersist: save segment bundle filter: %w", err)
		}
	} else if paths.FilterPath != "" {
		if err := os.Remove(paths.FilterPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("ftspersist: remove stale segment bundle filter: %w", err)
		}
	}

	return nil
}
