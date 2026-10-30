// Package ftspersist provides filesystem persistence for FTS snapshots and segments.
package ftspersist

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/dariasmyr/fts-engine/v2/internal/sealedstate"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/ftse"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
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
	Access   AccessMode
	Registry *fts.SnapshotRegistry
	// MaxManifestBytes and MaxFilterBytes limit metadata decoded during load.
	// Values <= 0 use package defaults.
	MaxManifestBytes int64
	MaxFilterBytes   int64
}

type loadedSegment struct {
	Fields          map[string]*ftse.Reader
	MultiField      bool
	Filter          fts.Filter
	FilterName      string
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Pipelines       map[string]fts.PipelineDescriptor
	KeyGenerator    *fts.KeyGeneratorDescriptor
	Generation      string
	Close           func() error
}

type segmentManifest struct {
	Version         uint16
	MultiField      bool
	Fields          []segmentFieldMeta
	CollectionStats *fts.CollectionStatsSnapshot
	Registry        []fts.DocID
	Tombstones      []uint64
	Filter          *filterMeta
	Pipelines       map[string]fts.PipelineDescriptor
	KeyGenerator    *fts.KeyGeneratorDescriptor
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
	segmentManifestVersion  uint16 = 5
	segmentManifestFile            = "manifest.gob"
	segmentFilterFile              = "filter.fidx"
	segmentCurrentFile             = "CURRENT"
	segmentGenerationsDir          = ".generations"
	segmentLockFile                = ".ftspersist.lock"
	segmentLeaseFile               = ".lease"
	defaultMaxManifestBytes        = int64(256 << 20)
	defaultMaxFilterBytes          = int64(512 << 20)
)

var errInvalidSegmentGeneration = errors.New("ftspersist: invalid segment generation")

// SaveSegment writes an immutable generation and atomically publishes it
// through the segment directory's CURRENT file. The service must not be mutated
// during the call; searches may continue concurrently.
func SaveSegment(ctx context.Context, paths SegmentPaths, svc *fts.Service, filterName string, opts SaveOptions) (err error) {
	if ctx == nil {
		return fmt.Errorf("ftspersist: save segment: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if svc == nil {
		return fmt.Errorf("ftspersist: save segment: nil service")
	}
	if paths.Dir == "" {
		return fmt.Errorf("ftspersist: save segment: empty dir")
	}
	if opts.Registry == nil {
		return fmt.Errorf("ftspersist: save segment: nil snapshot registry")
	}
	keyGenerator, ok := svc.KeyGeneratorDescriptor()
	if !ok {
		return fmt.Errorf("ftspersist: save segment: key generator descriptor is unavailable")
	}
	pipelines, err := svc.PipelineDescriptors()
	if err != nil {
		return fmt.Errorf("ftspersist: save segment: %w", err)
	}
	fields, searchFilter := svc.SnapshotFields()
	if len(fields) == 0 && !svc.IsMultiField() {
		return fmt.Errorf("ftspersist: save segment: no fields")
	}
	multiField := svc.IsMultiField()
	collectionStats := svc.SnapshotCollectionStats()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()

	if err := os.MkdirAll(paths.Dir, 0o755); err != nil {
		return fmt.Errorf("ftspersist: save segment: create root: %w", err)
	}
	rootLock, _, err := acquireFileLock(filepath.Join(paths.Dir, segmentLockFile), false, false)
	if err != nil {
		return fmt.Errorf("ftspersist: save segment: lock root: %w", err)
	}
	defer func() {
		err = errors.Join(err, rootLock.Close())
	}()
	generationsDir := filepath.Join(paths.Dir, segmentGenerationsDir)
	if err := os.MkdirAll(generationsDir, 0o755); err != nil {
		return fmt.Errorf("ftspersist: save segment: create generations: %w", err)
	}
	generationDir, err := os.MkdirTemp(generationsDir, "generation-")
	if err != nil {
		return fmt.Errorf("ftspersist: save segment: create generation: %w", err)
	}
	published := false
	defer func() {
		if !published {
			err = errors.Join(err, os.RemoveAll(generationDir))
		}
	}()
	leasePath := filepath.Join(generationDir, segmentLeaseFile)
	leaseFile, err := os.OpenFile(leasePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("ftspersist: save segment: create generation lease: %w", err)
	}
	if err := leaseFile.Close(); err != nil {
		return fmt.Errorf("ftspersist: save segment: close generation lease: %w", err)
	}

	fieldNames := make([]string, 0, len(fields))
	for fieldName := range fields {
		fieldNames = append(fieldNames, fieldName)
	}
	slices.Sort(fieldNames)
	manifest := segmentManifest{
		Version:         segmentManifestVersion,
		MultiField:      multiField,
		Fields:          make([]segmentFieldMeta, 0, len(fieldNames)),
		CollectionStats: collectionStats,
		Registry:        append([]fts.DocID(nil), registry...),
		Tombstones:      append([]uint64(nil), tombstones...),
		Pipelines:       pipelines,
		KeyGenerator:    &keyGenerator,
	}

	for _, fieldName := range fieldNames {
		if err := ctx.Err(); err != nil {
			return err
		}
		fieldIndex := fields[fieldName]
		source, ok := fieldIndex.(posting.Exporter)
		if !ok {
			return fmt.Errorf("ftspersist: save segment: field %q does not support segment export", fieldName)
		}
		fileName := segmentFieldFileName(fieldName)
		path := filepath.Join(generationDir, fileName)
		if err := saveAtomicWithOptions(path, opts, func(w io.Writer) error {
			return ftse.WriteFromSourceWithTombstones(ctx, w, source, manifest.Tombstones)
		}); err != nil {
			return fmt.Errorf("ftspersist: save segment field %q: %w", fieldName, err)
		}
		manifest.Fields = append(manifest.Fields, segmentFieldMeta{FieldName: fieldName, FileName: fileName})
	}

	if searchFilter != nil && filterName != "" {
		manifest.Filter = &filterMeta{FilterName: filterName, FileName: segmentFilterFile}
		filterPath := filepath.Join(generationDir, segmentFilterFile)
		if err := saveAtomicWithOptions(filterPath, opts, func(w io.Writer) error {
			return opts.Registry.SaveFilterSnapshot(w, filterName, searchFilter)
		}); err != nil {
			return fmt.Errorf("ftspersist: save segment filter: %w", err)
		}
	}

	manifestPath := filepath.Join(generationDir, segmentManifestFile)
	if err := saveAtomicWithOptions(manifestPath, opts, func(w io.Writer) error {
		return gob.NewEncoder(w).Encode(manifest)
	}); err != nil {
		return fmt.Errorf("ftspersist: save segment manifest: %w", err)
	}

	if opts.SyncFile {
		if err := syncDir(generationDir); err != nil {
			return fmt.Errorf("ftspersist: save segment: sync generation: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	generation := filepath.Base(generationDir)
	currentGeneration, hasCurrentGeneration, err := readCurrentSegmentGeneration(paths.Dir)
	canPrune := err == nil
	if err != nil && !errors.Is(err, errInvalidSegmentGeneration) {
		return err
	}
	keepGenerations := map[string]struct{}{generation: {}}
	if hasCurrentGeneration {
		keepGenerations[currentGeneration] = struct{}{}
	}
	if canPrune {
		if err := pruneSegmentGenerations(ctx, generationsDir, keepGenerations); err != nil {
			return fmt.Errorf("ftspersist: save segment: prune generations: %w", err)
		}
	}
	if opts.SyncFile {
		if err := syncDir(generationsDir); err != nil {
			return fmt.Errorf("ftspersist: save segment: sync generations directory: %w", err)
		}
	}
	currentPath := filepath.Join(paths.Dir, segmentCurrentFile)
	if err := saveAtomicWithOptions(currentPath, opts, func(w io.Writer) error {
		_, writeErr := io.WriteString(w, generation+"\n")
		return writeErr
	}); err != nil {
		current, readErr := os.ReadFile(currentPath)
		published = readErr == nil && strings.TrimSpace(string(current)) == generation
		return fmt.Errorf("ftspersist: publish segment generation: %w", err)
	}
	published = true
	return nil
}

func loadSegmentData(paths SegmentPaths, load SegmentLoadOptions) (*loadedSegment, error) {
	if paths.Dir == "" {
		return nil, fmt.Errorf("ftspersist: load segment: empty dir")
	}
	if load.Registry == nil {
		return nil, fmt.Errorf("ftspersist: load segment: nil snapshot registry")
	}
	if load.Access == "" {
		load.Access = AccessFile
	}
	if load.Access != AccessFile && load.Access != AccessMmap {
		return nil, fmt.Errorf("ftspersist: load segment: unsupported access mode %q", load.Access)
	}

	dataDir, generation, err := resolveSegmentDataDir(paths.Dir)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(dataDir, segmentManifestFile)
	manifestBytes, err := readFileLimited(manifestPath, load.MaxManifestBytes, defaultMaxManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("ftspersist: load segment manifest %q: %w", manifestPath, err)
	}
	var manifest segmentManifest
	if err := gob.NewDecoder(bytes.NewReader(manifestBytes)).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("ftspersist: decode segment manifest: %w", err)
	}
	if manifest.Version != segmentManifestVersion {
		if manifest.Version > 0 && manifest.Version < segmentManifestVersion {
			if manifest.Version == segmentManifestVersion-1 {
				return nil, fmt.Errorf("ftspersist: load segment: legacy manifest version %d uses analyzer metadata: %w", manifest.Version, fts.ErrPersistenceRebuildRequired)
			}
			return nil, fmt.Errorf("ftspersist: load segment: legacy manifest version %d: %w", manifest.Version, fts.ErrPersistenceRebuildRequired)
		}
		return nil, fmt.Errorf("ftspersist: load segment: unsupported manifest version %d", manifest.Version)
	}
	if len(manifest.Fields) == 0 && !manifest.MultiField {
		return nil, fmt.Errorf("ftspersist: load segment: manifest has no fields")
	}
	seenFields := make(map[string]struct{}, len(manifest.Fields))
	seenFiles := make(map[string]struct{}, len(manifest.Fields))
	for _, field := range manifest.Fields {
		if field.FieldName == "" {
			return nil, fmt.Errorf("ftspersist: load segment: empty field name in manifest")
		}
		if _, exists := seenFields[field.FieldName]; exists {
			return nil, fmt.Errorf("ftspersist: load segment: duplicate field %q", field.FieldName)
		}
		seenFields[field.FieldName] = struct{}{}
		if err := validateSegmentFileName(field.FileName); err != nil {
			return nil, fmt.Errorf("ftspersist: load segment: invalid file for field %q: %w", field.FieldName, err)
		}
		if _, exists := seenFiles[field.FileName]; exists {
			return nil, fmt.Errorf("ftspersist: load segment: duplicate file %q", field.FileName)
		}
		seenFiles[field.FileName] = struct{}{}
	}
	fieldNames := make([]string, 0, len(manifest.Fields))
	for _, field := range manifest.Fields {
		fieldNames = append(fieldNames, field.FieldName)
	}
	if err := fts.ValidatePipelineFields(fieldNames, manifest.Pipelines); err != nil {
		return nil, fmt.Errorf("ftspersist: load segment: %w", err)
	}
	if manifest.Filter != nil {
		if manifest.Filter.FilterName == "" {
			return nil, fmt.Errorf("ftspersist: load segment: empty filter name")
		}
		if err := validateSegmentFileName(manifest.Filter.FileName); err != nil {
			return nil, fmt.Errorf("ftspersist: load segment: invalid filter file: %w", err)
		}
	}

	loaded := &loadedSegment{
		Fields:          make(map[string]*ftse.Reader, len(manifest.Fields)),
		MultiField:      manifest.MultiField,
		CollectionStats: manifest.CollectionStats,
		Registry:        append([]fts.DocID(nil), manifest.Registry...),
		Tombstones:      append([]uint64(nil), manifest.Tombstones...),
		Pipelines:       clonePipelineDescriptors(manifest.Pipelines),
		KeyGenerator:    cloneKeyGeneratorDescriptor(manifest.KeyGenerator),
		Generation:      generation,
		Close:           func() error { return nil },
	}
	closers := make([]func() error, 0, len(manifest.Fields))

	for _, field := range manifest.Fields {
		segmentPath := filepath.Join(dataDir, field.FileName)
		var reader *ftse.Reader
		switch load.Access {
		case AccessFile:
			fileReader, err := ftse.OpenFileReader(segmentPath)
			if err != nil {
				closeErr := closeAllReverse(closers)
				return nil, errors.Join(
					fmt.Errorf("ftspersist: load segment field %q from %q: %w", field.FieldName, segmentPath, err),
					wrapError(closeErr, "ftspersist: clean up opened segment fields"),
				)
			}
			reader = fileReader.Reader
			closers = append(closers, fileReader.Close)
		case AccessMmap:
			mapped, err := ftse.OpenFile(segmentPath)
			if err != nil {
				closeErr := closeAllReverse(closers)
				return nil, errors.Join(
					fmt.Errorf("ftspersist: load segment field %q from %q: %w", field.FieldName, segmentPath, err),
					wrapError(closeErr, "ftspersist: clean up opened segment fields"),
				)
			}
			reader = mapped.Reader
			closers = append(closers, mapped.Close)
		}
		loaded.Fields[field.FieldName] = reader
	}
	if len(closers) > 0 {
		loaded.Close = onceClose(func() error {
			err := closeAllReverse(closers)
			closers = nil
			return err
		})
	}
	if manifest.Filter != nil {
		filterPath := filepath.Join(dataDir, manifest.Filter.FileName)
		filterFile, err := os.Open(filterPath)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("ftspersist: load segment filter %q: %w", filterPath, err),
				loaded.Close(),
			)
		}
		info, statErr := filterFile.Stat()
		maxFilterBytes := load.MaxFilterBytes
		if maxFilterBytes <= 0 {
			maxFilterBytes = defaultMaxFilterBytes
		}
		if statErr != nil || info.Size() > maxFilterBytes {
			closeErr := filterFile.Close()
			cleanupErr := loaded.Close()
			if statErr != nil {
				return nil, errors.Join(fmt.Errorf("ftspersist: stat segment filter %q: %w", filterPath, statErr), closeErr, cleanupErr)
			}
			return nil, errors.Join(fmt.Errorf("ftspersist: segment filter %q size %d exceeds limit %d", filterPath, info.Size(), maxFilterBytes), closeErr, cleanupErr)
		}
		loadedFilter, loadErr := load.Registry.LoadFilterSnapshot(io.LimitReader(filterFile, maxFilterBytes+1))
		closeErr := filterFile.Close()
		if loadErr != nil || closeErr != nil {
			cleanupErr := loaded.Close()
			return nil, errors.Join(
				wrapError(loadErr, "ftspersist: decode segment filter %q", filterPath),
				wrapError(closeErr, "ftspersist: close segment filter %q", filterPath),
				cleanupErr,
			)
		}
		loaded.FilterName = loadedFilter.FilterName
		loaded.Filter = loadedFilter.Filter
	}

	return loaded, nil
}

func resolveSegmentDataDir(root string) (string, string, error) {
	generation, ok, err := readCurrentSegmentGeneration(root)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return root, "", nil
	}
	return filepath.Join(root, segmentGenerationsDir, generation), generation, nil
}

func readCurrentSegmentGeneration(root string) (string, bool, error) {
	currentPath := filepath.Join(root, segmentCurrentFile)
	data, err := os.ReadFile(currentPath)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ftspersist: read segment generation: %w", err)
	}
	generation := strings.TrimSpace(string(data))
	if generation == "" || filepath.Base(generation) != generation || !strings.HasPrefix(generation, "generation-") {
		return "", false, fmt.Errorf("%w %q", errInvalidSegmentGeneration, generation)
	}
	return generation, true, nil
}

func pruneSegmentGenerations(ctx context.Context, generationsDir string, keep map[string]struct{}) error {
	entries, err := os.ReadDir(generationsDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "generation-") {
			continue
		}
		if _, ok := keep[entry.Name()]; ok {
			continue
		}
		lease, acquired, err := acquireFileLock(
			filepath.Join(generationsDir, entry.Name(), segmentLeaseFile),
			false,
			true,
		)
		if err != nil {
			return fmt.Errorf("lock %q for pruning: %w", entry.Name(), err)
		}
		if !acquired {
			continue
		}
		if err := lease.Close(); err != nil {
			return fmt.Errorf("unlock %q before pruning: %w", entry.Name(), err)
		}
		if err := os.RemoveAll(filepath.Join(generationsDir, entry.Name())); err != nil {
			return fmt.Errorf("remove %q: %w", entry.Name(), err)
		}
	}
	return nil
}

func cloneKeyGeneratorDescriptor(descriptor *fts.KeyGeneratorDescriptor) *fts.KeyGeneratorDescriptor {
	if descriptor == nil {
		return nil
	}
	cloned := *descriptor
	return &cloned
}

func validateSegmentFileName(fileName string) error {
	if fileName == "" {
		return fmt.Errorf("empty file name")
	}
	if fileName == "." || fileName == ".." || filepath.Base(fileName) != fileName {
		return fmt.Errorf("file name %q must not contain a path", fileName)
	}
	return nil
}

func restoreSegmentService(loaded *loadedSegment, keyGen fts.KeyGenerator, opts ...*fts.Option) (*fts.Service, error) {
	if loaded == nil {
		return nil, fmt.Errorf("ftspersist: restore segment service: nil segment")
	}
	indexes := make(map[string]fts.Index, len(loaded.Fields))
	for fieldName, reader := range loaded.Fields {
		indexes[fieldName] = reader
	}
	service, err := sealedstate.Restore(sealedstate.State{
		Fields:          indexes,
		MultiField:      loaded.MultiField,
		Filter:          loaded.Filter,
		CollectionStats: loaded.CollectionStats,
		Registry:        loaded.Registry,
		Tombstones:      loaded.Tombstones,
		Pipelines:       loaded.Pipelines,
		KeyGenerator:    loaded.KeyGenerator,
	}, keyGen, opts...)
	if err != nil {
		return nil, fmt.Errorf("ftspersist: restore segment service: %w", err)
	}
	return service, nil
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

func LoadSegment(paths SegmentPaths, keyGen fts.KeyGenerator, load SegmentLoadOptions, opts ...*fts.Option) (*LoadedService, error) {
	if paths.Dir == "" {
		return nil, fmt.Errorf("ftspersist: load segment: empty dir")
	}
	if load.Registry == nil {
		return nil, fmt.Errorf("ftspersist: load segment: nil snapshot registry")
	}
	if load.Access != "" && load.Access != AccessFile && load.Access != AccessMmap {
		return nil, fmt.Errorf("ftspersist: load segment: unsupported access mode %q", load.Access)
	}
	rootLock, _, err := acquireFileLock(filepath.Join(paths.Dir, segmentLockFile), true, false)
	if err != nil {
		return nil, fmt.Errorf("ftspersist: load segment: lock root: %w", err)
	}
	loadedSegment, err := loadSegmentData(paths, load)
	if err != nil {
		lockErr := rootLock.Close()
		return nil, errors.Join(err, lockErr)
	}
	var generationLease *fileLock
	if loadedSegment.Generation != "" {
		generationLease, _, err = acquireFileLock(
			filepath.Join(paths.Dir, segmentGenerationsDir, loadedSegment.Generation, segmentLeaseFile),
			true,
			false,
		)
		if err != nil {
			closeErr := loadedSegment.Close()
			lockErr := rootLock.Close()
			return nil, errors.Join(
				fmt.Errorf("ftspersist: load segment: lease generation: %w", err),
				closeErr,
				lockErr,
			)
		}
	}
	baseClose := loadedSegment.Close
	loadedSegment.Close = onceClose(func() error {
		closeErr := baseClose()
		if generationLease != nil {
			closeErr = errors.Join(closeErr, generationLease.Close())
		}
		return closeErr
	})
	rootLockErr := rootLock.Close()
	if rootLockErr != nil {
		return nil, errors.Join(
			fmt.Errorf("ftspersist: load segment: unlock root: %w", rootLockErr),
			loadedSegment.Close(),
		)
	}

	service, err := restoreSegmentService(loadedSegment, keyGen, opts...)
	if err != nil {
		return nil, errors.Join(err, loadedSegment.Close())
	}
	return &LoadedService{Service: service, Close: loadedSegment.Close, FilterName: loadedSegment.FilterName}, nil
}

func segmentFieldFileName(fieldName string) string {
	if fieldName == "" || fieldName == fts.DefaultField {
		return "_default.ftse.fidx"
	}
	return url.PathEscape(fieldName) + ".ftse.fidx"
}

func closeAllReverse(closers []func() error) error {
	var joined error
	for i := len(closers) - 1; i >= 0; i-- {
		if closers[i] == nil {
			continue
		}
		if err := closers[i](); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}
