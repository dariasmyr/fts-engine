package semanticpersist

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/arithmetic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const (
	currentFileName      = "CURRENT"
	objectsDirectory     = "objects"
	segmentsDirectory    = "segments"
	generationsDirectory = "generations"
	vectorsFileName      = "vectors.bin"
	graphFileName        = "graph.bin"
	manifestFileName     = "manifest.bin"
	stateFileName        = "semantic-state.bin"
)

// Publish writes a detached service's clean committed state and atomically
// switches CURRENT. Opened stores publish subsequent generations through Store.
func Publish(
	ctx context.Context,
	root string,
	service *semantic.Service,
	options Options,
) (Generation, error) {
	if ctx == nil {
		return Generation{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Generation{}, err
	}
	if service == nil ||
		root == "" ||
		(options.Durability != 0 &&
			options.Durability != DurabilitySynchronous &&
			options.Durability != DurabilityAsynchronous) {
		return Generation{}, ErrCorrupt
	}

	if err := normalizeOptions(&options); err != nil {
		return Generation{}, err
	}

	paths, rootCreated, err := prepareStoreDirectories(root)
	if err != nil {
		return Generation{}, err
	}

	// Serialize the CURRENT check and the complete publication across processes.
	// Without one lock, two writers could both validate the same base generation.
	storeLock, err := acquireStoreLock(filepath.Join(paths.root, "LOCK"))
	if err != nil {
		return Generation{}, err
	}
	defer storeLock.Close()

	if options.Durability == DurabilitySynchronous {
		if err := syncDirectory(paths.segments); err != nil {
			return Generation{}, err
		}
		if err := syncDirectory(paths.objects); err != nil {
			return Generation{}, err
		}
		if err := syncDirectory(paths.generations); err != nil {
			return Generation{}, err
		}
		if err := syncDirectory(paths.root); err != nil {
			return Generation{}, err
		}

		if rootCreated {
			if err := syncDirectory(filepath.Dir(paths.root)); err != nil {
				return Generation{}, err
			}
		}
	}

	return publishDetached(ctx, paths, service, options)
}

func Open(
	ctx context.Context,
	root string,
	options OpenOptions,
) (*Store, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	limits := normalizeLimits(options.Limits)
	if err := validateLimits(limits); err != nil {
		return nil, err
	}

	paths, err := validateStoreDirectories(root)
	if err != nil {
		return nil, err
	}

	storeLock, err := acquireStoreLock(filepath.Join(paths.root, "LOCK"))
	if err != nil {
		return nil, err
	}

	service, generation, manifestValue, err := openCurrent(
		ctx,
		paths,
		limits,
		options.ExpectedDescriptors,
	)
	if err != nil {
		_ = storeLock.Close()
		return nil, err
	}

	return &Store{
		service:      service,
		paths:        paths,
		limits:       limits,
		generation:   generation,
		segmentFiles: componentSegmentFiles(manifestValue),
		storeLock:    storeLock,
		publishGate:  make(chan struct{}, 1),
	}, nil
}

func normalizeOptions(options *Options) error {
	if options.Durability == 0 {
		options.Durability = DurabilitySynchronous
	}
	if options.Durability != DurabilitySynchronous && options.Durability != DurabilityAsynchronous {
		return ErrCorrupt
	}
	options.Limits = normalizeLimits(options.Limits)
	return validateLimits(options.Limits)
}

func publishDetached(ctx context.Context, paths storePaths, service *semantic.Service, options Options) (Generation, error) {
	currentGeneration, err := fullyValidatedCurrentGeneration(ctx, paths, options.Limits)
	if err != nil {
		return Generation{}, err
	}
	generation, _, err := publishValidated(ctx, paths, service, options, currentGeneration, nil)
	return generation, err
}

func publishFromOpenedStore(ctx context.Context, paths storePaths, service *semantic.Service, options Options, reusable map[uint64]manifestSegment) (Generation, map[uint64]manifestSegment, error) {
	currentGeneration, err := manifestValidatedCurrentGeneration(ctx, paths, options.Limits)
	if err != nil {
		return Generation{}, nil, err
	}
	return publishValidated(ctx, paths, service, options, currentGeneration, reusable)
}

func publishValidated(ctx context.Context, paths storePaths, service *semantic.Service, options Options, currentGeneration uint64, reusable map[uint64]manifestSegment) (Generation, map[uint64]manifestSegment, error) {
	if err := ctx.Err(); err != nil {
		return Generation{}, nil, err
	}
	if currentGeneration != options.ExpectedGeneration || currentGeneration == math.MaxUint64 {
		return Generation{}, nil, ErrStaleGeneration
	}
	state, err := service.CommittedState(ctx)
	if err != nil {
		return Generation{}, nil, err
	}
	captured, err := captureCommittedState(state, options.Limits)
	if err != nil {
		return Generation{}, nil, err
	}
	segmentFiles := make([]manifestSegment, len(captured.segments))
	reusedSegmentFiles := false
	for i, persisted := range captured.segments {
		if err := validateSegmentData(persisted.data, captured.config, options.Limits); err != nil {
			return Generation{}, nil, err
		}
		if files, ok := reusable[persisted.data.ComponentID]; ok {
			if err := verifyStoredSegment(filepath.Join(paths.segments, files.ObjectID), files.Vectors, files.Graph, options.Limits, options.Durability); err != nil {
				return Generation{}, nil, err
			}
			segmentFiles[i] = files
			reusedSegmentFiles = true
			continue
		}
		segmentFiles[i], err = writeStoredSegment(ctx, paths, persisted.data.Vectors, persisted.data.Index, options)
		if err != nil {
			return Generation{}, nil, err
		}
	}
	if reusedSegmentFiles && options.Durability == DurabilitySynchronous {
		if err := beforeStep(ctx, options, stepSyncReusedSegments); err != nil {
			return Generation{}, nil, err
		}
		if err := syncDirectory(paths.segments); err != nil {
			return Generation{}, nil, err
		}
		if err := afterStep(options, stepSyncReusedSegments, false); err != nil {
			return Generation{}, nil, err
		}
	}
	generationID := currentGeneration + 1
	manifestRef, err := writeGeneration(ctx, paths, generationID, segmentFiles, captured, options)
	if err != nil {
		return Generation{}, nil, err
	}
	generation, err := commitCurrent(ctx, paths, generationID, manifestRef, options)
	if err != nil {
		return Generation{}, nil, err
	}
	nextSegmentFiles := make(map[uint64]manifestSegment, len(segmentFiles))
	for i, persisted := range captured.segments {
		nextSegmentFiles[persisted.data.ComponentID] = segmentFiles[i]
	}
	return generation, nextSegmentFiles, nil
}

type storePaths struct {
	root, objects, segments, generations string
}

func prepareStoreDirectories(root string) (storePaths, bool, error) {
	rootCreated := false
	if info, err := os.Lstat(root); errors.Is(err, os.ErrNotExist) {
		if err := validateDirectory(filepath.Dir(root)); err != nil {
			return storePaths{}, false, err
		}
		if err := os.Mkdir(root, 0o700); err != nil {
			return storePaths{}, false, err
		}
		rootCreated = true
	} else if err != nil {
		return storePaths{}, false, err
	} else if info.Mode()&os.ModeSymlink != 0 {
		return storePaths{}, false, ErrSymlink
	} else if !info.IsDir() {
		return storePaths{}, false, ErrCorrupt
	}
	paths := storePaths{root: root, objects: filepath.Join(root, objectsDirectory), generations: filepath.Join(root, generationsDirectory)}
	paths.segments = filepath.Join(paths.objects, segmentsDirectory)
	for _, path := range []string{paths.root, paths.objects, paths.segments, paths.generations} {
		if err := ensureDirectory(path); err != nil {
			return storePaths{}, false, err
		}
	}
	return paths, rootCreated, nil
}

func validateStoreDirectories(root string) (storePaths, error) {
	paths := storePaths{root: root, objects: filepath.Join(root, objectsDirectory), generations: filepath.Join(root, generationsDirectory)}
	paths.segments = filepath.Join(paths.objects, segmentsDirectory)
	for _, path := range []string{paths.root, paths.objects, paths.segments, paths.generations} {
		if err := validateDirectory(path); err != nil {
			return storePaths{}, err
		}
	}
	return paths, nil
}

func ensureDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return validateDirectory(path)
}

func validateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ErrSymlink
	}
	if !info.IsDir() {
		return ErrCorrupt
	}
	return nil
}

func validateSegmentData(data semantic.SegmentData, config semantic.Config, limits Limits) error {
	semanticLimits := config.Limits
	if data.Vectors == nil || data.Index == nil {
		return ErrLimitExceeded
	}
	dimensions := data.Index.Dimensions()
	length := data.Index.Len()
	search := data.Index.Report().Search
	if dimensions > limits.MaxDimensions || length > limits.MaxVectors ||
		search.MaxK > limits.MaxK || semanticLimits.MaxDocumentsPerSearch <= 0 || semanticLimits.MaxDocumentsPerSearch > limits.MaxK || semanticLimits.MaxDocumentsPerSearch > search.MaxK ||
		semanticLimits.MaxChunkCandidates < semanticLimits.MaxDocumentsPerSearch || semanticLimits.MaxChunkCandidates > limits.MaxK || semanticLimits.MaxChunkCandidates > search.MaxK ||
		semanticLimits.MaxChunksPerDocumentHit <= 0 || semanticLimits.MaxChunksPerDocumentHit > limits.MaxChunksPerDocument {
		return ErrLimitExceeded
	}
	components, ok := arithmetic.CheckMultiply64(uint64(length), uint64(dimensions))
	if !ok {
		return ErrLimitExceeded
	}
	vectorBytes, ok := arithmetic.CheckMultiply64(components, 4)
	if !ok || vectorBytes > limits.MaxVectorBytes || limits.MaxFileBytes < 44 || vectorBytes > limits.MaxFileBytes-44 {
		return ErrLimitExceeded
	}
	index := data.Index
	report := index.Report()
	if search.MaxK > limits.MaxK || search.MaxEfSearch > limits.MaxEfSearch || search.MaxVisitLimit > limits.MaxVisitLimit ||
		report.Build.MaxNeighbors != config.HNSW.MaxNeighbors || report.Build.LevelZeroMaxNeighbors != config.HNSW.MaxNeighbors*2 ||
		report.Build.EfConstruction != config.HNSW.EfConstruction || report.Build.Seed != config.HNSW.Seed ||
		uint64(report.Storage.DirectedLinks) > limits.MaxGraphLinks ||
		graphFileSize(index) > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
		return ErrLimitExceeded
	}
	return nil
}

func validateOpenReferences(manifestData []byte, value manifest, limits Limits) error {
	if value.State.Size > limits.MaxFileBytes {
		return ErrLimitExceeded
	}
	// Account conservatively for decoded slices, strings, validation maps, and
	// the immutable reader copies before allocating referenced file buffers.
	estimate := uint64(len(manifestData))
	components := []struct {
		size       uint64
		multiplier uint64
	}{{value.State.Size, 16}}
	for _, segment := range value.Segments {
		if segment.Vectors.Size > min(limits.MaxFileBytes, limits.MaxVectorBytes+128) || segment.Graph.Size > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
			return ErrLimitExceeded
		}
		components = append(components, struct{ size, multiplier uint64 }{segment.Vectors.Size, 4}, struct{ size, multiplier uint64 }{segment.Graph.Size, 8})
	}
	for _, component := range components {
		weighted, ok := arithmetic.CheckMultiply64(component.size, component.multiplier)
		if !ok {
			return ErrLimitExceeded
		}
		estimate, ok = arithmetic.CheckAdd64(estimate, weighted)
		if !ok {
			return ErrLimitExceeded
		}
	}
	scratchBytes, ok := arithmetic.CheckMultiply64(uint64(limits.MaxDimensions), 8)
	if !ok {
		return ErrLimitExceeded
	}
	estimate, ok = arithmetic.CheckAdd64(estimate, scratchBytes)
	if !ok {
		return ErrLimitExceeded
	}
	if estimate > limits.MaxOpenBytes {
		return ErrLimitExceeded
	}
	return nil
}

func readRegularFile(path string, limit uint64) ([]byte, error) {
	if limit >= math.MaxInt64 {
		return nil, ErrLimitExceeded
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrSymlink
	}
	if !info.Mode().IsRegular() || uint64(info.Size()) > limit {
		return nil, ErrLimitExceeded
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("semanticpersist: read: %w", err)
	}
	if uint64(len(data)) > limit {
		return nil, ErrLimitExceeded
	}
	return data, nil
}

func readReferencedFile(path string, reference fileReference, limit uint64) ([]byte, error) {
	return readReferencedFileData(path, reference, limit, true)
}

func readReferencedFileWithoutHash(path string, reference fileReference, limit uint64) ([]byte, error) {
	return readReferencedFileData(path, reference, limit, false)
}

func readReferencedFileData(path string, reference fileReference, limit uint64, verifyHash bool) ([]byte, error) {
	if reference.Size == 0 || reference.Size > limit {
		return nil, ErrLimitExceeded
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrSymlink
	}
	if !info.Mode().IsRegular() || uint64(info.Size()) != reference.Size {
		return nil, ErrCorrupt
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(reference.Size)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) != reference.Size || verifyHash && sha256.Sum256(data) != reference.SHA256 {
		return nil, ErrCorrupt
	}
	return data, nil
}

func verifyReferencedFile(path string, reference fileReference, limit uint64, durability DurabilityMode) error {
	if reference.Size == 0 || reference.Size > limit {
		return ErrLimitExceeded
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ErrSymlink
	}
	if !info.Mode().IsRegular() || uint64(info.Size()) != reference.Size {
		return ErrCorrupt
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	hash := sha256.New()
	buffer := make([]byte, 64<<10)
	written, copyErr := io.CopyBuffer(hash, io.LimitReader(file, int64(reference.Size)+1), buffer)
	if copyErr == nil && (uint64(written) != reference.Size || !equalSHA256(hash.Sum(nil), reference.SHA256)) {
		copyErr = ErrCorrupt
	}
	if copyErr == nil && durability == DurabilitySynchronous {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func equalSHA256(sum []byte, expected [sha256.Size]byte) bool {
	if len(sum) != sha256.Size {
		return false
	}
	var actual [sha256.Size]byte
	copy(actual[:], sum)
	return actual == expected
}

func syncRegularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ErrSymlink
	}
	if !info.Mode().IsRegular() {
		return ErrCorrupt
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func beforeStep(ctx context.Context, options Options, step publicationStep) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if options.beforeStep != nil {
		return options.beforeStep(step)
	}
	return nil
}

func afterStep(options Options, step publicationStep, committed bool) error {
	var err error
	if options.afterStep != nil {
		err = options.afterStep(step)
	}
	if err != nil && committed {
		return fmt.Errorf("%w: %v", ErrIndeterminate, err)
	}
	return err
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func writeAllFile(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func ensureContained(root, path string) error {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || filepath.IsAbs(relative) || len(relative) >= 3 && relative[:3] == ".."+string(filepath.Separator) {
		return ErrPathEscape
	}
	return nil
}

func manifestValidatedCurrentGeneration(ctx context.Context, paths storePaths, limits Limits) (uint64, error) {
	current, _, err := readCurrentManifest(ctx, paths, limits)
	if errors.Is(err, ErrCurrentMissing) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return current.GenerationID, nil
}

func fullyValidatedCurrentGeneration(ctx context.Context, paths storePaths, limits Limits) (uint64, error) {
	_, generation, _, err := openCurrent(ctx, paths, limits, semantic.PipelineDescriptor{})
	if errors.Is(err, ErrCurrentMissing) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return generation.ID, nil
}

func componentSegmentFiles(value manifest) map[uint64]manifestSegment {
	if len(value.componentIDs) != len(value.Segments) {
		return nil
	}
	segmentFiles := make(map[uint64]manifestSegment, len(value.Segments))
	for i, files := range value.Segments {
		segmentFiles[value.componentIDs[i]] = files
	}
	return segmentFiles
}
