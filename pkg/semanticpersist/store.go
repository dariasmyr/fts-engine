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
func Publish(ctx context.Context, root string, service *semantic.Service, options Options) (Generation, error) {
	if ctx == nil {
		return Generation{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Generation{}, err
	}
	if service == nil || root == "" || (options.Durability != 0 && options.Durability != DurabilitySynchronous && options.Durability != DurabilityAsynchronous) {
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
		for _, path := range []string{paths.segments, paths.objects, paths.generations, paths.root} {
			if err := syncDirectory(path); err != nil {
				return Generation{}, err
			}
		}
		if rootCreated {
			if err := syncDirectory(filepath.Dir(paths.root)); err != nil {
				return Generation{}, err
			}
		}
	}
	return publishLocked(ctx, paths, service, options)
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

func Open(ctx context.Context, root string, options OpenOptions) (*Store, error) {
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
	service, generation, err := openCurrent(ctx, paths, limits, options.ExpectedDescriptors)
	if err != nil {
		_ = storeLock.Close()
		return nil, err
	}
	return &Store{service: service, paths: paths, limits: limits, generation: generation, storeLock: storeLock, publishGate: make(chan struct{}, 1)}, nil
}

func publishLocked(ctx context.Context, paths storePaths, service *semantic.Service, options Options) (Generation, error) {
	if err := ctx.Err(); err != nil {
		return Generation{}, err
	}
	currentGeneration, err := validatedCurrentGeneration(ctx, paths, options.Limits)
	if err != nil {
		return Generation{}, err
	}
	if currentGeneration != options.ExpectedGeneration || currentGeneration == math.MaxUint64 {
		return Generation{}, ErrStaleGeneration
	}
	snapshot, err := service.CommittedSnapshot(ctx)
	if err != nil {
		return Generation{}, err
	}
	segments := snapshot.Segments()
	objects := make([]segmentObject, len(segments))
	for i, persisted := range segments {
		segment := persisted.Snapshot()
		if err := validateSegment(segment, snapshot.Config(), options.Limits); err != nil {
			return Generation{}, err
		}
		objects[i], err = writeSegmentObject(ctx, paths, segment, options)
		if err != nil {
			return Generation{}, err
		}
	}
	generationID := currentGeneration + 1
	manifestRef, err := writeGeneration(ctx, paths, generationID, objects, snapshot, options)
	if err != nil {
		return Generation{}, err
	}
	return commitCurrent(ctx, paths, generationID, manifestRef, options)
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

func validateSegment(segment semantic.SegmentSnapshot, config semantic.Config, limits Limits) error {
	if segment.Vectors() == nil || segment.Index() == nil ||
		segment.Dimensions() > limits.MaxDimensions || segment.Len() > limits.MaxVectors ||
		segment.MaxK() > limits.MaxK || config.MaxK <= 0 || config.MaxK > limits.MaxK || config.MaxK > segment.MaxK() ||
		config.MaxChunkCandidates < config.MaxK || config.MaxChunkCandidates > limits.MaxK || config.MaxChunkCandidates > segment.MaxK() ||
		config.MaxChunksPerDocumentHit <= 0 || config.MaxChunksPerDocumentHit > limits.MaxChunksPerDocument {
		return ErrLimitExceeded
	}
	components, ok := checkedMultiply64(uint64(segment.Len()), uint64(segment.Dimensions()))
	if !ok {
		return ErrLimitExceeded
	}
	vectorBytes, ok := checkedMultiply64(components, 4)
	if !ok || vectorBytes > limits.MaxVectorBytes || limits.MaxFileBytes < 44 || vectorBytes > limits.MaxFileBytes-44 {
		return ErrLimitExceeded
	}
	index := segment.Index()
	search := segment.SearchConfig()
	report := index.Report()
	if search.MaxK > limits.MaxK || search.MaxEfSearch > limits.MaxEfSearch || search.MaxVisitLimit > limits.MaxVisitLimit ||
		uint64(report.Storage.DirectedLinks) > limits.MaxGraphLinks ||
		graphFileSize(index) > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
		return ErrLimitExceeded
	}
	validString := func(value string) bool { return len(value) <= limits.MaxStringBytes }
	descriptor := segment.Pipeline()
	if !validString(descriptor.Embedding.ProviderID) || !validString(descriptor.Embedding.ModelID) || !validString(descriptor.Embedding.ModelVersion) || !validString(descriptor.Embedding.PipelineFingerprint) ||
		!validString(descriptor.Chunking.ID) || !validString(descriptor.Chunking.Fingerprint) {
		return ErrLimitExceeded
	}
	documentChunks := make(map[string]int)
	for _, record := range segment.Rows() {
		if !validString(string(record.Chunk.ID)) || !validString(string(record.Chunk.DocID)) || !validString(record.Chunk.Field) {
			return ErrLimitExceeded
		}
		docID := string(record.Chunk.DocID)
		documentChunks[docID]++
		if documentChunks[docID] > limits.MaxChunksPerDocument {
			return ErrLimitExceeded
		}
	}
	if len(documentChunks) > limits.MaxDocuments {
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
		weighted, ok := checkedMultiply64(component.size, component.multiplier)
		if !ok {
			return ErrLimitExceeded
		}
		estimate, ok = checkedAdd64(estimate, weighted)
		if !ok {
			return ErrLimitExceeded
		}
	}
	scratchBytes, ok := checkedMultiply64(uint64(limits.MaxDimensions), 8)
	if !ok {
		return ErrLimitExceeded
	}
	estimate, ok = checkedAdd64(estimate, scratchBytes)
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
	if uint64(len(data)) != reference.Size || sha256.Sum256(data) != reference.SHA256 {
		return nil, ErrCorrupt
	}
	return data, nil
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

func generationName(id uint64) string { return fmt.Sprintf("%020d", id) }

func validatedCurrentGeneration(ctx context.Context, paths storePaths, limits Limits) (uint64, error) {
	_, generation, err := openCurrent(ctx, paths, limits, semantic.PipelineDescriptor{})
	if errors.Is(err, ErrCurrentMissing) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return generation.ID, nil
}
