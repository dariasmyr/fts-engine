package semanticpersist

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
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

// PublishSealedSegment writes one complete generation and atomically switches CURRENT.
// Synchronous mode fsyncs files and affected directories; asynchronous mode
// only provides atomic process-visible publication, not power-loss durability.
func PublishSealedSegment(ctx context.Context, root string, generationID uint64, sealed SealedSegment, options Options) (Generation, error) {
	if ctx == nil {
		return Generation{}, vector.ErrNilContext
	}
	if generationID == 0 || root == "" || (options.Durability != 0 && options.Durability != DurabilitySynchronous && options.Durability != DurabilityAsynchronous) {
		return Generation{}, ErrCorrupt
	}
	if options.Durability == 0 {
		options.Durability = DurabilitySynchronous
	}
	options.Limits = normalizeLimits(options.Limits)
	if err := validateLimits(options.Limits); err != nil {
		return Generation{}, err
	}
	if err := validateSealedSegment(sealed, options.Limits); err != nil {
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
	currentGeneration, err := validatedCurrentGeneration(paths, options.Limits)
	if err != nil {
		return Generation{}, err
	}
	if currentGeneration != options.ExpectedGeneration || generationID <= currentGeneration {
		return Generation{}, ErrStaleGeneration
	}
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

	object, err := writeSegmentObject(ctx, paths, sealed, options)
	if err != nil {
		return Generation{}, err
	}
	manifestRef, err := writeGeneration(ctx, paths, generationID, object, sealed, options)
	if err != nil {
		return Generation{}, err
	}
	return commitCurrent(ctx, paths, generationID, object.ID, manifestRef, options)
}

// Publish is the short name for publishing one sealed segment generation.
func Publish(ctx context.Context, root string, generationID uint64, sealed SealedSegment, options Options) (Generation, error) {
	return PublishSealedSegment(ctx, root, generationID, sealed, options)
}

func Open(root string, options OpenOptions) (*Loaded, error) {
	limits := normalizeLimits(options.Limits)
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	paths, err := validateStoreDirectories(root)
	if err != nil {
		return nil, err
	}
	// Retain the shared lock in Loaded until Close so no writer can publish or
	// repair the store while this reader is being opened or used.
	storeLock, err := acquireStoreReadLock(filepath.Join(paths.root, "LOCK"))
	if err != nil {
		return nil, err
	}
	loaded, err := openCurrent(paths, limits, options.ExpectedDescriptors)
	if err != nil {
		_ = storeLock.Close()
		return nil, err
	}
	loaded.storeLock = storeLock
	return loaded, nil
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

func validateSealedSegment(sealed SealedSegment, limits Limits) error {
	if sealed.Segment == nil || sealed.Segment.Vectors() == nil ||
		sealed.Segment.Dimensions() > limits.MaxDimensions || sealed.Segment.Len() > limits.MaxVectors ||
		sealed.Segment.MaxK() > limits.MaxK || sealed.Segment.Len() > limits.MaxVectors ||
		sealed.MaxK > limits.MaxK || sealed.MaxChunkCandidates > limits.MaxK {
		return ErrLimitExceeded
	}
	if err := sealed.Segment.Validate(); err != nil {
		return err
	}
	components, ok := checkedMultiply64(uint64(sealed.Segment.Len()), uint64(sealed.Segment.Dimensions()))
	if !ok {
		return ErrLimitExceeded
	}
	vectorBytes, ok := checkedMultiply64(components, 4)
	if !ok || vectorBytes > limits.MaxVectorBytes || limits.MaxFileBytes < 44 || vectorBytes > limits.MaxFileBytes-44 {
		return ErrLimitExceeded
	}
	if sealed.Segment.Kind() == semantic.SegmentKindChunkHNSW {
		index := sealed.Segment.Index()
		if index == nil {
			return ErrLimitExceeded
		}
		search := index.SearchConfig()
		if search.MaxK > limits.MaxK || search.MaxEfSearch > limits.MaxEfSearch || search.MaxVisitLimit > limits.MaxVisitLimit ||
			uint64(index.StorageStats().DirectedLinks) > limits.MaxGraphLinks ||
			graphFileSize(index) > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
			return ErrLimitExceeded
		}
	}
	validString := func(value string) bool { return len(value) <= limits.MaxStringBytes }
	metadata := sealed.Segment.Metadata()
	if !validString(metadata.Embedding.ProviderID) || !validString(metadata.Embedding.ModelID) || !validString(metadata.Embedding.ModelVersion) || !validString(metadata.Embedding.PipelineFingerprint) ||
		!validString(metadata.Chunking.ID) || !validString(metadata.Chunking.Fingerprint) {
		return ErrLimitExceeded
	}
	documentChunks := make(map[string]int)
	for _, record := range sealed.Segment.Rows() {
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
	vectorFileLimit := min(limits.MaxFileBytes, limits.MaxVectorBytes+128)
	graphFileLimit := min(limits.MaxFileBytes, limits.MaxGraphBytes)
	if value.Vectors.Size > vectorFileLimit || value.Graph.Size > graphFileLimit || value.State.Size > limits.MaxFileBytes {
		return ErrLimitExceeded
	}
	// Account conservatively for decoded slices, strings, validation maps, and
	// the immutable reader copies before allocating referenced file buffers.
	estimate := uint64(len(manifestData))
	for _, component := range []struct {
		size       uint64
		multiplier uint64
	}{
		{value.Vectors.Size, 4},
		{value.Graph.Size, 8},
		{value.State.Size, 16},
	} {
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
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("semanticpersist: read: %w", err)
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
	data, err := io.ReadAll(file)
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

func beforeStep(ctx context.Context, options Options, step PublicationStep) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if options.BeforeStep != nil {
		return options.BeforeStep(step)
	}
	return nil
}

func afterStep(options Options, step PublicationStep, committed bool) error {
	var err error
	if options.AfterStep != nil {
		err = options.AfterStep(step)
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

func validatedCurrentGeneration(paths storePaths, limits Limits) (uint64, error) {
	loaded, err := openCurrent(paths, limits, semantic.PipelineDescriptor{})
	if errors.Is(err, ErrCurrentMissing) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	id := loaded.Generation.ID
	if err := loaded.Close(); err != nil {
		return 0, err
	}
	return id, nil
}
