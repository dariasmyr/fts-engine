package semanticpersist

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	vectorflat "github.com/dariasmyr/fts-engine/pkg/vector/flat"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
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

// Publish writes one complete generation and atomically switches CURRENT.
// Synchronous mode fsyncs files and affected directories; asynchronous mode
// only provides atomic process-visible publication, not power-loss durability.
func Publish(ctx context.Context, root string, generationID uint64, snapshot semantic.Snapshot, options Options) (Generation, error) {
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
	if err := validateSnapshotLimits(snapshot, options.Limits); err != nil {
		return Generation{}, err
	}
	if err := snapshot.Validate(); err != nil {
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

	// Build the immutable segment under its final parent so installing it is one
	// same-filesystem rename. Until that rename, failures only leave temp work.
	segmentTemp, err := os.MkdirTemp(paths.segments, ".tmp-seg-")
	if err != nil {
		return Generation{}, fmt.Errorf("semanticpersist: create segment temp: %w", err)
	}
	segmentTempOwned := true
	defer func() {
		if segmentTempOwned {
			_ = os.RemoveAll(segmentTemp)
		}
	}()

	if err := beforeStep(ctx, options, StepWriteVectors); err != nil {
		return Generation{}, err
	}
	vectorsRef, err := writeVectorsFile(filepath.Join(segmentTemp, vectorsFileName), snapshot.Segment.Vectors(), snapshot.Segment.MaxK(), options.Durability)
	if err != nil {
		return Generation{}, err
	}
	if err := afterStep(options, StepWriteVectors, false); err != nil {
		return Generation{}, err
	}
	if vectorsRef.Size > options.Limits.MaxFileBytes || vectorsRef.Size > options.Limits.MaxVectorBytes+128 {
		return Generation{}, ErrLimitExceeded
	}
	var graphRef fileReference
	if snapshot.Segment.Kind() == semantic.SegmentKindChunkHNSW {
		if err := beforeStep(ctx, options, StepWriteGraph); err != nil {
			return Generation{}, err
		}
		graphRef, err = writeGraphFile(filepath.Join(segmentTemp, graphFileName), snapshot.Segment.HNSW(), vectorsRef, options.Durability)
		if err != nil {
			return Generation{}, err
		}
		if err := afterStep(options, StepWriteGraph, false); err != nil {
			return Generation{}, err
		}
		if graphRef.Size > min(options.Limits.MaxFileBytes, options.Limits.MaxGraphBytes) {
			return Generation{}, ErrLimitExceeded
		}
	}
	if options.Durability == DurabilitySynchronous {
		if err := beforeStep(ctx, options, StepSyncSegment); err != nil {
			return Generation{}, err
		}
		if err := syncDirectory(segmentTemp); err != nil {
			return Generation{}, err
		}
		if err := afterStep(options, StepSyncSegment, false); err != nil {
			return Generation{}, err
		}
	}

	objectID := segmentObjectID(snapshot.Segment.Kind(), vectorsRef, graphRef)
	objectPath := filepath.Join(paths.segments, objectID)
	if err := beforeStep(ctx, options, StepRenameSegment); err != nil {
		return Generation{}, err
	}
	if _, err := os.Lstat(objectPath); err == nil {
		// Content-addressed reuse is allowed only after the existing files match
		// the exact sizes and hashes produced by this snapshot.
		if err := verifyExistingObject(objectPath, snapshot.Segment.Kind(), vectorsRef, graphRef, options.Limits, options.Durability); err != nil {
			return Generation{}, err
		}
		if options.Durability == DurabilitySynchronous {
			if err := syncDirectory(paths.segments); err != nil {
				return Generation{}, err
			}
		}
		if err := os.RemoveAll(segmentTemp); err != nil {
			return Generation{}, err
		}
		segmentTempOwned = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return Generation{}, err
	} else {
		if err := os.Rename(segmentTemp, objectPath); err != nil {
			return Generation{}, fmt.Errorf("semanticpersist: install segment: %w", err)
		}
		segmentTempOwned = false
		if options.Durability == DurabilitySynchronous {
			if err := syncDirectory(paths.segments); err != nil {
				return Generation{}, err
			}
		}
	}
	if err := afterStep(options, StepRenameSegment, false); err != nil {
		return Generation{}, err
	}

	// A generation binds logical state to one immutable segment object. Installing
	// its directory does not publish it; CURRENT remains the only commit pointer.
	generationTemp, err := os.MkdirTemp(paths.generations, ".tmp-gen-")
	if err != nil {
		return Generation{}, fmt.Errorf("semanticpersist: create generation temp: %w", err)
	}
	generationTempOwned := true
	defer func() {
		if generationTempOwned {
			_ = os.RemoveAll(generationTemp)
		}
	}()
	if err := beforeStep(ctx, options, StepWriteState); err != nil {
		return Generation{}, err
	}
	stateData, stateRef, err := encodeState(snapshot, options.Limits)
	if err != nil {
		return Generation{}, err
	}
	if err := writeDataFile(filepath.Join(generationTemp, stateFileName), stateData, options.Durability); err != nil {
		return Generation{}, err
	}
	if err := afterStep(options, StepWriteState, false); err != nil {
		return Generation{}, err
	}
	manifestValue := manifest{
		Version: manifestVersion, GenerationID: generationID, ObjectID: objectID, SegmentKind: snapshot.Segment.Kind(),
		Vectors: vectorsRef, Graph: graphRef, State: stateRef,
	}
	if err := beforeStep(ctx, options, StepWriteManifest); err != nil {
		return Generation{}, err
	}
	manifestData, manifestRef, err := encodeManifest(manifestValue, options.Limits)
	if err != nil {
		return Generation{}, err
	}
	if err := validateOpenReferences(manifestData, manifestValue, options.Limits); err != nil {
		return Generation{}, err
	}
	if err := writeDataFile(filepath.Join(generationTemp, manifestFileName), manifestData, options.Durability); err != nil {
		return Generation{}, err
	}
	if err := afterStep(options, StepWriteManifest, false); err != nil {
		return Generation{}, err
	}
	if options.Durability == DurabilitySynchronous {
		if err := beforeStep(ctx, options, StepSyncGeneration); err != nil {
			return Generation{}, err
		}
		if err := syncDirectory(generationTemp); err != nil {
			return Generation{}, err
		}
		if err := afterStep(options, StepSyncGeneration, false); err != nil {
			return Generation{}, err
		}
	}
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	if _, err := os.Lstat(generationPath); err == nil {
		return Generation{}, ErrGenerationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return Generation{}, err
	}
	if err := beforeStep(ctx, options, StepRenameGeneration); err != nil {
		return Generation{}, err
	}
	if err := os.Rename(generationTemp, generationPath); err != nil {
		return Generation{}, fmt.Errorf("semanticpersist: install generation: %w", err)
	}
	generationTempOwned = false
	if options.Durability == DurabilitySynchronous {
		if err := syncDirectory(paths.generations); err != nil {
			return Generation{}, err
		}
	}
	if err := afterStep(options, StepRenameGeneration, false); err != nil {
		return Generation{}, err
	}

	// The segment and generation are now complete but still orphaned. Readers
	// continue opening the previous generation until CURRENT is replaced.
	if err := beforeStep(ctx, options, StepWriteCurrent); err != nil {
		return Generation{}, err
	}
	currentData, _, err := encodeCurrent(currentRecord{GenerationID: generationID, ManifestHash: manifestRef.SHA256}, options.Limits)
	if err != nil {
		return Generation{}, err
	}
	currentTemp, err := os.CreateTemp(paths.root, ".tmp-current-")
	if err != nil {
		return Generation{}, err
	}
	currentTempName := currentTemp.Name()
	defer os.Remove(currentTempName)
	if err := writeAllFile(currentTemp, currentData); err != nil {
		_ = currentTemp.Close()
		return Generation{}, err
	}
	if options.Durability == DurabilitySynchronous {
		if err := currentTemp.Sync(); err != nil {
			_ = currentTemp.Close()
			return Generation{}, err
		}
	}
	if err := currentTemp.Close(); err != nil {
		return Generation{}, err
	}
	if err := afterStep(options, StepWriteCurrent, false); err != nil {
		return Generation{}, err
	}
	if err := beforeStep(ctx, options, StepReplaceCurrent); err != nil {
		return Generation{}, err
	}
	// Commit point: after this atomic replacement, a normal pre-commit failure is
	// no longer possible because readers may already observe this generation.
	if err := atomicReplace(currentTempName, filepath.Join(paths.root, currentFileName)); err != nil {
		return Generation{}, fmt.Errorf("semanticpersist: replace CURRENT: %w", err)
	}
	// Ignore caller cancellation after commit and finish reporting durability.
	// Any post-commit failure is wrapped as ErrIndeterminate.
	if err := afterStep(options, StepReplaceCurrent, true); err != nil {
		return Generation{}, err
	}

	if options.Durability == DurabilitySynchronous {
		if err := beforeStep(context.Background(), options, StepSyncStore); err != nil {
			return Generation{}, fmt.Errorf("%w: %v", ErrIndeterminate, err)
		}
		if err := syncDirectory(paths.root); err != nil {
			return Generation{}, fmt.Errorf("%w: %v", ErrIndeterminate, err)
		}
		if err := afterStep(options, StepSyncStore, true); err != nil {
			return Generation{}, err
		}
	}
	return Generation{ID: generationID, ObjectID: objectID}, nil
}

func Open(root string, limits Limits) (*Loaded, error) {
	limits = normalizeLimits(limits)
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
	loaded, err := openCurrent(paths, limits)
	if err != nil {
		_ = storeLock.Close()
		return nil, err
	}
	loaded.storeLock = storeLock
	return loaded, nil
}

func openCurrent(paths storePaths, limits Limits) (*Loaded, error) {
	// CURRENT is authoritative. Normal open never scans generation directories or
	// promotes a newer orphan automatically.
	currentData, err := readRegularFile(filepath.Join(paths.root, currentFileName), limits.MaxFileBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrCurrentMissing
	}
	if err != nil {
		return nil, err
	}
	current, err := decodeCurrent(currentData, limits)
	if err != nil {
		return nil, err
	}
	return openGeneration(paths, current.GenerationID, current.ManifestHash, true, limits)
}

// RepairCurrent explicitly validates and selects generationID. Normal Open
// never scans for or promotes orphan generations.
func RepairCurrent(root string, generationID uint64, options Options) error {
	options.Limits = normalizeLimits(options.Limits)
	if err := validateLimits(options.Limits); err != nil {
		return err
	}
	if options.Durability == 0 {
		options.Durability = DurabilitySynchronous
	} else if options.Durability != DurabilitySynchronous && options.Durability != DurabilityAsynchronous {
		return ErrCorrupt
	}
	paths, err := validateStoreDirectories(root)
	if err != nil {
		return err
	}
	storeLock, err := acquireStoreLock(filepath.Join(paths.root, "LOCK"))
	if err != nil {
		return err
	}
	defer storeLock.Close()
	// Repair validates exactly the caller-selected generation; it does not guess
	// which orphan is newest or safest.
	loaded, manifestHash, err := openGenerationForRepair(paths, generationID, options.Limits)
	if err != nil {
		return err
	}
	if err := loaded.Close(); err != nil {
		return err
	}
	if options.Durability == DurabilitySynchronous {
		if err := syncPublishedGeneration(paths, generationID, loaded.Generation.ObjectID, loaded.Snapshot.Segment.Kind() == semantic.SegmentKindChunkHNSW); err != nil {
			return err
		}
	}
	data, _, err := encodeCurrent(currentRecord{GenerationID: generationID, ManifestHash: manifestHash}, options.Limits)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(paths.root, ".tmp-current-repair-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := writeAllFile(temp, data); err != nil {
		_ = temp.Close()
		return err
	}
	if options.Durability == DurabilitySynchronous {
		if err := temp.Sync(); err != nil {
			_ = temp.Close()
			return err
		}
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := atomicReplace(name, filepath.Join(paths.root, currentFileName)); err != nil {
		return err
	}
	if options.Durability == DurabilitySynchronous {
		if err := syncDirectory(paths.root); err != nil {
			return fmt.Errorf("%w: %v", ErrIndeterminate, err)
		}
	}
	return nil
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

func openGeneration(paths storePaths, generationID uint64, expectedManifestHash [sha256.Size]byte, checkHash bool, limits Limits) (*Loaded, error) {
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	if err := validateDirectory(generationPath); err != nil {
		return nil, err
	}
	manifestData, err := readRegularFile(filepath.Join(generationPath, manifestFileName), limits.MaxFileBytes)
	if err != nil {
		return nil, err
	}
	manifestHash := sha256.Sum256(manifestData)
	// This closes the first link in the hash chain: CURRENT identifies not only a
	// generation number, but the exact manifest bytes expected for that number.
	if checkHash && manifestHash != expectedManifestHash {
		return nil, ErrCorrupt
	}
	manifestValue, err := decodeManifest(manifestData, limits)
	if err != nil {
		if errors.Is(err, ErrUnsupportedVersion) {
			return nil, err
		}
		return nil, ErrCorrupt
	}
	if manifestValue.GenerationID != generationID {
		return nil, ErrCorrupt
	}
	if !validObjectID(manifestValue.ObjectID) {
		return nil, ErrInvalidObjectID
	}
	wantObjectID := segmentObjectID(manifestValue.SegmentKind, manifestValue.Vectors, manifestValue.Graph)
	if manifestValue.ObjectID != wantObjectID {
		return nil, ErrCorrupt
	}
	// Reject a generation whose declared files could exceed the total open
	// allocation budget before reading and decoding those files.
	if err := validateOpenReferences(manifestData, manifestValue, limits); err != nil {
		return nil, err
	}
	stateData, err := readReferencedFile(filepath.Join(generationPath, stateFileName), manifestValue.State, limits.MaxFileBytes)
	if err != nil {
		return nil, err
	}
	state, err := decodeState(stateData, limits)
	if err != nil {
		return nil, err
	}
	objectPath := filepath.Join(paths.segments, manifestValue.ObjectID)
	if err := ensureContained(paths.root, objectPath); err != nil {
		return nil, err
	}
	if err := validateDirectory(objectPath); err != nil {
		return nil, err
	}
	vectorFileLimit := min(limits.MaxFileBytes, limits.MaxVectorBytes+128)
	vectorsData, err := readReferencedFile(filepath.Join(objectPath, vectorsFileName), manifestValue.Vectors, vectorFileLimit)
	if err != nil {
		return nil, err
	}
	vectorReader, vectorMetadata, err := vectorflat.Open(bytes.NewReader(vectorsData), vectorflat.CodecLimits{
		MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxVectorBytes: limits.MaxVectorBytes, MaxK: limits.MaxK,
	})
	if err != nil {
		return nil, err
	}
	if vectorMetadata.Size != manifestValue.Vectors.Size || vectorMetadata.SHA256 != manifestValue.Vectors.SHA256 {
		return nil, ErrCorrupt
	}
	var segment *semantic.Segment
	switch manifestValue.SegmentKind {
	case semantic.SegmentKindChunkHNSW:
		graphLimit := min(limits.MaxFileBytes, limits.MaxGraphBytes)
		graphData, readErr := readReferencedFile(filepath.Join(objectPath, graphFileName), manifestValue.Graph, graphLimit)
		if readErr != nil {
			return nil, readErr
		}
		graphReader, graphMetadata, openErr := hnsw.OpenIndexReader(bytes.NewReader(graphData), vectorReader, hnsw.VectorFileReference{
			Size: manifestValue.Vectors.Size, SHA256: manifestValue.Vectors.SHA256,
		}, hnsw.GraphLimits{
			MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxVectorBytes: limits.MaxVectorBytes,
			MaxGraphBytes: graphLimit, MaxLinks: limits.MaxGraphLinks, MaxK: limits.MaxK,
			MaxEfSearch: limits.MaxEfSearch, MaxVisitLimit: limits.MaxVisitLimit,
		})
		if openErr != nil {
			return nil, openErr
		}
		if graphMetadata.Size != manifestValue.Graph.Size || graphMetadata.SHA256 != manifestValue.Graph.SHA256 {
			return nil, ErrCorrupt
		}
		segment, err = semantic.NewSegment(semantic.MutableHeadID, semantic.SegmentMetadata{Space: state.Space, Chunking: state.Chunking}, vectorReader, graphReader, state.Rows)
	default:
		return nil, ErrCorrupt
	}
	if err != nil {
		return nil, err
	}
	if segment.Len() != len(state.Rows) {
		return nil, ErrCorrupt
	}
	snapshot := semantic.Snapshot{
		Space: state.Space, Chunking: state.Chunking, MaxAllocatedVectorID: state.MaxAllocatedVectorID, Segment: segment,
		MaxK:               state.MaxK,
		MaxChunkCandidates: state.MaxChunkCandidates, MaxChunksPerDocumentHit: state.MaxChunksPerDocumentHit,
	}
	// Snapshot validation performs cross-file validation: vector rows, row metadata,
	// descriptors, and limits must agree.
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	return &Loaded{Generation: Generation{ID: generationID, ObjectID: manifestValue.ObjectID}, Snapshot: snapshot}, nil
}

func openGenerationForRepair(paths storePaths, generationID uint64, limits Limits) (*Loaded, [sha256.Size]byte, error) {
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	if err := validateDirectory(generationPath); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	manifestData, err := readRegularFile(filepath.Join(generationPath, manifestFileName), limits.MaxFileBytes)
	if err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	hash := sha256.Sum256(manifestData)
	loaded, err := openGeneration(paths, generationID, hash, true, limits)
	return loaded, hash, err
}

func writeVectorsFile(path string, source vector.PreparedVectorSource, maxK int, durability DurabilityMode) (fileReference, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fileReference{}, err
	}
	metadata, writeErr := vectorflat.WriteSource(file, source, maxK)
	if writeErr == nil && durability == DurabilitySynchronous {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fileReference{}, writeErr
	}
	if closeErr != nil {
		return fileReference{}, closeErr
	}
	return fileReference{Size: metadata.Size, SHA256: metadata.SHA256}, nil
}

func writeGraphFile(path string, reader *hnsw.Reader, vectors fileReference, durability DurabilityMode) (fileReference, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fileReference{}, err
	}
	metadata, writeErr := hnsw.WriteGraph(file, reader, hnsw.VectorFileReference{Size: vectors.Size, SHA256: vectors.SHA256})
	if writeErr == nil && durability == DurabilitySynchronous {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fileReference{}, writeErr
	}
	if closeErr != nil {
		return fileReference{}, closeErr
	}
	return fileReference{Size: metadata.Size, SHA256: metadata.SHA256}, nil
}

func validateSnapshotLimits(snapshot semantic.Snapshot, limits Limits) error {
	if snapshot.Segment == nil || snapshot.Segment.Vectors() == nil ||
		snapshot.Segment.Dimensions() > limits.MaxDimensions || snapshot.Segment.Len() > limits.MaxVectors ||
		snapshot.Segment.MaxK() > limits.MaxK || snapshot.Segment.Len() > limits.MaxVectors ||
		snapshot.MaxK > limits.MaxK || snapshot.MaxChunkCandidates > limits.MaxK {
		return ErrLimitExceeded
	}
	components, ok := checkedMultiply64(uint64(snapshot.Segment.Len()), uint64(snapshot.Segment.Dimensions()))
	if !ok {
		return ErrLimitExceeded
	}
	vectorBytes, ok := checkedMultiply64(components, 4)
	if !ok || vectorBytes > limits.MaxVectorBytes || limits.MaxFileBytes < 44 || vectorBytes > limits.MaxFileBytes-44 {
		return ErrLimitExceeded
	}
	if snapshot.Segment.Kind() == semantic.SegmentKindChunkHNSW {
		graph := snapshot.Segment.HNSW()
		if graph == nil {
			return ErrLimitExceeded
		}
		search := graph.SearchConfig()
		if search.MaxK > limits.MaxK || search.MaxEfSearch > limits.MaxEfSearch || search.MaxVisitLimit > limits.MaxVisitLimit ||
			uint64(graph.StorageStats().DirectedLinks) > limits.MaxGraphLinks ||
			graphFileSize(graph) > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
			return ErrLimitExceeded
		}
	}
	validString := func(value string) bool { return len(value) <= limits.MaxStringBytes }
	if !validString(snapshot.Space.ID) || !validString(snapshot.Space.ModelVersion) || !validString(snapshot.Space.Fingerprint) ||
		!validString(snapshot.Chunking.ID) || !validString(snapshot.Chunking.Fingerprint) {
		return ErrLimitExceeded
	}
	documentChunks := make(map[string]int)
	for _, record := range snapshot.Segment.Rows() {
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

func writeDataFile(path string, data []byte, durability DurabilityMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writeErr := writeAllFile(file, data)
	if writeErr == nil && durability == DurabilitySynchronous {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
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
	return readBounded(file, limit)
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
	data, err := readBounded(file, reference.Size)
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) != reference.Size || sha256.Sum256(data) != reference.SHA256 {
		return nil, ErrCorrupt
	}
	return data, nil
}

func verifyExistingObject(path string, kind semantic.SegmentKind, vectors, graph fileReference, limits Limits, durability DurabilityMode) error {
	if err := validateDirectory(path); err != nil {
		return err
	}
	vectorsPath := filepath.Join(path, vectorsFileName)
	graphPath := filepath.Join(path, graphFileName)
	if _, err := readReferencedFile(vectorsPath, vectors, limits.MaxVectorBytes+128); err != nil {
		return err
	}
	if kind == semantic.SegmentKindChunkHNSW {
		if _, err := readReferencedFile(graphPath, graph, min(limits.MaxFileBytes, limits.MaxGraphBytes)); err != nil {
			return err
		}
	}
	if durability == DurabilitySynchronous {
		// Upgrade an object left by an asynchronous publication before allowing a
		// synchronous generation to depend on it.
		files := []string{vectorsPath}
		if kind == semantic.SegmentKindChunkHNSW {
			files = append(files, graphPath)
		}
		for _, file := range files {
			if err := syncRegularFile(file); err != nil {
				return err
			}
		}
		return syncDirectory(path)
	}
	return nil
}

func syncPublishedGeneration(paths storePaths, generationID uint64, objectID string, hasGraph bool) error {
	// Repair may select an orphan produced asynchronously. Sync every referenced
	// file and directory before publishing a synchronous repaired CURRENT.
	objectPath := filepath.Join(paths.segments, objectID)
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	files := []string{
		filepath.Join(objectPath, vectorsFileName),
		filepath.Join(generationPath, stateFileName), filepath.Join(generationPath, manifestFileName),
	}
	if hasGraph {
		files = append(files, filepath.Join(objectPath, graphFileName))
	}
	for _, file := range files {
		if err := syncRegularFile(file); err != nil {
			return err
		}
	}
	for _, directory := range []string{objectPath, generationPath, paths.segments, paths.objects, paths.generations, paths.root} {
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}

func graphFileSize(reader *hnsw.Reader) uint64 {
	if reader == nil {
		return 0
	}
	stats := reader.StorageStats()
	padding := uint64((4 - stats.VectorRows%4) % 4)
	size, ok := checkedAdd64(164, stats.NodeMetadataBytes)
	if !ok {
		return ^uint64(0)
	}
	for _, part := range []uint64{padding, stats.OffsetBytes, stats.LinkBytes} {
		size, ok = checkedAdd64(size, part)
		if !ok {
			return ^uint64(0)
		}
	}
	return size
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
	loaded, err := openCurrent(paths, limits)
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
