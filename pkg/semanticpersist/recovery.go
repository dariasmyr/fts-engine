package semanticpersist

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func openCurrent(ctx context.Context, paths storePaths, limits Limits, expected semantic.PipelineDescriptor) (*semantic.Service, Generation, manifest, error) {
	current, manifestValue, err := readCurrentManifest(ctx, paths, limits)
	if err != nil {
		return nil, Generation{}, manifest{}, err
	}
	service, generation, manifestValue, err := openDecodedGeneration(ctx, paths, current.GenerationID, manifestValue, limits, expected)
	return service, generation, manifestValue, err
}

func readCurrentManifest(ctx context.Context, paths storePaths, limits Limits) (currentRecord, manifest, error) {
	if err := ctx.Err(); err != nil {
		return currentRecord{}, manifest{}, err
	}
	currentData, err := readRegularFile(filepath.Join(paths.root, currentFileName), limits.MaxFileBytes)
	if errors.Is(err, os.ErrNotExist) {
		return currentRecord{}, manifest{}, ErrCurrentMissing
	}
	if err != nil {
		return currentRecord{}, manifest{}, err
	}
	current, err := decodeCurrent(currentData, limits)
	if err != nil {
		return currentRecord{}, manifest{}, err
	}
	generationPath := filepath.Join(paths.generations, generationName(current.GenerationID))
	if err := validateDirectory(generationPath); err != nil {
		return currentRecord{}, manifest{}, err
	}
	manifestData, err := readRegularFile(filepath.Join(generationPath, manifestFileName), limits.MaxFileBytes)
	if err != nil {
		return currentRecord{}, manifest{}, err
	}
	if sha256.Sum256(manifestData) != current.ManifestHash {
		return currentRecord{}, manifest{}, ErrCorrupt
	}
	manifestValue, err := decodeManifest(manifestData, limits)
	if err != nil {
		return currentRecord{}, manifest{}, err
	}
	if manifestValue.GenerationID != current.GenerationID {
		return currentRecord{}, manifest{}, ErrCorrupt
	}
	if err := validateOpenReferences(manifestData, manifestValue, limits); err != nil {
		return currentRecord{}, manifest{}, err
	}
	return current, manifestValue, nil
}

// RepairCurrent explicitly validates and selects generationID. Normal Open
// never scans for or promotes orphan generations.
func RepairCurrent(ctx context.Context, root string, generationID uint64, options Options) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
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
	lock, err := acquireStoreLock(filepath.Join(paths.root, "LOCK"))
	if err != nil {
		return err
	}
	defer lock.Close()
	_, _, manifestValue, manifestHash, err := openGenerationForRepair(ctx, paths, generationID, options.Limits)
	if err != nil {
		return err
	}
	return repairCurrent(paths, generationID, manifestValue.Segments, manifestHash, options)
}

func openGeneration(ctx context.Context, paths storePaths, generationID uint64, expectedManifestHash [sha256.Size]byte, checkHash bool, limits Limits, expected semantic.PipelineDescriptor) (*semantic.Service, Generation, manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, Generation{}, manifest{}, err
	}
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	if err := validateDirectory(generationPath); err != nil {
		return nil, Generation{}, manifest{}, err
	}
	manifestData, err := readRegularFile(filepath.Join(generationPath, manifestFileName), limits.MaxFileBytes)
	if err != nil {
		return nil, Generation{}, manifest{}, err
	}
	manifestHash := sha256.Sum256(manifestData)
	if checkHash && manifestHash != expectedManifestHash {
		return nil, Generation{}, manifest{}, ErrCorrupt
	}
	manifestValue, err := decodeManifest(manifestData, limits)
	if err != nil {
		if errors.Is(err, ErrUnsupportedVersion) {
			return nil, Generation{}, manifest{}, err
		}
		return nil, Generation{}, manifest{}, ErrCorrupt
	}
	if manifestValue.GenerationID != generationID {
		return nil, Generation{}, manifest{}, ErrCorrupt
	}
	if err := validateOpenReferences(manifestData, manifestValue, limits); err != nil {
		return nil, Generation{}, manifest{}, err
	}
	return openDecodedGeneration(ctx, paths, generationID, manifestValue, limits, expected)
}

func openDecodedGeneration(ctx context.Context, paths storePaths, generationID uint64, manifestValue manifest, limits Limits, expected semantic.PipelineDescriptor) (*semantic.Service, Generation, manifest, error) {
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	stateData, err := readReferencedFile(filepath.Join(generationPath, stateFileName), manifestValue.State, limits.MaxFileBytes)
	if err != nil {
		return nil, Generation{}, manifest{}, err
	}
	state, err := decodeState(stateData, limits)
	if err != nil {
		return nil, Generation{}, manifest{}, err
	}
	if len(state.Segments) != len(manifestValue.Segments) {
		return nil, Generation{}, manifest{}, ErrCorrupt
	}
	if err := validateExpectedDescriptors(state.Config, expected); err != nil {
		return nil, Generation{}, manifest{}, err
	}

	stored := make([]semantic.StoredSegment, len(state.Segments))
	manifestValue.componentIDs = make([]uint64, len(state.Segments))
	for i, stateSegment := range state.Segments {
		manifestSegment := manifestValue.Segments[i]
		segment, err := openStoredSegment(ctx, paths, manifestSegment, state.Config, stateSegment, limits)
		if err != nil {
			return nil, Generation{}, manifest{}, err
		}
		stored[i] = semantic.StoredSegment{Data: segment, LivenessWords: stateSegment.LivenessWords}
		manifestValue.componentIDs[i] = stateSegment.ComponentID
	}
	service, err := semantic.Restore(ctx, semantic.RestoreState{
		Config: state.Config, Revision: state.Revision, MaxAllocatedVectorID: state.MaxAllocatedVectorID,
		NextComponentID: state.NextComponentID, Segments: stored,
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, Generation{}, manifest{}, ctx.Err()
		}
		return nil, Generation{}, manifest{}, ErrCorrupt
	}
	return service, Generation{ID: generationID}, manifestValue, nil
}

func openStoredSegment(ctx context.Context, paths storePaths, value manifestSegment, config semantic.Config, state decodedStateSegment, limits Limits) (semantic.SegmentData, error) {
	if !ValidObjectID(value.ObjectID) || value.ObjectID != segmentObjectID(value.Vectors, value.Graph) {
		return semantic.SegmentData{}, ErrCorrupt
	}
	storedSegmentPath := filepath.Join(paths.segments, value.ObjectID)
	if err := ensureContained(paths.root, storedSegmentPath); err != nil {
		return semantic.SegmentData{}, err
	}
	if err := validateDirectory(storedSegmentPath); err != nil {
		return semantic.SegmentData{}, err
	}
	vectorsData, err := readReferencedFileWithoutHash(filepath.Join(storedSegmentPath, vectorsFileName), value.Vectors, min(limits.MaxFileBytes, limits.MaxVectorBytes+128))
	if err != nil {
		return semantic.SegmentData{}, err
	}
	prepared, metadata, err := openBytes(vectorsData, codecLimitsConfig{MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxVectorBytes: limits.MaxVectorBytes, MaxK: limits.MaxK})

	if err != nil || metadata.Size != value.Vectors.Size || metadata.SHA256 != value.Vectors.SHA256 {
		return semantic.SegmentData{}, ErrCorrupt
	}

	vectors, err := memorystore.NewPrepared(
		prepared.Calculator,
		prepared.Values,
	)
	if err != nil {
		return semantic.SegmentData{}, ErrCorrupt
	}

	graphData, err := readReferencedFileWithoutHash(filepath.Join(storedSegmentPath, graphFileName), value.Graph, min(limits.MaxFileBytes, limits.MaxGraphBytes))
	if err != nil {
		return semantic.SegmentData{}, err
	}

	index, graphMetadata, err := hnsw.OpenGraphBytes(
		ctx,
		graphData,
		vectors,
		hnsw.VectorFileReference{
			Size:   value.Vectors.Size,
			SHA256: value.Vectors.SHA256,
		},
		hnsw.GraphLimits{
			MaxDimensions:  limits.MaxDimensions,
			MaxVectors:     limits.MaxVectors,
			MaxVectorBytes: limits.MaxVectorBytes,
			MaxGraphBytes:  min(limits.MaxFileBytes, limits.MaxGraphBytes),
			MaxLinks:       limits.MaxGraphLinks,
			MaxK:           limits.MaxK,
			MaxEfSearch:    limits.MaxEfSearch,
			MaxVisitLimit:  limits.MaxVisitLimit,
		})
	if err != nil {
		return semantic.SegmentData{}, err
	}
	if graphMetadata.Size != value.Graph.Size || graphMetadata.SHA256 != value.Graph.SHA256 {
		return semantic.SegmentData{}, ErrCorrupt
	}
	segment := semantic.SegmentData{
		ComponentID: state.ComponentID,
		Pipeline:    semantic.PipelineDescriptor{Embedding: config.Embedding, Chunking: config.Chunking},
		Rows:        state.Rows,
		Vectors:     vectors,
		Index:       index,
	}
	if err := validateSegmentData(segment, config, limits); err != nil {
		return semantic.SegmentData{}, err
	}
	return segment, nil
}

func openGenerationForRepair(ctx context.Context, paths storePaths, generationID uint64, limits Limits) (*semantic.Service, Generation, manifest, [sha256.Size]byte, error) {
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	manifestData, err := readRegularFile(filepath.Join(generationPath, manifestFileName), limits.MaxFileBytes)
	if err != nil {
		return nil, Generation{}, manifest{}, [sha256.Size]byte{}, err
	}
	hash := sha256.Sum256(manifestData)
	service, generation, manifestValue, err := openGeneration(ctx, paths, generationID, hash, true, limits, semantic.PipelineDescriptor{})
	return service, generation, manifestValue, hash, err
}

func validateExpectedDescriptors(config semantic.Config, expected semantic.PipelineDescriptor) error {
	if expected == (semantic.PipelineDescriptor{}) {
		return nil
	}
	if config.Embedding != expected.Embedding {
		return ErrEmbeddingMismatch
	}
	if config.Chunking != expected.Chunking {
		return ErrChunkingMismatch
	}
	return nil
}
