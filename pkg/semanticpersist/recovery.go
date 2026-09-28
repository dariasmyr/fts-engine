package semanticpersist

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func openCurrent(paths storePaths, limits Limits, expected semantic.PipelineDescriptor) (*Loaded, error) {
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
	return openGeneration(paths, current.GenerationID, current.ManifestHash, true, limits, expected)
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
	return repairCurrent(paths, generationID, loaded.Generation.ObjectID,
		loaded.Sealed.Segment.Kind() == semantic.SegmentKindChunkHNSW, manifestHash, options)
}

func openGeneration(paths storePaths, generationID uint64, expectedManifestHash [sha256.Size]byte, checkHash bool, limits Limits, expected semantic.PipelineDescriptor) (*Loaded, error) {
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	if err := validateDirectory(generationPath); err != nil {
		return nil, err
	}
	manifestData, err := readRegularFile(filepath.Join(generationPath, manifestFileName), limits.MaxFileBytes)
	if err != nil {
		return nil, err
	}
	manifestHash := sha256.Sum256(manifestData)
	// This closes the first link in the hash chain: CURRENT identifies not only a generation number, but the exact manifest bytes expected for that number.
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
	// Reject a generation whose declared files could exceed the total open allocation budget before reading and decoding those files.
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
	vectorReader, vectorMetadata, err := OpenVectorFile(bytes.NewReader(vectorsData), CodecLimits{
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
		index, graphMetadata, openErr := hnsw.OpenGraphFile(bytes.NewReader(graphData), vectorReader, hnsw.VectorFileReference{
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
		segment, err = semantic.NewSegment(state.ComponentID, semantic.SegmentMetadata{Embedding: state.Embedding, Chunking: state.Chunking}, index, state.Rows)
	default:
		return nil, ErrCorrupt
	}
	if err != nil {
		return nil, err
	}
	if segment.Len() != len(state.Rows) {
		return nil, ErrCorrupt
	}
	sealed := SealedSegment{
		Segment: segment, MaxAllocatedVectorID: state.MaxAllocatedVectorID,
		MaxK: state.MaxK, MaxChunkCandidates: state.MaxChunkCandidates,
		MaxChunksPerDocumentHit: state.MaxChunksPerDocumentHit,
	}
	if err := validateSealedSegment(sealed, limits); err != nil {
		return nil, err
	}
	if err := validateExpectedDescriptors(sealed, expected); err != nil {
		return nil, err
	}
	return &Loaded{
		Generation: Generation{ID: generationID, ObjectID: manifestValue.ObjectID},
		Sealed:     sealed,
	}, nil
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
	loaded, err := openGeneration(paths, generationID, hash, true, limits, semantic.PipelineDescriptor{})
	return loaded, hash, err
}
