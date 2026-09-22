package semanticpersist

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	vectorflat "github.com/dariasmyr/fts-engine/pkg/vector/flat"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

const (
	segmentVectorsFile = "vectors.bin"
	segmentGraphFile   = "graph.bin"
	segmentStateFile   = "state.bin"
	segmentManifest    = "manifest.bin"
)

// SegmentPaths identifies one standalone immutable semantic segment directory.
type SegmentPaths struct {
	Dir string
}

// SaveSealedSegment writes one immutable ANN component without creating a
// generation or changing CURRENT.
func SaveSealedSegment(ctx context.Context, paths SegmentPaths, sealed SealedSegment, options Options) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if paths.Dir == "" || sealed.Segment == nil {
		return ErrCorrupt
	}
	options.Limits = normalizeLimits(options.Limits)
	if options.Durability == 0 {
		options.Durability = DurabilitySynchronous
	}
	if err := validateLimits(options.Limits); err != nil {
		return err
	}
	if err := validateSealedSegment(sealed, options.Limits); err != nil {
		return err
	}

	parent := filepath.Dir(paths.Dir)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	temp, err := os.MkdirTemp(parent, ".tmp-segment-")
	if err != nil {
		return err
	}
	owned := true
	defer func() {
		if owned {
			_ = os.RemoveAll(temp)
		}
	}()

	vectorsRef, err := writeVectorsFile(filepath.Join(temp, segmentVectorsFile), sealed.Segment.Vectors(), sealed.Segment.MaxK(), options.Durability)
	if err != nil {
		return err
	}
	graphRef, err := writeGraphFile(filepath.Join(temp, segmentGraphFile), sealed.Segment.Searcher(), vectorsRef, options.Durability)
	if err != nil {
		return err
	}
	stateData, stateRef, err := encodeState(sealed, options.Limits)
	if err != nil {
		return err
	}
	if err := writeDataFile(filepath.Join(temp, segmentStateFile), stateData, options.Durability); err != nil {
		return err
	}
	manifestValue := manifest{
		Version: manifestVersion, GenerationID: 1,
		ObjectID:    segmentObjectID(sealed.Segment.Kind(), vectorsRef, graphRef),
		SegmentKind: sealed.Segment.Kind(), Vectors: vectorsRef, Graph: graphRef, State: stateRef,
	}
	manifestData, _, err := encodeManifest(manifestValue, options.Limits)
	if err != nil {
		return err
	}
	if err := writeDataFile(filepath.Join(temp, segmentManifest), manifestData, options.Durability); err != nil {
		return err
	}
	if options.Durability == DurabilitySynchronous {
		if err := syncDirectory(temp); err != nil {
			return err
		}
	}
	if _, err := os.Lstat(paths.Dir); err == nil {
		return ErrGenerationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(temp, paths.Dir); err != nil {
		return err
	}
	owned = false
	if options.Durability == DurabilitySynchronous {
		return syncDirectory(parent)
	}
	return nil
}

// SaveSegment is a short name alias for callers that already use the segment
// persistence API. It accepts only a sealed segment.
func SaveSegment(ctx context.Context, paths SegmentPaths, sealed SealedSegment, options Options) error {
	return SaveSealedSegment(ctx, paths, sealed, options)
}

// OpenSealedSegment opens one immutable ANN component without creating a
// mutable service or acquiring a generation lock. The returned payload is
// independent of a mutable service or generation publication.
func OpenSealedSegment(paths SegmentPaths, limits Limits) (*LoadedSealedSegment, error) {
	if paths.Dir == "" {
		return nil, ErrCorrupt
	}
	limits = normalizeLimits(limits)
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	if err := validateDirectory(paths.Dir); err != nil {
		return nil, err
	}
	manifestData, err := readRegularFile(filepath.Join(paths.Dir, segmentManifest), limits.MaxFileBytes)
	if err != nil {
		return nil, err
	}
	value, err := decodeManifest(manifestData, limits)
	if err != nil {
		return nil, err
	}
	if value.GenerationID != 1 || value.SegmentKind != semantic.SegmentKindChunkHNSW || value.ObjectID != segmentObjectID(value.SegmentKind, value.Vectors, value.Graph) {
		return nil, ErrCorrupt
	}
	stateData, err := readReferencedFile(filepath.Join(paths.Dir, segmentStateFile), value.State, limits.MaxFileBytes)
	if err != nil {
		return nil, err
	}
	state, err := decodeState(stateData, limits)
	if err != nil {
		return nil, err
	}
	vectorData, err := readReferencedFile(filepath.Join(paths.Dir, segmentVectorsFile), value.Vectors, min(limits.MaxFileBytes, limits.MaxVectorBytes+128))
	if err != nil {
		return nil, err
	}
	vectorReader, vectorMetadata, err := vectorflat.Open(bytes.NewReader(vectorData), vectorflat.CodecLimits{
		MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxVectorBytes: limits.MaxVectorBytes, MaxK: limits.MaxK,
	})
	if err != nil {
		return nil, err
	}
	if vectorMetadata.Size != value.Vectors.Size || vectorMetadata.SHA256 != value.Vectors.SHA256 {
		return nil, ErrCorrupt
	}
	graphData, err := readReferencedFile(filepath.Join(paths.Dir, segmentGraphFile), value.Graph, min(limits.MaxFileBytes, limits.MaxGraphBytes))
	if err != nil {
		return nil, err
	}
	searcher, graphMetadata, err := hnsw.OpenSearcher(bytes.NewReader(graphData), vectorReader.VectorSource(), hnsw.VectorFileReference{Size: value.Vectors.Size, SHA256: value.Vectors.SHA256}, hnsw.GraphLimits{
		MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxVectorBytes: limits.MaxVectorBytes,
		MaxGraphBytes: min(limits.MaxFileBytes, limits.MaxGraphBytes), MaxLinks: limits.MaxGraphLinks,
		MaxK: limits.MaxK, MaxEfSearch: limits.MaxEfSearch, MaxVisitLimit: limits.MaxVisitLimit,
	})
	if err != nil {
		return nil, err
	}
	if graphMetadata.Size != value.Graph.Size || graphMetadata.SHA256 != value.Graph.SHA256 {
		return nil, ErrCorrupt
	}
	segment, err := semantic.NewSegment(state.ComponentID, semantic.SegmentMetadata{Space: state.Space, Chunking: state.Chunking}, searcher, state.Rows)
	if err != nil {
		return nil, err
	}
	sealed := SealedSegment{
		Segment: segment, Space: state.Space, Chunking: state.Chunking, MaxAllocatedVectorID: state.MaxAllocatedVectorID,
		MaxK: state.MaxK, MaxChunkCandidates: state.MaxChunkCandidates,
		MaxChunksPerDocumentHit: state.MaxChunksPerDocumentHit,
	}
	if err := validateSealedSegment(sealed, limits); err != nil {
		return nil, err
	}
	return &LoadedSealedSegment{Sealed: sealed}, nil
}

// OpenSegment is the name-level alias for OpenSealedSegment.
func OpenSegment(paths SegmentPaths, limits Limits) (*LoadedSealedSegment, error) {
	return OpenSealedSegment(paths, limits)
}
