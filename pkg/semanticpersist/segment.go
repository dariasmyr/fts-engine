package semanticpersist

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

// LoadedSegment is a read-only segment opened without a mutable semantic
// service or a generation CURRENT pointer.
type LoadedSegment struct {
	Snapshot semantic.Snapshot
}

// SaveSegment writes one immutable HNSW segment atomically. It does not create
// a generation, acquire a store lock, or update CURRENT.
func SaveSegment(ctx context.Context, paths SegmentPaths, snapshot semantic.Snapshot, options Options) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if paths.Dir == "" {
		return ErrCorrupt
	}
	options.Limits = normalizeLimits(options.Limits)
	if options.Durability == 0 {
		options.Durability = DurabilitySynchronous
	}
	if err := validateLimits(options.Limits); err != nil {
		return err
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	if snapshot.Segment.Kind() != semantic.SegmentKindChunkHNSW || snapshot.Segment.HNSW() == nil {
		return ErrCorrupt
	}
	if err := validateSnapshotLimits(snapshot, options.Limits); err != nil {
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

	vectorsRef, err := writeVectorsFile(filepath.Join(temp, segmentVectorsFile), snapshot.Segment.Vectors(), snapshot.Segment.MaxK(), options.Durability)
	if err != nil {
		return err
	}
	graphRef, err := writeGraphFile(filepath.Join(temp, segmentGraphFile), snapshot.Segment.HNSW(), vectorsRef, options.Durability)
	if err != nil {
		return err
	}
	stateData, stateRef, err := encodeState(snapshot, options.Limits)
	if err != nil {
		return err
	}
	if err := writeDataFile(filepath.Join(temp, segmentStateFile), stateData, options.Durability); err != nil {
		return err
	}
	manifestValue := manifest{
		Version: manifestVersion, GenerationID: 1,
		ObjectID:    segmentObjectID(snapshot.Segment.Kind(), vectorsRef, graphRef),
		SegmentKind: semantic.SegmentKindChunkHNSW, Vectors: vectorsRef, Graph: graphRef, State: stateRef,
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

// OpenSegment opens one standalone HNSW segment. The returned snapshot owns
// fully loaded readers and does not hold a filesystem lock.
func OpenSegment(paths SegmentPaths, limits Limits) (*LoadedSegment, error) {
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
	graphReader, graphMetadata, err := hnsw.OpenGraph(bytes.NewReader(graphData), vectorReader, hnsw.VectorFileReference{Size: value.Vectors.Size, SHA256: value.Vectors.SHA256}, hnsw.GraphLimits{
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
	segment, err := semantic.NewHNSWSegment(semantic.MutableHeadID, vectorReader, graphReader, state.Rows)
	if err != nil {
		return nil, err
	}
	snapshot := semantic.Snapshot{
		Space: state.Space, Chunking: state.Chunking, MaxAllocatedVectorID: state.MaxAllocatedVectorID,
		Segment: segment, Rows: state.Rows, MaxK: state.MaxK,
		MaxChunkCandidates: state.MaxChunkCandidates, MaxChunksPerDocumentHit: state.MaxChunksPerDocumentHit,
	}
	if err := snapshot.Validate(); err != nil {
		return nil, fmt.Errorf("semanticpersist: validate standalone segment: %w", err)
	}
	return &LoadedSegment{Snapshot: snapshot}, nil
}
