package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

type objectStore struct {
	layout     layout
	limits     Limits
	durability durabilityPolicy
	hooks      Options
}

func newObjectStore(l layout, options Options) objectStore {
	return objectStore{layout: l, limits: options.Limits, durability: durabilityPolicy{mode: options.Durability}, hooks: options}
}

func (s objectStore) put(ctx context.Context, data semantic.SegmentData) (semanticformat.SegmentRef, error) {
	segmentTemp, err := os.MkdirTemp(s.layout.segments, ".tmp-seg-")
	if err != nil {
		return semanticformat.SegmentRef{}, fmt.Errorf("semanticpersist: create segment temp: %w", err)
	}
	owned := true
	defer func() {
		if owned {
			_ = os.RemoveAll(segmentTemp)
		}
	}()

	if err := beforeStep(ctx, s.hooks, stepWriteVectors); err != nil {
		return semanticformat.SegmentRef{}, err
	}
	vectorsRef, err := s.writeVectors(ctx, filepath.Join(segmentTemp, vectorsFileName), data.Vectors, data.Index.Report().Search.MaxK)
	if err != nil {
		return semanticformat.SegmentRef{}, err
	}
	if err := afterStep(s.hooks, stepWriteVectors, false); err != nil {
		return semanticformat.SegmentRef{}, err
	}

	if err := beforeStep(ctx, s.hooks, stepWriteGraph); err != nil {
		return semanticformat.SegmentRef{}, err
	}
	graphRef, err := s.writeGraph(ctx, filepath.Join(segmentTemp, graphFileName), data.Index, vectorsRef)
	if err != nil {
		return semanticformat.SegmentRef{}, err
	}
	if err := afterStep(s.hooks, stepWriteGraph, false); err != nil {
		return semanticformat.SegmentRef{}, err
	}

	if s.durability.synchronous() {
		if err := beforeStep(ctx, s.hooks, stepSyncSegment); err != nil {
			return semanticformat.SegmentRef{}, err
		}
		if err := s.durability.syncDirectory(segmentTemp); err != nil {
			return semanticformat.SegmentRef{}, err
		}
		if err := afterStep(s.hooks, stepSyncSegment, false); err != nil {
			return semanticformat.SegmentRef{}, err
		}
	}

	ref := semanticformat.SegmentRef{ObjectID: segmentObjectID(vectorsRef, graphRef), Vectors: vectorsRef, Graph: graphRef}
	storedPath := s.layout.segment(ref.ObjectID)
	if err := ensureContained(s.layout.root, storedPath); err != nil {
		return semanticformat.SegmentRef{}, err
	}
	if err := beforeStep(ctx, s.hooks, stepRenameSegment); err != nil {
		return semanticformat.SegmentRef{}, err
	}
	if _, err := os.Lstat(storedPath); err == nil {
		if err := s.verify(ref); err != nil {
			return semanticformat.SegmentRef{}, err
		}
		if s.durability.synchronous() {
			if err := s.durability.syncDirectory(s.layout.segments); err != nil {
				return semanticformat.SegmentRef{}, err
			}
		}
		if err := os.RemoveAll(segmentTemp); err != nil {
			return semanticformat.SegmentRef{}, err
		}
		owned = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return semanticformat.SegmentRef{}, err
	} else {
		if err := os.Rename(segmentTemp, storedPath); err != nil {
			return semanticformat.SegmentRef{}, fmt.Errorf("semanticpersist: install segment: %w", err)
		}
		owned = false
		if err := s.durability.syncDirectory(s.layout.segments); err != nil {
			return semanticformat.SegmentRef{}, err
		}
	}
	if err := afterStep(s.hooks, stepRenameSegment, false); err != nil {
		return semanticformat.SegmentRef{}, err
	}
	return ref, nil
}

func (s objectStore) writeVectors(ctx context.Context, path string, source vector.PreparedVectorStore, maxK int) (semanticformat.FileRef, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return semanticformat.FileRef{}, err
	}
	metadata, writeErr := semanticformat.WriteVectorFile(ctx, file, source, maxK, vectorFormatLimits(s.limits))
	if writeErr == nil {
		writeErr = s.durability.syncFile(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return semanticformat.FileRef{}, mapFormatError(writeErr)
	}
	if closeErr != nil {
		return semanticformat.FileRef{}, closeErr
	}
	if metadata.Size > s.limits.MaxFileBytes || metadata.Size > s.limits.MaxVectorBytes+128 {
		return semanticformat.FileRef{}, ErrLimitExceeded
	}
	return metadata.FileRef, nil
}

func (s objectStore) writeGraph(ctx context.Context, path string, index *hnsw.Index, vectors semanticformat.FileRef) (semanticformat.FileRef, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return semanticformat.FileRef{}, err
	}
	metadata, writeErr := hnsw.WriteGraph(ctx, file, index, hnsw.VectorFileReference{Size: vectors.Size, SHA256: vectors.SHA256})
	if writeErr == nil {
		writeErr = s.durability.syncFile(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return semanticformat.FileRef{}, writeErr
	}
	if closeErr != nil {
		return semanticformat.FileRef{}, closeErr
	}
	ref := semanticformat.FileRef{Size: metadata.Size, SHA256: metadata.SHA256}
	if ref.Size > min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes) {
		return semanticformat.FileRef{}, ErrLimitExceeded
	}
	return ref, nil
}

func (s objectStore) verify(ref semanticformat.SegmentRef) error {
	if !validObjectID(ref.ObjectID) || ref.ObjectID != segmentObjectID(ref.Vectors, ref.Graph) {
		return ErrCorrupt
	}
	path := s.layout.segment(ref.ObjectID)
	if err := ensureContained(s.layout.root, path); err != nil {
		return err
	}
	if err := validateDirectory(path); err != nil {
		return err
	}
	if err := verifyReferencedFile(filepath.Join(path, vectorsFileName), ref.Vectors, min(s.limits.MaxFileBytes, s.limits.MaxVectorBytes+128), s.durability.synchronous()); err != nil {
		return err
	}
	if err := verifyReferencedFile(filepath.Join(path, graphFileName), ref.Graph, min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes), s.durability.synchronous()); err != nil {
		return err
	}
	return s.durability.syncDirectory(path)
}

func (s objectStore) open(ctx context.Context, ref semanticformat.SegmentRef, config semantic.Config, state semanticformat.SegmentState) (semantic.SegmentData, error) {
	if !validObjectID(ref.ObjectID) || ref.ObjectID != segmentObjectID(ref.Vectors, ref.Graph) {
		return semantic.SegmentData{}, ErrCorrupt
	}
	path := s.layout.segment(ref.ObjectID)
	if err := ensureContained(s.layout.root, path); err != nil {
		return semantic.SegmentData{}, err
	}
	if err := validateDirectory(path); err != nil {
		return semantic.SegmentData{}, err
	}
	vectorsData, err := readReferencedFileWithoutHash(filepath.Join(path, vectorsFileName), ref.Vectors, min(s.limits.MaxFileBytes, s.limits.MaxVectorBytes+128))
	if err != nil {
		return semantic.SegmentData{}, err
	}
	decoded, metadata, err := semanticformat.DecodeVectorFile(vectorsData, vectorFormatLimits(s.limits))
	if err != nil {
		return semantic.SegmentData{}, mapFormatError(err)
	}
	if metadata.Size != ref.Vectors.Size || metadata.SHA256 != ref.Vectors.SHA256 {
		return semantic.SegmentData{}, ErrCorrupt
	}
	vectors, err := memorystore.NewPrepared(decoded.Calculator, decoded.Values)
	if err != nil {
		return semantic.SegmentData{}, ErrCorrupt
	}
	graphData, err := readReferencedFileWithoutHash(filepath.Join(path, graphFileName), ref.Graph, min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes))
	if err != nil {
		return semantic.SegmentData{}, err
	}
	index, graphMetadata, err := hnsw.OpenGraphBytes(ctx, graphData, vectors,
		hnsw.VectorFileReference{Size: ref.Vectors.Size, SHA256: ref.Vectors.SHA256},
		hnsw.GraphLimits{MaxDimensions: s.limits.MaxDimensions, MaxVectors: s.limits.MaxVectors, MaxVectorBytes: s.limits.MaxVectorBytes,
			MaxGraphBytes: min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes), MaxLinks: s.limits.MaxGraphLinks,
			MaxK: s.limits.MaxK, MaxEfSearch: s.limits.MaxEfSearch, MaxVisitLimit: s.limits.MaxVisitLimit})
	if err != nil {
		return semantic.SegmentData{}, err
	}
	if graphMetadata.Size != ref.Graph.Size || graphMetadata.SHA256 != ref.Graph.SHA256 {
		return semantic.SegmentData{}, ErrCorrupt
	}
	segment := semantic.SegmentData{ComponentID: state.ComponentID, Pipeline: semantic.PipelineDescriptor{Embedding: config.Embedding, Chunking: config.Chunking}, Rows: state.Rows, Vectors: vectors, Index: index}
	if err := validateSegmentData(segment, config, s.limits); err != nil {
		return semantic.SegmentData{}, err
	}
	return segment, nil
}
