package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/hnswformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

type objectStore struct {
	layout     layout
	limits     Limits
	durability durabilityPolicy
	hooks      PublishOptions
}

func newObjectStore(l layout, options PublishOptions) objectStore {
	return objectStore{layout: l, limits: options.Limits, durability: durabilityPolicy{mode: options.Durability}, hooks: options}
}

func (s objectStore) put(ctx context.Context, data semantic.CheckpointSegment, maxK int) (generationformat.SegmentRef, error) {
	segmentTemp, err := os.MkdirTemp(s.layout.segments, ".tmp-seg-")
	if err != nil {
		return generationformat.SegmentRef{}, fmt.Errorf("semanticpersist: create segment temp: %w", err)
	}
	owned := true
	defer func() {
		if owned {
			_ = os.RemoveAll(segmentTemp)
		}
	}()

	if err := beforeStep(ctx, s.hooks, stepWriteVectors); err != nil {
		return generationformat.SegmentRef{}, err
	}
	vectorsRef, err := s.writeVectors(ctx, filepath.Join(segmentTemp, vectorsFileName), data.Vectors, maxK)
	if err != nil {
		return generationformat.SegmentRef{}, err
	}
	if err := afterStep(s.hooks, stepWriteVectors, false); err != nil {
		return generationformat.SegmentRef{}, err
	}

	if err := beforeStep(ctx, s.hooks, stepWriteGraph); err != nil {
		return generationformat.SegmentRef{}, err
	}
	index, ok := data.Index.(*hnsw.Index)
	if !ok || index == nil {
		return generationformat.SegmentRef{}, ErrCorrupt
	}
	graphRef, err := s.writeGraph(ctx, filepath.Join(segmentTemp, graphFileName), index, vectorsRef)
	if err != nil {
		return generationformat.SegmentRef{}, err
	}
	if err := afterStep(s.hooks, stepWriteGraph, false); err != nil {
		return generationformat.SegmentRef{}, err
	}

	if s.durability.synchronous() {
		if err := beforeStep(ctx, s.hooks, stepSyncSegment); err != nil {
			return generationformat.SegmentRef{}, err
		}
		if err := s.durability.syncDirectory(segmentTemp); err != nil {
			return generationformat.SegmentRef{}, err
		}
		if err := afterStep(s.hooks, stepSyncSegment, false); err != nil {
			return generationformat.SegmentRef{}, err
		}
	}

	ref := generationformat.SegmentRef{ObjectID: segmentObjectID(vectorsRef, graphRef), Vectors: vectorsRef, Graph: graphRef}
	storedPath := s.layout.segment(ref.ObjectID)
	if err := ensureContained(s.layout.root, storedPath); err != nil {
		return generationformat.SegmentRef{}, err
	}
	if err := beforeStep(ctx, s.hooks, stepRenameSegment); err != nil {
		return generationformat.SegmentRef{}, err
	}
	if _, err := os.Lstat(storedPath); err == nil {
		if err := s.verify(ref); err != nil {
			return generationformat.SegmentRef{}, err
		}
		if s.durability.synchronous() {
			if err := s.durability.syncDirectory(s.layout.segments); err != nil {
				return generationformat.SegmentRef{}, err
			}
		}
		if err := os.RemoveAll(segmentTemp); err != nil {
			return generationformat.SegmentRef{}, err
		}
		owned = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return generationformat.SegmentRef{}, err
	} else {
		if err := os.Rename(segmentTemp, storedPath); err != nil {
			return generationformat.SegmentRef{}, fmt.Errorf("semanticpersist: install segment: %w", err)
		}
		owned = false
		if err := s.durability.syncDirectory(s.layout.segments); err != nil {
			return generationformat.SegmentRef{}, err
		}
	}
	if err := afterStep(s.hooks, stepRenameSegment, false); err != nil {
		return generationformat.SegmentRef{}, err
	}
	return ref, nil
}

func (s objectStore) writeVectors(ctx context.Context, path string, source vector.PreparedVectorStore, maxK int) (generationformat.FileRef, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return generationformat.FileRef{}, err
	}
	metadata, writeErr := hnswformat.WriteVectorFile(ctx, file, source, maxK, vectorFormatLimits(s.limits))
	if writeErr == nil {
		writeErr = s.durability.syncFile(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return generationformat.FileRef{}, mapFormatError(writeErr)
	}
	if closeErr != nil {
		return generationformat.FileRef{}, closeErr
	}
	if metadata.Size > s.limits.MaxFileBytes || metadata.Size > s.limits.MaxVectorBytes+128 {
		return generationformat.FileRef{}, ErrLimitExceeded
	}
	return generationFileRef(metadata.FileRef), nil
}

func (s objectStore) writeGraph(ctx context.Context, path string, index *hnsw.Index, vectors generationformat.FileRef) (generationformat.FileRef, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return generationformat.FileRef{}, err
	}
	metadata, writeErr := hnswformat.WriteGraphFile(ctx, file, index, hnswFileRef(vectors))
	if writeErr == nil {
		writeErr = s.durability.syncFile(file)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return generationformat.FileRef{}, mapFormatError(writeErr)
	}
	if closeErr != nil {
		return generationformat.FileRef{}, closeErr
	}
	ref := generationformat.FileRef{Size: metadata.Size, SHA256: metadata.SHA256}
	if ref.Size > min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes) {
		return generationformat.FileRef{}, ErrLimitExceeded
	}
	return ref, nil
}

func (s objectStore) verify(ref generationformat.SegmentRef) error {
	if !validObjectID(ref.ObjectID) || ref.ObjectID != segmentObjectID(ref.Vectors, ref.Graph) {
		return ErrCorrupt
	}
	path := s.layout.segment(ref.ObjectID)
	if err := ensureContained(s.layout.root, path); err != nil {
		return err
	}
	if err := validateDirectory(path); err != nil {
		return corruptMissingReference(err)
	}
	if err := verifyReferencedFile(filepath.Join(path, vectorsFileName), ref.Vectors, min(s.limits.MaxFileBytes, s.limits.MaxVectorBytes+128), s.durability.synchronous()); err != nil {
		return err
	}
	if err := verifyReferencedFile(filepath.Join(path, graphFileName), ref.Graph, min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes), s.durability.synchronous()); err != nil {
		return err
	}
	return s.durability.syncDirectory(path)
}

func (s objectStore) open(ctx context.Context, ref generationformat.SegmentRef, config semantic.Config, state generationformat.SegmentState) (semantic.CheckpointSegment, error) {
	if !validObjectID(ref.ObjectID) || ref.ObjectID != segmentObjectID(ref.Vectors, ref.Graph) {
		return semantic.CheckpointSegment{}, ErrCorrupt
	}
	path := s.layout.segment(ref.ObjectID)
	if err := ensureContained(s.layout.root, path); err != nil {
		return semantic.CheckpointSegment{}, err
	}
	if err := validateDirectory(path); err != nil {
		return semantic.CheckpointSegment{}, corruptMissingReference(err)
	}
	vectorsData, err := readReferencedFileWithoutHash(filepath.Join(path, vectorsFileName), ref.Vectors, min(s.limits.MaxFileBytes, s.limits.MaxVectorBytes+128))
	if err != nil {
		return semantic.CheckpointSegment{}, err
	}
	decoded, metadata, err := hnswformat.DecodeVectorFile(ctx, vectorsData, vectorFormatLimits(s.limits))
	if err != nil {
		return semantic.CheckpointSegment{}, mapFormatError(err)
	}
	if metadata.Size != ref.Vectors.Size || metadata.SHA256 != ref.Vectors.SHA256 {
		return semantic.CheckpointSegment{}, ErrCorrupt
	}
	vectors, err := memorystore.NewPrepared(decoded.Calculator, decoded.Values)
	if err != nil {
		return semantic.CheckpointSegment{}, ErrCorrupt
	}
	graphData, err := readReferencedFileWithoutHash(filepath.Join(path, graphFileName), ref.Graph, min(s.limits.MaxFileBytes, s.limits.MaxGraphBytes))
	if err != nil {
		return semantic.CheckpointSegment{}, err
	}
	index, graphMetadata, err := hnswformat.OpenGraphFile(
		ctx,
		graphData,
		vectors,
		hnswFileRef(ref.Vectors),
		hnsw.SearchConfig{
			EfSearch:   config.HNSW.DefaultEfSearch,
			VisitLimit: config.HNSW.DefaultVisitLimit,
		},
		hnswSegmentLimits(s.limits),
	)
	if err != nil {
		return semantic.CheckpointSegment{}, mapFormatError(err)
	}
	if graphMetadata.Size != ref.Graph.Size || graphMetadata.SHA256 != ref.Graph.SHA256 {
		return semantic.CheckpointSegment{}, ErrCorrupt
	}
	segment := semantic.CheckpointSegment{
		ID:      state.ID,
		Schema:  config.Schema,
		Rows:    state.Rows,
		Vectors: vectors,
		Index:   index,
	}
	if err := validateSegmentData(segment, config, s.limits); err != nil {
		return semantic.CheckpointSegment{}, err
	}
	return segment, nil
}

func hnswFileRef(ref generationformat.FileRef) hnswformat.FileRef {
	return hnswformat.FileRef{Size: ref.Size, SHA256: ref.SHA256}
}

func generationFileRef(ref hnswformat.FileRef) generationformat.FileRef {
	return generationformat.FileRef{Size: ref.Size, SHA256: ref.SHA256}
}
