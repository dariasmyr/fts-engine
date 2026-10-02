package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

type segmentObject struct {
	ID      string
	Vectors fileReference
	Graph   fileReference
}

func writeSegmentObject(ctx context.Context, paths storePaths, segment semantic.SegmentSnapshot, options Options) (segmentObject, error) {
	segmentTemp, err := os.MkdirTemp(paths.segments, ".tmp-seg-")
	if err != nil {
		return segmentObject{}, fmt.Errorf("semanticpersist: create segment temp: %w", err)
	}
	owned := true
	defer func() {
		if owned {
			_ = os.RemoveAll(segmentTemp)
		}
	}()

	if err := beforeStep(ctx, options, stepWriteVectors); err != nil {
		return segmentObject{}, err
	}
	vectorsRef, err := writeVectorsFile(ctx, filepath.Join(segmentTemp, vectorsFileName), segment.Vectors(), segment.MaxK(), options.Durability)
	if err != nil {
		return segmentObject{}, err
	}
	if err := afterStep(options, stepWriteVectors, false); err != nil {
		return segmentObject{}, err
	}
	if vectorsRef.Size > options.Limits.MaxFileBytes || vectorsRef.Size > options.Limits.MaxVectorBytes+128 {
		return segmentObject{}, ErrLimitExceeded
	}

	if err := beforeStep(ctx, options, stepWriteGraph); err != nil {
		return segmentObject{}, err
	}
	graphRef, err := writeGraphFile(ctx, filepath.Join(segmentTemp, graphFileName), segment.Index(), vectorsRef, options.Durability)
	if err != nil {
		return segmentObject{}, err
	}
	if err := afterStep(options, stepWriteGraph, false); err != nil {
		return segmentObject{}, err
	}
	if graphRef.Size > min(options.Limits.MaxFileBytes, options.Limits.MaxGraphBytes) {
		return segmentObject{}, ErrLimitExceeded
	}
	if options.Durability == DurabilitySynchronous {
		if err := beforeStep(ctx, options, stepSyncSegment); err != nil {
			return segmentObject{}, err
		}
		if err := syncDirectory(segmentTemp); err != nil {
			return segmentObject{}, err
		}
		if err := afterStep(options, stepSyncSegment, false); err != nil {
			return segmentObject{}, err
		}
	}

	objectID := segmentObjectID(vectorsRef, graphRef)
	objectPath := filepath.Join(paths.segments, objectID)
	if err := beforeStep(ctx, options, stepRenameSegment); err != nil {
		return segmentObject{}, err
	}
	if _, err := os.Lstat(objectPath); err == nil {
		if err := verifyExistingObject(objectPath, vectorsRef, graphRef, options.Limits, options.Durability); err != nil {
			return segmentObject{}, err
		}
		if options.Durability == DurabilitySynchronous {
			if err := syncDirectory(paths.segments); err != nil {
				return segmentObject{}, err
			}
		}
		if err := os.RemoveAll(segmentTemp); err != nil {
			return segmentObject{}, err
		}
		owned = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return segmentObject{}, err
	} else {
		if err := os.Rename(segmentTemp, objectPath); err != nil {
			return segmentObject{}, fmt.Errorf("semanticpersist: install segment: %w", err)
		}
		owned = false
		if options.Durability == DurabilitySynchronous {
			if err := syncDirectory(paths.segments); err != nil {
				return segmentObject{}, err
			}
		}
	}
	if err := afterStep(options, stepRenameSegment, false); err != nil {
		return segmentObject{}, err
	}
	return segmentObject{ID: objectID, Vectors: vectorsRef, Graph: graphRef}, nil
}

func writeVectorsFile(ctx context.Context, path string, source vectorstore.PreparedVectorStore, maxK int, durability DurabilityMode) (fileReference, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fileReference{}, err
	}
	metadata, writeErr := writeVectorFile(ctx, file, source, maxK)
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

func writeGraphFile(ctx context.Context, path string, reader *hnsw.Index, vectors fileReference, durability DurabilityMode) (fileReference, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fileReference{}, err
	}
	metadata, writeErr := hnsw.WriteGraph(ctx, file, reader, hnsw.VectorFileReference{Size: vectors.Size, SHA256: vectors.SHA256})
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

func verifyExistingObject(path string, vectors, graph fileReference, limits Limits, durability DurabilityMode) error {
	if err := validateDirectory(path); err != nil {
		return err
	}
	vectorsPath := filepath.Join(path, vectorsFileName)
	graphPath := filepath.Join(path, graphFileName)
	if _, err := readReferencedFile(vectorsPath, vectors, limits.MaxVectorBytes+128); err != nil {
		return err
	}
	if _, err := readReferencedFile(graphPath, graph, min(limits.MaxFileBytes, limits.MaxGraphBytes)); err != nil {
		return err
	}
	if durability == DurabilitySynchronous {
		files := []string{vectorsPath, graphPath}
		for _, file := range files {
			if err := syncRegularFile(file); err != nil {
				return err
			}
		}
		return syncDirectory(path)
	}
	return nil
}

func graphFileSize(index *hnsw.Index) uint64 {
	if index == nil {
		return 0
	}
	stats := index.Report().Storage
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
