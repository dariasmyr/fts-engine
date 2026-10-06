package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/arithmetic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func writeStoredSegment(
	ctx context.Context,
	paths storePaths,
	vectors vector.PreparedVectorStore,
	index *hnsw.Index,
	options Options,
) (manifestSegment, error) {
	// Build the segment in a temporary directory first so partially written
	// files are never exposed as a committed segment.
	segmentTemp, err := os.MkdirTemp(paths.segments, ".tmp-seg-")
	if err != nil {
		return manifestSegment{}, fmt.Errorf("semanticpersist: create segment temp: %w", err)
	}

	owned := true
	defer func() {
		if owned {
			_ = os.RemoveAll(segmentTemp)
		}
	}()

	// Persist the prepared vector matrix and capture its content reference.
	if err := beforeStep(ctx, options, stepWriteVectors); err != nil {
		return manifestSegment{}, err
	}

	vectorsRef, err := writeVectorsFile(
		ctx,
		filepath.Join(segmentTemp, vectorsFileName),
		vectors,
		index.Report().Search.MaxK,
		options.Durability,
	)
	if err != nil {
		return manifestSegment{}, err
	}

	if err := afterStep(options, stepWriteVectors, false); err != nil {
		return manifestSegment{}, err
	}

	if vectorsRef.Size > options.Limits.MaxFileBytes ||
		vectorsRef.Size > options.Limits.MaxVectorBytes+128 {
		return manifestSegment{}, ErrLimitExceeded
	}

	// Persist the HNSW graph and bind it to the vector file reference.
	if err := beforeStep(ctx, options, stepWriteGraph); err != nil {
		return manifestSegment{}, err
	}

	graphRef, err := writeGraphFile(
		ctx,
		filepath.Join(segmentTemp, graphFileName),
		index,
		vectorsRef,
		options.Durability,
	)
	if err != nil {
		return manifestSegment{}, err
	}

	if err := afterStep(options, stepWriteGraph, false); err != nil {
		return manifestSegment{}, err
	}

	if graphRef.Size > min(options.Limits.MaxFileBytes, options.Limits.MaxGraphBytes) {
		return manifestSegment{}, ErrLimitExceeded
	}

	// Make the temporary segment durable before publishing it by name.
	if options.Durability == DurabilitySynchronous {
		if err := beforeStep(ctx, options, stepSyncSegment); err != nil {
			return manifestSegment{}, err
		}

		if err := syncDirectory(segmentTemp); err != nil {
			return manifestSegment{}, err
		}

		if err := afterStep(options, stepSyncSegment, false); err != nil {
			return manifestSegment{}, err
		}
	}

	// Derive the immutable object identity from the persisted vector and graph
	// references, then install or reuse the corresponding segment directory.
	objectID := segmentObjectID(vectorsRef, graphRef)
	storedSegmentPath := filepath.Join(paths.segments, objectID)

	if err := beforeStep(ctx, options, stepRenameSegment); err != nil {
		return manifestSegment{}, err
	}

	// Reuse an already existing identical segment when possible. Otherwise,
	// atomically install the fully written temporary directory.
	if _, err := os.Lstat(storedSegmentPath); err == nil {
		if err := verifyStoredSegment(
			storedSegmentPath,
			vectorsRef,
			graphRef,
			options.Limits,
			options.Durability,
		); err != nil {
			return manifestSegment{}, err
		}

		if options.Durability == DurabilitySynchronous {
			if err := syncDirectory(paths.segments); err != nil {
				return manifestSegment{}, err
			}
		}

		if err := os.RemoveAll(segmentTemp); err != nil {
			return manifestSegment{}, err
		}

		owned = false
	} else if !errors.Is(err, os.ErrNotExist) {
		return manifestSegment{}, err
	} else {
		if err := os.Rename(segmentTemp, storedSegmentPath); err != nil {
			return manifestSegment{}, fmt.Errorf("semanticpersist: install segment: %w", err)
		}

		owned = false

		if options.Durability == DurabilitySynchronous {
			if err := syncDirectory(paths.segments); err != nil {
				return manifestSegment{}, err
			}
		}
	}

	if err := afterStep(options, stepRenameSegment, false); err != nil {
		return manifestSegment{}, err
	}

	// Return references that identify the immutable persisted segment.
	return manifestSegment{
		ObjectID: objectID,
		Vectors:  vectorsRef,
		Graph:    graphRef,
	}, nil
}

func writeVectorsFile(
	ctx context.Context,
	path string,
	source vector.PreparedVectorStore,
	maxK int,
	durability DurabilityMode,
) (fileReference, error) {
	// Create a new vector file exclusively so an existing object is never
	// overwritten accidentally.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fileReference{}, err
	}

	// Encode the prepared vectors and optionally force file contents to disk.
	metadata, writeErr := writeVectorFile(ctx, file, source, maxK, false)
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

	// Expose only the persisted identity needed by the manifest and graph file.
	return fileReference{
		Size:   metadata.Size,
		SHA256: metadata.SHA256,
	}, nil
}

func writeGraphFile(
	ctx context.Context,
	path string,
	reader *hnsw.Index,
	vectors fileReference,
	durability DurabilityMode,
) (fileReference, error) {
	// Create the graph file separately from the vector payload.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fileReference{}, err
	}

	// Encode the HNSW graph together with the vector file reference it belongs to.
	metadata, writeErr := hnsw.WriteGraph(
		ctx,
		file,
		reader,
		hnsw.VectorFileReference{
			Size:   vectors.Size,
			SHA256: vectors.SHA256,
		},
	)

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

	return fileReference{
		Size:   metadata.Size,
		SHA256: metadata.SHA256,
	}, nil
}

func verifyStoredSegment(path string, vectors, graph fileReference, limits Limits, durability DurabilityMode) error {
	if err := validateDirectory(path); err != nil {
		return err
	}
	vectorsPath := filepath.Join(path, vectorsFileName)
	graphPath := filepath.Join(path, graphFileName)
	if err := verifyReferencedFile(vectorsPath, vectors, limits.MaxVectorBytes+128, durability); err != nil {
		return err
	}
	if err := verifyReferencedFile(graphPath, graph, min(limits.MaxFileBytes, limits.MaxGraphBytes), durability); err != nil {
		return err
	}
	if durability == DurabilitySynchronous {
		return syncDirectory(path)
	}
	return nil
}

func graphFileSize(index *hnsw.Index) uint64 {
	if index == nil {
		return 0
	}

	stats := index.Report().Storage
	// Account for alignment padding required by the graph file format.
	padding := uint64((4 - stats.VectorRows%4) % 4)
	// Start with the fixed-size graph header and per-node metadata.
	size, ok := arithmetic.CheckAdd64(164, stats.NodeMetadataBytes)
	if !ok {
		return ^uint64(0)
	}
	// Add the variable-sized serialized graph sections.
	for _, part := range []uint64{padding, stats.OffsetBytes, stats.LinkBytes} {
		size, ok = arithmetic.CheckAdd64(size, part)
		if !ok {
			return ^uint64(0)
		}
	}
	// Return the expected serialized graph file size.
	return size
}
