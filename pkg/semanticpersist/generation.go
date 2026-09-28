package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func writeGeneration(ctx context.Context, paths storePaths, generationID uint64, object segmentObject, sealed SealedSegment, options Options) (fileReference, error) {
	generationTemp, err := os.MkdirTemp(paths.generations, ".tmp-gen-")
	if err != nil {
		return fileReference{}, fmt.Errorf("semanticpersist: create generation temp: %w", err)
	}
	owned := true
	defer func() {
		if owned {
			_ = os.RemoveAll(generationTemp)
		}
	}()

	if err := beforeStep(ctx, options, StepWriteState); err != nil {
		return fileReference{}, err
	}
	stateData, stateRef, err := encodeState(sealed, options.Limits)
	if err != nil {
		return fileReference{}, err
	}
	if err := writeDataFile(filepath.Join(generationTemp, stateFileName), stateData, options.Durability); err != nil {
		return fileReference{}, err
	}
	if err := afterStep(options, StepWriteState, false); err != nil {
		return fileReference{}, err
	}

	manifestValue := manifest{
		Version: manifestVersion, GenerationID: generationID, ObjectID: object.ID, SegmentKind: sealed.Segment.Kind(),
		Vectors: object.Vectors, Graph: object.Graph, State: stateRef,
	}
	if err := beforeStep(ctx, options, StepWriteManifest); err != nil {
		return fileReference{}, err
	}
	manifestData, manifestRef, err := encodeManifest(manifestValue, options.Limits)
	if err != nil {
		return fileReference{}, err
	}
	if err := validateOpenReferences(manifestData, manifestValue, options.Limits); err != nil {
		return fileReference{}, err
	}
	if err := writeDataFile(filepath.Join(generationTemp, manifestFileName), manifestData, options.Durability); err != nil {
		return fileReference{}, err
	}
	if err := afterStep(options, StepWriteManifest, false); err != nil {
		return fileReference{}, err
	}
	if options.Durability == DurabilitySynchronous {
		if err := beforeStep(ctx, options, StepSyncGeneration); err != nil {
			return fileReference{}, err
		}
		if err := syncDirectory(generationTemp); err != nil {
			return fileReference{}, err
		}
		if err := afterStep(options, StepSyncGeneration, false); err != nil {
			return fileReference{}, err
		}
	}

	generationPath := filepath.Join(paths.generations, generationName(generationID))
	if _, err := os.Lstat(generationPath); err == nil {
		return fileReference{}, ErrGenerationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return fileReference{}, err
	}
	if err := beforeStep(ctx, options, StepRenameGeneration); err != nil {
		return fileReference{}, err
	}
	if err := os.Rename(generationTemp, generationPath); err != nil {
		return fileReference{}, fmt.Errorf("semanticpersist: install generation: %w", err)
	}
	owned = false
	if options.Durability == DurabilitySynchronous {
		if err := syncDirectory(paths.generations); err != nil {
			return fileReference{}, err
		}
	}
	if err := afterStep(options, StepRenameGeneration, false); err != nil {
		return fileReference{}, err
	}
	return manifestRef, nil
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

func syncPublishedGeneration(paths storePaths, generationID uint64, objectID string, hasGraph bool) error {
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
