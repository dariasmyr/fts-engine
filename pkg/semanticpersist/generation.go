package semanticpersist

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

func writeGeneration(ctx context.Context, paths storePaths, generationID uint64, objects []segmentObject, snapshot *semantic.CommittedSnapshot, options Options) (fileReference, error) {
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

	if err := beforeStep(ctx, options, stepWriteState); err != nil {
		return fileReference{}, err
	}
	stateData, stateRef, err := encodeState(snapshot, options.Limits)
	if err != nil {
		return fileReference{}, err
	}
	if err := writeDataFile(filepath.Join(generationTemp, stateFileName), stateData, options.Durability); err != nil {
		return fileReference{}, err
	}
	if err := afterStep(options, stepWriteState, false); err != nil {
		return fileReference{}, err
	}

	manifestValue := manifest{GenerationID: generationID, State: stateRef, Segments: make([]manifestSegment, len(objects))}
	for i, object := range objects {
		manifestValue.Segments[i] = manifestSegment{ObjectID: object.ID, Vectors: object.Vectors, Graph: object.Graph}
	}
	if err := beforeStep(ctx, options, stepWriteManifest); err != nil {
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
	if err := afterStep(options, stepWriteManifest, false); err != nil {
		return fileReference{}, err
	}
	if options.Durability == DurabilitySynchronous {
		if err := beforeStep(ctx, options, stepSyncGeneration); err != nil {
			return fileReference{}, err
		}
		if err := syncDirectory(generationTemp); err != nil {
			return fileReference{}, err
		}
		if err := afterStep(options, stepSyncGeneration, false); err != nil {
			return fileReference{}, err
		}
	}

	generationPath := filepath.Join(paths.generations, generationName(generationID))
	if _, err := os.Lstat(generationPath); err == nil {
		return fileReference{}, ErrGenerationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return fileReference{}, err
	}
	if err := beforeStep(ctx, options, stepRenameGeneration); err != nil {
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
	if err := afterStep(options, stepRenameGeneration, false); err != nil {
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

func syncPublishedGeneration(paths storePaths, generationID uint64, segments []manifestSegment) error {
	generationPath := filepath.Join(paths.generations, generationName(generationID))
	files := []string{filepath.Join(generationPath, stateFileName), filepath.Join(generationPath, manifestFileName)}
	directories := []string{generationPath}
	for _, segment := range segments {
		objectPath := filepath.Join(paths.segments, segment.ObjectID)
		files = append(files, filepath.Join(objectPath, vectorsFileName), filepath.Join(objectPath, graphFileName))
		directories = append(directories, objectPath)
	}
	for _, file := range files {
		if err := syncRegularFile(file); err != nil {
			return err
		}
	}
	directories = append(directories, paths.segments, paths.objects, paths.generations, paths.root)
	for _, directory := range directories {
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}
