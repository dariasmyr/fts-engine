package semanticpersist

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
)

func commitCurrent(ctx context.Context, paths storePaths, generationID uint64, manifestRef fileReference, options Options) (Generation, error) {
	if err := beforeStep(ctx, options, stepWriteCurrent); err != nil {
		return Generation{}, err
	}
	currentData, _, err := encodeCurrent(currentRecord{GenerationID: generationID, ManifestHash: manifestRef.SHA256}, options.Limits)
	if err != nil {
		return Generation{}, err
	}
	currentTemp, err := os.CreateTemp(paths.root, ".tmp-current-")
	if err != nil {
		return Generation{}, err
	}
	currentTempName := currentTemp.Name()
	defer os.Remove(currentTempName)
	if err := writeAllFile(currentTemp, currentData); err != nil {
		_ = currentTemp.Close()
		return Generation{}, err
	}
	if options.Durability == DurabilitySynchronous {
		if err := currentTemp.Sync(); err != nil {
			_ = currentTemp.Close()
			return Generation{}, err
		}
	}
	if err := currentTemp.Close(); err != nil {
		return Generation{}, err
	}
	if err := afterStep(options, stepWriteCurrent, false); err != nil {
		return Generation{}, err
	}
	if err := beforeStep(ctx, options, stepReplaceCurrent); err != nil {
		return Generation{}, err
	}
	if err := atomicReplace(currentTempName, filepath.Join(paths.root, currentFileName)); err != nil {
		return Generation{}, fmt.Errorf("semanticpersist: replace CURRENT: %w", err)
	}
	if err := afterStep(options, stepReplaceCurrent, true); err != nil {
		return Generation{}, err
	}
	if options.Durability == DurabilitySynchronous {
		if err := beforeStep(context.Background(), options, stepSyncStore); err != nil {
			return Generation{}, fmt.Errorf("%w: %v", ErrIndeterminate, err)
		}
		if err := syncDirectory(paths.root); err != nil {
			return Generation{}, fmt.Errorf("%w: %v", ErrIndeterminate, err)
		}
		if err := afterStep(options, stepSyncStore, true); err != nil {
			return Generation{}, err
		}
	}
	return Generation{ID: generationID}, nil
}

func repairCurrent(paths storePaths, generationID uint64, segments []manifestSegment, manifestHash [sha256.Size]byte, options Options) error {
	if options.Durability == DurabilitySynchronous {
		if err := syncPublishedGeneration(paths, generationID, segments); err != nil {
			return err
		}
	}
	data, _, err := encodeCurrent(currentRecord{GenerationID: generationID, ManifestHash: manifestHash}, options.Limits)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(paths.root, ".tmp-current-repair-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := writeAllFile(temp, data); err != nil {
		_ = temp.Close()
		return err
	}
	if options.Durability == DurabilitySynchronous {
		if err := temp.Sync(); err != nil {
			_ = temp.Close()
			return err
		}
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := atomicReplace(name, filepath.Join(paths.root, currentFileName)); err != nil {
		return err
	}
	if options.Durability == DurabilitySynchronous {
		if err := syncDirectory(paths.root); err != nil {
			return fmt.Errorf("%w: %v", ErrIndeterminate, err)
		}
	}
	return nil
}
