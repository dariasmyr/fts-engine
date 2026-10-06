package semanticpersist

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
)

// SCUR wire-format version.
const currentVersion = uint16(1)

// Current contains only fixed-width fields: the common header, generation ID,
// manifest digest, and checksum footer.
const currentEncodedSize = wireHeaderSize + wireUint64Size + sha256.Size + wireChecksumSize

// Current is the decoded SCUR payload.
type Current struct {
	GenerationID uint64
	ManifestHash [sha256.Size]byte
}

func EncodeCurrent(value Current, limits Limits) ([]byte, FileReference, error) {
	if value.GenerationID == 0 {
		return nil, FileReference{}, ErrCorrupt
	}
	e := newEncoder("SCUR", currentVersion, currentEncodedSize, limits)
	e.writeUint64(value.GenerationID)
	e.writeBytes(value.ManifestHash[:])
	return e.finish()
}

func DecodeCurrent(data []byte, limits Limits) (Current, error) {
	d, err := newDecoder(data, "SCUR", currentVersion, limits)
	if err != nil {
		return Current{}, err
	}
	value := Current{GenerationID: d.readUint64()}
	copy(value.ManifestHash[:], d.readBytes(sha256.Size))
	if err := d.done(); err != nil {
		return Current{}, err
	}
	if value.GenerationID == 0 {
		return Current{}, ErrCorrupt
	}
	return value, nil
}

func encodeCurrent(value currentRecord, limits Limits) ([]byte, fileReference, error) {
	data, ref, err := EncodeCurrent(Current{GenerationID: value.GenerationID, ManifestHash: value.ManifestHash}, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeCurrent(data []byte, limits Limits) (currentRecord, error) {
	value, err := DecodeCurrent(data, codecLimits(limits))
	if err != nil {
		return currentRecord{}, mapCodecError(err)
	}
	return currentRecord{GenerationID: value.GenerationID, ManifestHash: value.ManifestHash}, nil
}

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
