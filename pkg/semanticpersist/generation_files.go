package semanticpersist

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/internal/format"
	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
)

type persistedGeneration struct {
	id          uint64
	manifest    generationformat.GenerationManifest
	manifestRef generationformat.FileRef
	state       generationformat.IndexState
}

type generationFiles struct {
	layout      layout
	limits      Config
	checkpoints saveCheckpoints
}

func newGenerationFiles(l layout, cfg settings) generationFiles {
	return generationFiles{layout: l, limits: cfg.limits, checkpoints: cfg.checkpoints}
}

func (s generationFiles) write(ctx context.Context, id uint64, state generationformat.IndexState, segments []generationformat.SegmentRef) (persistedGeneration, error) {
	if id == 0 || len(state.Segments) != len(segments) {
		return persistedGeneration{}, ErrCorrupt
	}
	manifestPreview := generationformat.GenerationManifest{GenerationID: id, Segments: segments}
	if err := validateManifestObjectRefs(manifestPreview); err != nil {
		return persistedGeneration{}, err
	}
	generationTemp, err := os.MkdirTemp(s.layout.generations, ".tmp-gen-")
	if err != nil {
		return persistedGeneration{}, fmt.Errorf("semanticpersist: create generation temp: %w", err)
	}
	owned := true
	defer func() {
		if owned {
			_ = os.RemoveAll(generationTemp)
		}
	}()

	if err := s.checkpoints.before(ctx, checkpointWriteState); err != nil {
		return persistedGeneration{}, err
	}
	stateData, stateRef, err := generationformat.EncodeState(state, generationFormatConfig(s.limits))
	if err != nil {
		return persistedGeneration{}, mapFormatError(err)
	}
	if err := writeDataFile(filepath.Join(generationTemp, stateFileName), stateData); err != nil {
		return persistedGeneration{}, err
	}
	if err := s.checkpoints.after(checkpointWriteState); err != nil {
		return persistedGeneration{}, err
	}

	manifest := generationformat.GenerationManifest{GenerationID: id, State: stateRef, Segments: segments}
	if err := s.checkpoints.before(ctx, checkpointWriteManifest); err != nil {
		return persistedGeneration{}, err
	}
	manifestData, manifestRef, err := generationformat.EncodeManifest(manifest, generationFormatConfig(s.limits))
	if err != nil {
		return persistedGeneration{}, mapFormatError(err)
	}
	if err := validateOpenReferences(uint64(len(manifestData)), manifest, s.limits); err != nil {
		return persistedGeneration{}, err
	}
	if err := writeDataFile(filepath.Join(generationTemp, manifestFileName), manifestData); err != nil {
		return persistedGeneration{}, err
	}
	if err := s.checkpoints.after(checkpointWriteManifest); err != nil {
		return persistedGeneration{}, err
	}

	if err := s.checkpoints.before(ctx, checkpointSyncGeneration); err != nil {
		return persistedGeneration{}, err
	}
	if err := persistfs.SyncDirectory(generationTemp); err != nil {
		return persistedGeneration{}, err
	}
	if err := s.checkpoints.after(checkpointSyncGeneration); err != nil {
		return persistedGeneration{}, err
	}
	finalPath := s.layout.generation(id)
	if _, err := os.Lstat(finalPath); err == nil {
		return persistedGeneration{}, ErrGenerationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return persistedGeneration{}, err
	}
	if err := s.checkpoints.before(ctx, checkpointRenameGeneration); err != nil {
		return persistedGeneration{}, err
	}
	if err := os.Rename(generationTemp, finalPath); err != nil {
		return persistedGeneration{}, fmt.Errorf("semanticpersist: install generation: %w", err)
	}
	owned = false
	if err := persistfs.SyncDirectory(s.layout.generations); err != nil {
		return persistedGeneration{}, err
	}
	if err := s.checkpoints.after(checkpointRenameGeneration); err != nil {
		return persistedGeneration{}, err
	}
	return persistedGeneration{id: id, manifest: manifest, manifestRef: manifestRef, state: state}, nil
}

func (s generationFiles) open(ctx context.Context, id uint64, expectedHash *[sha256.Size]byte) (persistedGeneration, error) {
	if err := ctx.Err(); err != nil {
		return persistedGeneration{}, err
	}
	generationPath := s.layout.generation(id)
	if err := validateDirectory(generationPath); err != nil {
		if expectedHash != nil {
			err = corruptMissingReference(err)
		}
		return persistedGeneration{}, err
	}
	manifestData, err := readRegularFile(s.layout.manifest(id), s.limits.MaxFileBytes)
	if err != nil {
		if expectedHash != nil {
			err = corruptMissingReference(err)
		}
		return persistedGeneration{}, err
	}
	hash := sha256.Sum256(manifestData)
	if expectedHash != nil && hash != *expectedHash {
		return persistedGeneration{}, ErrCorrupt
	}
	manifest, err := generationformat.DecodeManifest(manifestData, generationFormatConfig(s.limits))
	if err != nil {
		return persistedGeneration{}, mapFormatError(err)
	}
	if manifest.GenerationID != id {
		return persistedGeneration{}, ErrCorrupt
	}
	if err := validateManifestObjectRefs(manifest); err != nil {
		return persistedGeneration{}, err
	}
	if err := validateOpenReferences(uint64(len(manifestData)), manifest, s.limits); err != nil {
		return persistedGeneration{}, err
	}
	stateData, err := readReferencedFile(s.layout.state(id), manifest.State, s.limits.MaxFileBytes)
	if err != nil {
		return persistedGeneration{}, err
	}
	state, err := generationformat.DecodeState(stateData, generationFormatConfig(s.limits))
	if err != nil {
		return persistedGeneration{}, mapFormatError(err)
	}
	if len(state.Segments) != len(manifest.Segments) {
		return persistedGeneration{}, ErrCorrupt
	}
	return persistedGeneration{id: id, manifest: manifest, manifestRef: generationformat.FileRef{Size: uint64(len(manifestData)), SHA256: hash}, state: state}, nil
}

func validateOpenReferences(manifestBytes uint64, value generationformat.GenerationManifest, limits Config) error {
	if value.State.Size > limits.MaxFileBytes {
		return ErrLimitExceeded
	}
	estimate := manifestBytes
	components := []struct{ size, multiplier uint64 }{{value.State.Size, 16}}
	for _, segment := range value.Segments {
		if segment.Vectors.Size > min(limits.MaxFileBytes, limits.MaxVectorBytes+128) || segment.Graph.Size > min(limits.MaxFileBytes, limits.MaxGraphBytes) {
			return ErrLimitExceeded
		}
		components = append(
			components,
			struct{ size, multiplier uint64 }{segment.Vectors.Size, 4},
			struct{ size, multiplier uint64 }{segment.Graph.Size, 8},
		)
	}
	for _, component := range components {
		weighted, ok := format.CheckedMultiplyUint64(component.size, component.multiplier)
		if !ok {
			return ErrLimitExceeded
		}
		estimate, ok = format.CheckedAddUint64(estimate, weighted)
		if !ok {
			return ErrLimitExceeded
		}
	}
	scratchBytes, ok := format.CheckedMultiplyUint64(uint64(limits.MaxDimensions), 8)
	if !ok {
		return ErrLimitExceeded
	}
	estimate, ok = format.CheckedAddUint64(estimate, scratchBytes)
	if !ok || estimate > limits.MaxOpenBytes {
		return ErrLimitExceeded
	}
	return nil
}

func validateManifestObjectRefs(manifest generationformat.GenerationManifest) error {
	for _, segment := range manifest.Segments {
		if !validObjectID(segment.ObjectID) || segment.ObjectID != segmentObjectID(segment.Vectors, segment.Graph) {
			return ErrCorrupt
		}
	}
	return nil
}
