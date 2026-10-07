package semanticpersist

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

type persistedGeneration struct {
	id          uint64
	manifest    semanticformat.GenerationManifest
	manifestRef semanticformat.FileRef
	state       semanticformat.ServiceState
}

type generationStore struct {
	layout     layout
	limits     Limits
	durability durabilityPolicy
	hooks      Options
}

func newGenerationStore(l layout, options Options) generationStore {
	return generationStore{layout: l, limits: options.Limits, durability: durabilityPolicy{mode: options.Durability}, hooks: options}
}

func (s generationStore) write(ctx context.Context, id uint64, state semanticformat.ServiceState, segments []semanticformat.SegmentRef) (persistedGeneration, error) {
	if id == 0 || len(state.Segments) != len(segments) {
		return persistedGeneration{}, ErrCorrupt
	}
	manifestPreview := semanticformat.GenerationManifest{GenerationID: id, Segments: segments}
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

	if err := beforeStep(ctx, s.hooks, stepWriteState); err != nil {
		return persistedGeneration{}, err
	}
	stateData, stateRef, err := semanticformat.EncodeState(state, stateFormatLimits(s.limits))
	if err != nil {
		return persistedGeneration{}, mapFormatError(err)
	}
	if err := writeDataFile(filepath.Join(generationTemp, stateFileName), stateData, s.durability); err != nil {
		return persistedGeneration{}, err
	}
	if err := afterStep(s.hooks, stepWriteState, false); err != nil {
		return persistedGeneration{}, err
	}

	manifest := semanticformat.GenerationManifest{GenerationID: id, State: stateRef, Segments: segments}
	if err := beforeStep(ctx, s.hooks, stepWriteManifest); err != nil {
		return persistedGeneration{}, err
	}
	manifestData, manifestRef, err := semanticformat.EncodeManifest(manifest, manifestFormatLimits(s.limits))
	if err != nil {
		return persistedGeneration{}, mapFormatError(err)
	}
	if err := validateOpenReferences(uint64(len(manifestData)), manifest, s.limits); err != nil {
		return persistedGeneration{}, err
	}
	if err := writeDataFile(filepath.Join(generationTemp, manifestFileName), manifestData, s.durability); err != nil {
		return persistedGeneration{}, err
	}
	if err := afterStep(s.hooks, stepWriteManifest, false); err != nil {
		return persistedGeneration{}, err
	}

	if s.durability.synchronous() {
		if err := beforeStep(ctx, s.hooks, stepSyncGeneration); err != nil {
			return persistedGeneration{}, err
		}
		if err := s.durability.syncDirectory(generationTemp); err != nil {
			return persistedGeneration{}, err
		}
		if err := afterStep(s.hooks, stepSyncGeneration, false); err != nil {
			return persistedGeneration{}, err
		}
	}
	finalPath := s.layout.generation(id)
	if _, err := os.Lstat(finalPath); err == nil {
		return persistedGeneration{}, ErrGenerationExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return persistedGeneration{}, err
	}
	if err := beforeStep(ctx, s.hooks, stepRenameGeneration); err != nil {
		return persistedGeneration{}, err
	}
	if err := os.Rename(generationTemp, finalPath); err != nil {
		return persistedGeneration{}, fmt.Errorf("semanticpersist: install generation: %w", err)
	}
	owned = false
	if err := s.durability.syncDirectory(s.layout.generations); err != nil {
		return persistedGeneration{}, err
	}
	if err := afterStep(s.hooks, stepRenameGeneration, false); err != nil {
		return persistedGeneration{}, err
	}
	return persistedGeneration{id: id, manifest: manifest, manifestRef: manifestRef, state: state}, nil
}

func (s generationStore) open(ctx context.Context, id uint64, expectedHash [sha256.Size]byte, checkHash bool) (persistedGeneration, error) {
	if err := ctx.Err(); err != nil {
		return persistedGeneration{}, err
	}
	generationPath := s.layout.generation(id)
	if err := validateDirectory(generationPath); err != nil {
		return persistedGeneration{}, err
	}
	manifestData, err := readRegularFile(s.layout.manifest(id), s.limits.MaxFileBytes)
	if err != nil {
		return persistedGeneration{}, err
	}
	hash := sha256.Sum256(manifestData)
	if checkHash && hash != expectedHash {
		return persistedGeneration{}, ErrCorrupt
	}
	manifest, err := semanticformat.DecodeManifest(manifestData, manifestFormatLimits(s.limits))
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
	state, err := semanticformat.DecodeState(stateData, stateFormatLimits(s.limits))
	if err != nil {
		return persistedGeneration{}, mapFormatError(err)
	}
	if len(state.Segments) != len(manifest.Segments) {
		return persistedGeneration{}, ErrCorrupt
	}
	return persistedGeneration{id: id, manifest: manifest, manifestRef: semanticformat.FileRef{Size: uint64(len(manifestData)), SHA256: hash}, state: state}, nil
}

func (s generationStore) openManifest(ctx context.Context, id uint64, expectedHash [sha256.Size]byte, checkHash bool) (semanticformat.GenerationManifest, semanticformat.FileRef, error) {
	if err := ctx.Err(); err != nil {
		return semanticformat.GenerationManifest{}, semanticformat.FileRef{}, err
	}
	if err := validateDirectory(s.layout.generation(id)); err != nil {
		return semanticformat.GenerationManifest{}, semanticformat.FileRef{}, err
	}
	data, err := readRegularFile(s.layout.manifest(id), s.limits.MaxFileBytes)
	if err != nil {
		return semanticformat.GenerationManifest{}, semanticformat.FileRef{}, err
	}
	hash := sha256.Sum256(data)
	if checkHash && hash != expectedHash {
		return semanticformat.GenerationManifest{}, semanticformat.FileRef{}, ErrCorrupt
	}
	manifest, err := semanticformat.DecodeManifest(data, manifestFormatLimits(s.limits))
	if err != nil {
		return semanticformat.GenerationManifest{}, semanticformat.FileRef{}, mapFormatError(err)
	}
	if manifest.GenerationID != id {
		return semanticformat.GenerationManifest{}, semanticformat.FileRef{}, ErrCorrupt
	}
	if err := validateManifestObjectRefs(manifest); err != nil {
		return semanticformat.GenerationManifest{}, semanticformat.FileRef{}, err
	}
	if err := validateOpenReferences(uint64(len(data)), manifest, s.limits); err != nil {
		return semanticformat.GenerationManifest{}, semanticformat.FileRef{}, err
	}
	return manifest, semanticformat.FileRef{Size: uint64(len(data)), SHA256: hash}, nil
}

func validateManifestObjectRefs(manifest semanticformat.GenerationManifest) error {
	for _, segment := range manifest.Segments {
		if !validObjectID(segment.ObjectID) || segment.ObjectID != segmentObjectID(segment.Vectors, segment.Graph) {
			return ErrCorrupt
		}
	}
	return nil
}
