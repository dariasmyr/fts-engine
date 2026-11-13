package semanticpersist

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type generationWriter struct {
	layout   layout
	reusable map[semantic.SegmentID]generationformat.SegmentRef
}

// Save persists one committed index generation using optimistic generation
// matching. The filesystem lock is held only for this operation.
func Save(
	ctx context.Context,
	root string,
	index *semantic.Index,
	config SaveConfig,
) (Generation, error) {
	if config.ExpectedGeneration == nil {
		return Generation{}, ErrExpectedGenerationRequired
	}
	cfg, err := newSettings(config.Config)
	if err != nil {
		return Generation{}, err
	}
	generationID, err := save(ctx, root, index, nil, config.ExpectedGeneration.ID, cfg)
	return Generation{ID: generationID}, err
}

// SaveService persists one committed model-backed service generation. The
// saved pipeline config is restored by LoadService and cannot be replaced by the
// caller during loading.
func SaveService(
	ctx context.Context,
	root string,
	service *semantic.Service,
	config SaveConfig,
) (Generation, error) {
	if config.ExpectedGeneration == nil {
		return Generation{}, ErrExpectedGenerationRequired
	}
	serviceConfig, index, err := persistedService(service)
	if err != nil {
		return Generation{}, err
	}
	cfg, err := newSettings(config.Config)
	if err != nil {
		return Generation{}, err
	}
	generationID, err := save(
		ctx,
		root,
		index,
		&serviceConfig,
		config.ExpectedGeneration.ID,
		cfg,
	)
	return Generation{ID: generationID}, err
}

// CommitAndSave prepares the pending revision, persists it using optimistic
// generation matching, and only then makes it searchable. Mutations are blocked
// during the operation; searches continue against the previous committed view.
// A failure before CURRENT replacement leaves the batch pending for retry.
func CommitAndSave(
	ctx context.Context,
	root string,
	index *semantic.Index,
	config SaveConfig,
) (Generation, error) {
	if config.ExpectedGeneration == nil {
		return Generation{}, ErrExpectedGenerationRequired
	}
	cfg, err := newSettings(config.Config)
	if err != nil {
		return Generation{}, err
	}
	generationID, err := commitAndSave(
		ctx,
		root,
		index,
		nil,
		config.ExpectedGeneration.ID,
		cfg,
	)
	return Generation{ID: generationID}, err
}

// CommitAndSaveService atomically commits and persists a model-backed service,
// including the immutable pipeline config required by LoadService.
func CommitAndSaveService(
	ctx context.Context,
	root string,
	service *semantic.Service,
	config SaveConfig,
) (Generation, error) {
	if config.ExpectedGeneration == nil {
		return Generation{}, ErrExpectedGenerationRequired
	}
	serviceConfig, index, err := persistedService(service)
	if err != nil {
		return Generation{}, err
	}
	cfg, err := newSettings(config.Config)
	if err != nil {
		return Generation{}, err
	}
	generationID, err := commitAndSave(
		ctx,
		root,
		index,
		&serviceConfig,
		config.ExpectedGeneration.ID,
		cfg,
	)
	return Generation{ID: generationID}, err
}

func persistedService(service *semantic.Service) (semantic.PipelineConfig, *semantic.Index, error) {
	if service == nil {
		return semantic.PipelineConfig{}, nil, ErrCorrupt
	}
	config, ok := service.Config()
	if !ok {
		return semantic.PipelineConfig{}, nil, semantic.ErrModelServiceUnavailable
	}
	index := service.Index()
	if index == nil || config.Index != index.Config() {
		return semantic.PipelineConfig{}, nil, ErrCorrupt
	}
	return config.Pipeline, index, nil
}

func commitAndSave(
	ctx context.Context,
	root string,
	index *semantic.Index,
	serviceConfig *semantic.PipelineConfig,
	expectedGenerationID uint64,
	cfg settings,
) (uint64, error) {
	if ctx == nil {
		return 0, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if root == "" || index == nil {
		return 0, ErrCorrupt
	}

	var generationID uint64
	var saveErr error
	err := index.CommitWithState(ctx, func(committed *semantic.CommittedIndexState) error {
		generationID, saveErr = saveCommittedState(
			ctx,
			root,
			committed,
			serviceConfig,
			expectedGenerationID,
			cfg,
		)
		if errors.Is(saveErr, ErrIndeterminate) {
			// State transition boundary: CURRENT may already reference the candidate,
			// so the matching in-memory view must be published before returning.
			return nil
		}
		return saveErr
	})
	if err != nil {
		return generationID, err
	}
	return generationID, saveErr
}

func save(
	ctx context.Context,
	root string,
	index *semantic.Index,
	serviceConfig *semantic.PipelineConfig,
	expectedGenerationID uint64,
	cfg settings,
) (uint64, error) {
	if ctx == nil {
		return 0, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if root == "" || index == nil {
		return 0, ErrCorrupt
	}
	committed, err := index.ExportState(ctx)
	if err != nil {
		return 0, err
	}
	return saveCommittedState(ctx, root, committed, serviceConfig, expectedGenerationID, cfg)
}

func saveCommittedState(
	ctx context.Context,
	root string,
	committed *semantic.CommittedIndexState,
	serviceConfig *semantic.PipelineConfig,
	expectedGenerationID uint64,
	cfg settings,
) (uint64, error) {
	if ctx == nil {
		return 0, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if root == "" || committed == nil {
		return 0, ErrCorrupt
	}

	// Validation boundary: convert the detached domain state into the wire DTO
	// before acquiring filesystem resources.
	state, err := buildPersistenceState(ctx, committed, serviceConfig)
	if err != nil {
		return 0, err
	}

	return withStorageOperationResult(root, operationLayoutPrepare, func(operation *storageOperation) (uint64, error) {
		l := operation.layout
		for _, path := range []string{l.segments, l.objects, l.generations, l.root} {
			if err := persistfs.SyncDirectory(path); err != nil {
				return 0, err
			}
		}
		if operation.rootCreated {
			if err := persistfs.SyncDirectory(filepath.Dir(l.root)); err != nil {
				return 0, err
			}
		}

		head, hasCurrent, err := readCurrent(ctx, l, cfg)
		if err != nil {
			return 0, err
		}
		currentID := uint64(0)
		if hasCurrent {
			currentID = head.GenerationID
		}
		if currentID != expectedGenerationID {
			return 0, ErrStaleGeneration
		}

		reusable, err := loadReusableSegments(ctx, l, cfg, head, hasCurrent, state.state.ID)
		if err != nil {
			return 0, err
		}
		nextID, err := nextGenerationID(l)
		if err != nil {
			return 0, err
		}
		writer := generationWriter{layout: l, reusable: reusable}
		generationID, err := writer.write(ctx, state, nextID, cfg)
		if err != nil {
			return 0, err
		}
		operation.markPublishedToStorage()
		return generationID, nil
	})
}

func readCurrent(
	ctx context.Context,
	l layout,
	cfg settings,
) (generationformat.Head, bool, error) {
	head, err := newCurrentFile(l, cfg).read(ctx)
	if errors.Is(err, ErrCurrentMissing) {
		return generationformat.Head{}, false, nil
	}
	if err != nil {
		return generationformat.Head{}, false, err
	}
	return head, true, nil
}

func loadReusableSegments(
	ctx context.Context,
	l layout,
	cfg settings,
	head generationformat.Head,
	hasCurrent bool,
	indexID semantic.IndexID,
) (map[semantic.SegmentID]generationformat.SegmentRef, error) {
	if !hasCurrent {
		return nil, nil
	}
	persisted, err := newGenerationFiles(l, cfg).open(
		ctx,
		head.GenerationID,
		&head.ManifestHash,
	)
	if err != nil {
		return nil, err
	}
	if persisted.state.ID != indexID {
		return nil, nil
	}
	reusable := make(map[semantic.SegmentID]generationformat.SegmentRef, len(persisted.state.Segments))
	for i, segment := range persisted.state.Segments {
		reusable[segment.ID] = persisted.manifest.Segments[i]
	}
	return reusable, nil
}

func nextGenerationID(l layout) (uint64, error) {
	entries, err := os.ReadDir(l.generations)
	if err != nil {
		return 0, err
	}
	var latest uint64
	for _, entry := range entries {
		if id, ok := parseGenerationName(entry.Name()); ok {
			latest = max(latest, id)
		}
	}
	if latest == math.MaxUint64 {
		return 0, ErrGenerationExhausted
	}
	return latest + 1, nil
}

func (w *generationWriter) write(
	ctx context.Context,
	state persistenceState,
	nextID uint64,
	cfg settings,
) (uint64, error) {
	segments := newSegmentStore(w.layout, cfg)
	segmentRefs := make([]generationformat.SegmentRef, len(state.segments))
	reused := false
	for i, segment := range state.segments {
		if err := validateSegmentData(segment.state, state.state.Config, cfg.limits); err != nil {
			return 0, err
		}
		if ref, ok := w.reusable[segment.state.ID]; ok {
			if err := segments.verify(ref); err != nil {
				return 0, err
			}
			segmentRefs[i] = ref
			reused = true
			continue
		}
		ref, err := segments.put(
			ctx,
			segment.state,
			state.state.Config.Operational.Search.MaxCandidateChunks,
		)
		if err != nil {
			return 0, err
		}
		segmentRefs[i] = ref
	}
	if reused {
		if err := cfg.checkpoints.before(ctx, checkpointSyncReusedSegments); err != nil {
			return 0, err
		}
		if err := persistfs.SyncDirectory(w.layout.segments); err != nil {
			return 0, err
		}
		if err := cfg.checkpoints.after(checkpointSyncReusedSegments); err != nil {
			return 0, err
		}
	}

	generation, err := newGenerationFiles(w.layout, cfg).write(
		ctx,
		nextID,
		state.state,
		segmentRefs,
	)
	if err != nil {
		return 0, err
	}
	err = newCurrentFile(w.layout, cfg).publishToStorage(ctx, generationformat.Head{
		GenerationID: nextID,
		ManifestHash: generation.manifestRef.SHA256,
	})
	if err != nil {
		return 0, err
	}
	return nextID, nil
}
