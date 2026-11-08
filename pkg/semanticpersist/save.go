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
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
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
	options SaveOptions,
) (Generation, error) {
	if options.ExpectedGeneration == nil {
		return Generation{}, ErrExpectedGenerationRequired
	}
	cfg, err := newSettings(options.Limits)
	if err != nil {
		return Generation{}, err
	}
	generationID, err := save(ctx, root, index, options.ExpectedGeneration.ID, cfg)
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
	options SaveOptions,
) (Generation, error) {
	if options.ExpectedGeneration == nil {
		return Generation{}, ErrExpectedGenerationRequired
	}
	cfg, err := newSettings(options.Limits)
	if err != nil {
		return Generation{}, err
	}
	generationID, err := commitAndSave(
		ctx,
		root,
		index,
		options.ExpectedGeneration.ID,
		cfg,
	)
	return Generation{ID: generationID}, err
}

func commitAndSave(
	ctx context.Context,
	root string,
	index *semantic.Index,
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
	return saveCommittedState(ctx, root, committed, expectedGenerationID, cfg)
}

func saveCommittedState(
	ctx context.Context,
	root string,
	committed *semantic.CommittedIndexState,
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
	state, err := buildPersistenceState(ctx, committed)
	if err != nil {
		return 0, err
	}

	l := newLayout(root)
	rootCreated, err := l.prepare()
	if err != nil {
		return 0, err
	}
	lock, err := storage.AcquireOperationLock(l.lock)
	if err != nil {
		return 0, err
	}
	defer lock.Close()

	for _, path := range []string{l.segments, l.objects, l.generations, l.root} {
		if err := persistfs.SyncDirectory(path); err != nil {
			return 0, err
		}
	}
	if rootCreated {
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
	return writer.write(ctx, state, nextID, cfg)
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
			state.state.Config.Limits.MaxChunkCandidates,
		)
		if err != nil {
			return 0, err
		}
		segmentRefs[i] = ref
	}
	if reused {
		if err := beforeStep(ctx, cfg.hooks, stepSyncReusedSegments); err != nil {
			return 0, err
		}
		if err := persistfs.SyncDirectory(w.layout.segments); err != nil {
			return 0, err
		}
		if err := afterStep(cfg.hooks, stepSyncReusedSegments, false); err != nil {
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
	err = newCurrentFile(w.layout, cfg).commit(ctx, generationformat.Head{
		GenerationID: nextID,
		ManifestHash: generation.manifestRef.SHA256,
	})
	if err != nil {
		return 0, err
	}
	return nextID, nil
}
