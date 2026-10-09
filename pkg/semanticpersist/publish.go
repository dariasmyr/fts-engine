package semanticpersist

import (
	"context"
	"errors"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

type publisher struct {
	layout       layout
	openedLimits Limits
	generation   Generation
	reusable     map[semantic.SegmentID]semanticformat.SegmentRef
}

func (p *publisher) publish(ctx context.Context, index *semantic.Index, options PublishOptions, fullyValidateCurrent bool) (Generation, error) {
	if options.ExpectedGeneration == nil {
		return Generation{}, ErrExpectedGenerationRequired
	}
	currentGeneration, err := p.currentGeneration(ctx, options, fullyValidateCurrent)
	if err != nil {
		return Generation{}, err
	}
	if currentGeneration != options.ExpectedGeneration.ID || currentGeneration == math.MaxUint64 {
		return Generation{}, ErrStaleGeneration
	}

	snapshot, err := buildPersistenceSnapshot(ctx, index)
	if err != nil {
		return Generation{}, err
	}
	objects := newObjectStore(p.layout, options)
	segmentRefs := make([]semanticformat.SegmentRef, len(snapshot.segments))
	reused := false
	for i, segment := range snapshot.segments {
		if err := validateSegmentData(segment.data, snapshot.state.Config, options.Limits); err != nil {
			return Generation{}, err
		}
		if ref, ok := p.reusable[segment.data.ID]; ok {
			if err := objects.verify(ref); err != nil {
				return Generation{}, err
			}
			segmentRefs[i] = ref
			reused = true
			continue
		}
		ref, err := objects.put(
			ctx,
			segment.data,
			snapshot.state.Config.Limits.MaxChunkCandidates,
		)
		if err != nil {
			return Generation{}, err
		}
		segmentRefs[i] = ref
	}
	if reused && options.Durability == DurabilitySynchronous {
		if err := beforeStep(ctx, options, stepSyncReusedSegments); err != nil {
			return Generation{}, err
		}
		if err := (durabilityPolicy{mode: options.Durability}).syncDirectory(p.layout.segments); err != nil {
			return Generation{}, err
		}
		if err := afterStep(options, stepSyncReusedSegments, false); err != nil {
			return Generation{}, err
		}
	}

	nextID := currentGeneration + 1
	generations := newGenerationStore(p.layout, options)
	generation, err := generations.write(ctx, nextID, snapshot.state, segmentRefs)
	if err != nil {
		return Generation{}, err
	}
	head := newHeadStore(p.layout, options)
	if err := head.commit(ctx, semanticformat.Head{GenerationID: nextID, ManifestHash: generation.manifestRef.SHA256}); err != nil {
		return Generation{}, err
	}

	p.generation = Generation{ID: nextID}
	p.reusable = make(map[semantic.SegmentID]semanticformat.SegmentRef, len(segmentRefs))
	for i, segment := range snapshot.segments {
		p.reusable[segment.data.ID] = segmentRefs[i]
	}
	return p.generation, nil
}

func (p *publisher) currentGeneration(ctx context.Context, options PublishOptions, fullyValidate bool) (uint64, error) {
	headStore := newHeadStore(p.layout, options)
	head, err := headStore.read(ctx)
	if errors.Is(err, ErrCurrentMissing) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !fullyValidate {
		generations := newGenerationStore(p.layout, options)
		if _, _, err := generations.openManifest(ctx, head.GenerationID, head.ManifestHash, true); err != nil {
			return 0, err
		}
		return head.GenerationID, nil
	}
	generations := newGenerationStore(p.layout, options)

	persisted, err := generations.open(
		ctx,
		head.GenerationID,
		&head.ManifestHash,
	)
	if err != nil {
		return 0, err
	}

	objects := newObjectStore(p.layout, options)

	_, err = restoreIndex(
		ctx,
		Generation{ID: head.GenerationID},
		persisted,
		objects,
		semantic.Schema{},
	)
	if err != nil {
		return 0, corruptMissingReference(err)
	}

	return head.GenerationID, nil
}
