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
	reusable     map[uint64]semanticformat.SegmentRef
}

func (p *publisher) publish(ctx context.Context, service *semantic.Service, options Options, fullyValidateCurrent bool) (Generation, error) {
	currentGeneration, err := p.currentGeneration(ctx, options, fullyValidateCurrent)
	if err != nil {
		return Generation{}, err
	}
	if currentGeneration != options.ExpectedGeneration || currentGeneration == math.MaxUint64 {
		return Generation{}, ErrStaleGeneration
	}

	snapshot, err := captureSnapshot(ctx, service)
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
		if ref, ok := p.reusable[segment.data.ComponentID]; ok {
			if err := objects.verify(ref); err != nil {
				return Generation{}, err
			}
			segmentRefs[i] = ref
			reused = true
			continue
		}
		ref, err := objects.put(ctx, segment.data)
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
	p.reusable = make(map[uint64]semanticformat.SegmentRef, len(segmentRefs))
	for i, segment := range snapshot.segments {
		p.reusable[segment.data.ComponentID] = segmentRefs[i]
	}
	return p.generation, nil
}

func (p *publisher) currentGeneration(ctx context.Context, options Options, fullyValidate bool) (uint64, error) {
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
	restorer := newRestorer(p.layout, options.Limits)
	if _, err := restorer.openGeneration(ctx, head.GenerationID, head.ManifestHash, true, semantic.PipelineDescriptor{}); err != nil {
		return 0, err
	}
	return head.GenerationID, nil
}
