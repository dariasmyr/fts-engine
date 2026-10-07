package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

type restoreResult struct {
	service    *semantic.Service
	generation Generation
	reusable   map[uint64]semanticformat.SegmentRef
}

func restoreService(
	ctx context.Context,
	generation Generation,
	persisted persistedGeneration,
	objects objectStore,
	expected semantic.PipelineDescriptor,
) (restoreResult, error) {
	if err := validateExpectedDescriptors(persisted.state.Config, expected); err != nil {
		return restoreResult{}, err
	}

	stored := make([]semantic.StoredSegment, len(persisted.state.Segments))
	reusable := make(
		map[uint64]semanticformat.SegmentRef,
		len(persisted.state.Segments),
	)

	for i, stateSegment := range persisted.state.Segments {
		ref := persisted.manifest.Segments[i]

		data, err := objects.open(
			ctx,
			ref,
			persisted.state.Config,
			stateSegment,
		)
		if err != nil {
			return restoreResult{}, err
		}

		stored[i] = semantic.StoredSegment{
			Data:          data,
			LivenessWords: stateSegment.LivenessWords,
		}

		reusable[stateSegment.ComponentID] = ref
	}

	service, err := semantic.Restore(
		ctx,
		semantic.RestoreState{
			Config:               persisted.state.Config,
			Revision:             persisted.state.Revision,
			MaxAllocatedVectorID: persisted.state.MaxAllocatedVectorID,
			NextComponentID:      persisted.state.NextComponentID,
			Segments:             stored,
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			return restoreResult{}, ctx.Err()
		}

		return restoreResult{}, ErrCorrupt
	}

	return restoreResult{
		service:    service,
		generation: generation,
		reusable:   reusable,
	}, nil
}

func validateExpectedDescriptors(
	config semantic.Config,
	expected semantic.PipelineDescriptor,
) error {
	if expected == (semantic.PipelineDescriptor{}) {
		return nil
	}

	if config.Embedding != expected.Embedding {
		return ErrEmbeddingMismatch
	}

	if config.Chunking != expected.Chunking {
		return ErrChunkingMismatch
	}

	return nil
}
