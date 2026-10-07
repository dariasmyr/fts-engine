package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

type restoreResult struct {
	index      *semantic.Index
	generation Generation
	reusable   map[semantic.SegmentID]semanticformat.SegmentRef
}

func restoreIndex(
	ctx context.Context,
	generation Generation,
	persisted persistedGeneration,
	objects objectStore,
	expected semantic.Schema,
) (restoreResult, error) {
	if err := validateExpectedSchema(persisted.state.Config, expected); err != nil {
		return restoreResult{}, err
	}

	stored := make([]semantic.StateSegment, len(persisted.state.Segments))

	reusable := make(
		map[semantic.SegmentID]semanticformat.SegmentRef,
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

		stored[i] = semantic.StateSegment{
			Data:          data,
			LivenessWords: stateSegment.LivenessWords,
		}

		reusable[stateSegment.ID] = ref
	}

	index, err := semantic.Open(
		ctx,
		semantic.State{
			Config:               persisted.state.Config,
			Revision:             persisted.state.Revision,
			MaxAllocatedVectorID: persisted.state.MaxAllocatedVectorID,
			NextSegmentID:        persisted.state.NextSegmentID,
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
		index:      index,
		generation: generation,
		reusable:   reusable,
	}, nil
}

func validateExpectedSchema(
	config semantic.Config,
	expected semantic.Schema,
) error {
	if expected == (semantic.Schema{}) {
		return nil
	}

	if config.Schema.Embedding != expected.Embedding {
		return ErrEmbeddingMismatch
	}

	if config.Schema.Chunking != expected.Chunking {
		return ErrChunkingMismatch
	}

	return nil
}
