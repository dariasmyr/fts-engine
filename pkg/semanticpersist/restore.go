package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
)

type restoreResult struct {
	index      *semantic.Index
	generation Generation
	reusable   map[semantic.SegmentID]generationformat.SegmentRef
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

	segments := make([]semantic.CheckpointSegment, len(persisted.state.Segments))

	reusable := make(
		map[semantic.SegmentID]generationformat.SegmentRef,
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

		data.LivenessWords = append([]uint64(nil), stateSegment.LivenessWords...)
		segments[i] = data

		reusable[stateSegment.ID] = ref
	}

	index, err := semantic.Restore(
		ctx,
		semantic.IndexCheckpoint{
			Config:               persisted.state.Config,
			Revision:             persisted.state.Revision,
			MaxAllocatedVectorID: persisted.state.MaxAllocatedVectorID,
			NextSegmentID:        persisted.state.NextSegmentID,
			Segments:             segments,
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
