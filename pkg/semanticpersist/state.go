package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
)

type segmentSource struct {
	state semantic.SegmentState
}

type persistenceState struct {
	state    generationformat.IndexState
	segments []segmentSource
}

func buildPersistenceState(
	ctx context.Context,
	committed *semantic.CommittedIndexState,
	pipeline *semantic.PipelineConfig,
) (persistenceState, error) {
	if committed == nil {
		return persistenceState{}, ErrCorrupt
	}
	segments := committed.Segments
	result := persistenceState{
		state: generationformat.IndexState{
			ID: committed.ID, Config: committed.Config,
			Revision:             committed.Revision,
			MaxAllocatedVectorID: committed.MaxAllocatedVectorID, NextSegmentID: committed.NextSegmentID,
			Segments: make([]generationformat.SegmentState, len(segments)),
		},
		segments: make([]segmentSource, len(segments)),
	}
	if pipeline != nil {
		config := *pipeline
		result.state.Pipeline = &config
	}
	for i, segment := range segments {
		if err := ctx.Err(); err != nil {
			return persistenceState{}, err
		}
		result.segments[i] = segmentSource{state: segment}
		result.state.Segments[i] = generationformat.SegmentState{
			ID: segment.ID, Rows: segment.Rows, LivenessWords: segment.LivenessWords,
		}
	}
	return result, nil
}

func restoreIndex(
	ctx context.Context,
	persisted persistedGeneration,
	store segmentStore,
) (*semantic.Index, error) {
	segments := make([]semantic.SegmentState, len(persisted.state.Segments))

	for i, stateSegment := range persisted.state.Segments {
		ref := persisted.manifest.Segments[i]

		data, err := store.open(
			ctx,
			ref,
			persisted.state.Config,
			stateSegment,
		)
		if err != nil {
			return nil, err
		}

		data.LivenessWords = append([]uint64(nil), stateSegment.LivenessWords...)
		segments[i] = data
	}

	index, err := semantic.Restore(
		ctx,
		semantic.CommittedIndexState{
			ID:                   persisted.state.ID,
			Config:               persisted.state.Config,
			Revision:             persisted.state.Revision,
			MaxAllocatedVectorID: persisted.state.MaxAllocatedVectorID,
			NextSegmentID:        persisted.state.NextSegmentID,
			Segments:             segments,
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrCorrupt
	}
	return index, nil
}
