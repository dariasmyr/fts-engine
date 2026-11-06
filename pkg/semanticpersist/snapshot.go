package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
)

type segmentSource struct {
	checkpoint semantic.CheckpointSegment
}

type persistenceSnapshot struct {
	state    generationformat.ServiceSnapshot
	segments []segmentSource
}

func buildPersistenceSnapshot(ctx context.Context, service *semantic.Index) (persistenceSnapshot, error) {
	checkpoint, err := service.CreateCheckpoint(ctx)
	if err != nil {
		return persistenceSnapshot{}, err
	}
	segments := checkpoint.Segments
	result := persistenceSnapshot{
		state: generationformat.ServiceSnapshot{
			Config: checkpoint.Config, Revision: checkpoint.Revision,
			MaxAllocatedVectorID: checkpoint.MaxAllocatedVectorID, NextSegmentID: checkpoint.NextSegmentID,
			Segments: make([]generationformat.SegmentState, len(segments)),
		},
		segments: make([]segmentSource, len(segments)),
	}
	for i, segment := range segments {
		result.segments[i] = segmentSource{checkpoint: segment}
		result.state.Segments[i] = generationformat.SegmentState{
			ID: segment.ID, Rows: segment.Rows, LivenessWords: segment.LivenessWords,
		}
	}
	return result, nil
}
