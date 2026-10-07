package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

type snapshotSegment struct {
	data          semantic.SegmentData
	livenessWords []uint64
}

type snapshot struct {
	state    semanticformat.ServiceState
	segments []snapshotSegment
}

func captureSnapshot(ctx context.Context, service *semantic.Service) (snapshot, error) {
	committed, err := service.CommittedState(ctx)
	if err != nil {
		return snapshot{}, err
	}
	segments := committed.Segments()
	result := snapshot{
		state: semanticformat.ServiceState{
			Config: committed.Config(), Revision: committed.Revision(),
			MaxAllocatedVectorID: committed.MaxAllocatedVectorID(), NextComponentID: committed.NextComponentID(),
			Segments: make([]semanticformat.SegmentState, len(segments)),
		},
		segments: make([]snapshotSegment, len(segments)),
	}
	for i, segment := range segments {
		data := segment.Data()
		words := segment.LivenessWords()
		result.segments[i] = snapshotSegment{data: data, livenessWords: words}
		result.state.Segments[i] = semanticformat.SegmentState{ComponentID: data.ComponentID, Rows: data.Rows, LivenessWords: words}
	}
	return result, nil
}
