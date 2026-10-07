package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

type segmentSource struct {
	data          semantic.SegmentData
	livenessWords []uint64
}

type persistenceSnapshot struct {
	state    semanticformat.ServiceSnapshot
	segments []segmentSource
}

func buildPersistenceSnapshot(ctx context.Context, service *semantic.Service) (persistenceSnapshot, error) {
	committed, err := service.CommittedState(ctx)
	if err != nil {
		return persistenceSnapshot{}, err
	}
	segments := committed.Segments()
	result := persistenceSnapshot{
		state: semanticformat.ServiceSnapshot{
			Config: committed.Config(), Revision: committed.Revision(),
			MaxAllocatedVectorID: committed.MaxAllocatedVectorID(), NextComponentID: committed.NextComponentID(),
			Segments: make([]semanticformat.SegmentState, len(segments)),
		},
		segments: make([]segmentSource, len(segments)),
	}
	for i, segment := range segments {
		data := segment.Data()
		words := segment.LivenessWords()
		result.segments[i] = segmentSource{data: data, livenessWords: words}
		result.state.Segments[i] = semanticformat.SegmentState{ComponentID: data.ComponentID, Rows: data.Rows, LivenessWords: words}
	}
	return result, nil
}
