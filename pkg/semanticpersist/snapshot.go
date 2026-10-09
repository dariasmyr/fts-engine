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

func buildPersistenceSnapshot(ctx context.Context, service *semantic.Index) (persistenceSnapshot, error) {
	indexState, err := service.State(ctx)
	if err != nil {
		return persistenceSnapshot{}, err
	}
	segments := indexState.Segments
	result := persistenceSnapshot{
		state: semanticformat.ServiceSnapshot{
			Config: indexState.Config, Revision: semantic.Revision(indexState.Revision),
			MaxAllocatedVectorID: indexState.MaxAllocatedVectorID, NextSegmentID: indexState.NextSegmentID,
			Segments: make([]semanticformat.SegmentState, len(segments)),
		},
		segments: make([]segmentSource, len(segments)),
	}
	for i, segment := range segments {
		data := segment.Data
		words := segment.LivenessWords
		result.segments[i] = segmentSource{data: data, livenessWords: words}
		result.state.Segments[i] = semanticformat.SegmentState{ID: data.ID, Rows: data.Rows, LivenessWords: words}
	}
	return result, nil
}
