package semanticpersist

import (
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	semanticformat "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/format"
)

func encodeState(snapshot *semantic.CommittedSnapshot, limits Limits) ([]byte, fileReference, error) {
	segments := snapshot.Segments()
	stateSegments := make([]semanticformat.StateSegment, len(segments))
	for i, persisted := range segments {
		segment := persisted.Snapshot().Data()
		stateSegments[i] = semanticformat.StateSegment{ComponentID: segment.ComponentID, Rows: segment.Rows, LivenessWords: persisted.LivenessWords()}
	}
	value := semanticformat.State{Config: snapshot.Config(), Revision: snapshot.Revision(), MaxAllocatedVectorID: snapshot.MaxAllocatedVectorID(), NextComponentID: snapshot.NextComponentID(), Segments: stateSegments}
	data, ref, err := semanticformat.EncodeState(value, codecLimits(limits))
	return data, persistReference(ref), mapCodecError(err)
}

func decodeState(data []byte, limits Limits) (decodedState, error) {
	value, err := semanticformat.DecodeState(data, codecLimits(limits))
	if err != nil {
		return decodedState{}, mapCodecError(err)
	}
	segments := make([]decodedStateSegment, len(value.Segments))
	for i, segment := range value.Segments {
		segments[i] = decodedStateSegment{ComponentID: segment.ComponentID, Rows: segment.Rows, LivenessWords: segment.LivenessWords}
	}
	return decodedState{Config: value.Config, Revision: value.Revision, MaxAllocatedVectorID: value.MaxAllocatedVectorID, NextComponentID: value.NextComponentID, Segments: segments}, nil
}
