package semanticpersist

import (
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/format"
)

type committedSegment struct {
	data          semantic.SegmentData
	livenessWords []uint64
}

type committedState struct {
	config               semantic.Config
	revision             uint64
	maxAllocatedVectorID uint64
	nextComponentID      uint64
	segments             []committedSegment
	stateSegments        []format.StateSegment
	encodedState         []byte
	stateReference       fileReference
}

func captureCommittedState(state *semantic.CommittedState, limits Limits) (committedState, error) {
	segments := state.Segments()
	captured := committedState{
		config:               state.Config(),
		revision:             state.Revision(),
		maxAllocatedVectorID: state.MaxAllocatedVectorID(),
		nextComponentID:      state.NextComponentID(),
		segments:             make([]committedSegment, len(segments)),
		stateSegments:        make([]format.StateSegment, len(segments)),
	}
	for i, segment := range segments {
		data := segment.Data()
		livenessWords := segment.LivenessWords()
		captured.segments[i] = committedSegment{data: data, livenessWords: livenessWords}
		captured.stateSegments[i] = format.StateSegment{ComponentID: data.ComponentID, Rows: data.Rows, LivenessWords: livenessWords}
	}
	value := format.State{Config: captured.config, Revision: captured.revision, MaxAllocatedVectorID: captured.maxAllocatedVectorID, NextComponentID: captured.nextComponentID, Segments: captured.stateSegments}
	data, ref, err := format.EncodeState(value, codecLimits(limits))
	if err != nil {
		return committedState{}, mapCodecError(err)
	}
	captured.encodedState = data
	captured.stateReference = persistReference(ref)
	return captured, nil
}

func encodeState(state *semantic.CommittedState, limits Limits) ([]byte, fileReference, error) {
	captured, err := captureCommittedState(state, limits)
	if err != nil {
		return nil, fileReference{}, err
	}
	return captured.encodedState, captured.stateReference, nil
}

func decodeState(data []byte, limits Limits) (decodedState, error) {
	value, err := format.DecodeState(data, codecLimits(limits))
	if err != nil {
		return decodedState{}, mapCodecError(err)
	}
	segments := make([]decodedStateSegment, len(value.Segments))
	for i, segment := range value.Segments {
		segments[i] = decodedStateSegment{ComponentID: segment.ComponentID, Rows: segment.Rows, LivenessWords: segment.LivenessWords}
	}
	return decodedState{Config: value.Config, Revision: value.Revision, MaxAllocatedVectorID: value.MaxAllocatedVectorID, NextComponentID: value.NextComponentID, Segments: segments}, nil
}
