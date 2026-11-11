package semantic

type candidatePreconditions struct {
	revision             Revision
	segmentID            SegmentID
	maxAllocatedVectorID VectorID
	base                 *IndexView
}

type stateCandidate struct {
	preconditions    candidatePreconditions
	view             *IndexView
	locations        map[VectorID]vectorLocation
	allocatedSegment bool
	resetPending     bool
}

// candidatePreconditionsLocked captures the mutable state that must still
// match when a prepared view is published. The caller must hold stateMu.
func (i *Index) candidatePreconditionsLocked() candidatePreconditions {
	return candidatePreconditions{
		revision:             i.state.revision,
		segmentID:            i.state.nextSegmentID,
		maxAllocatedVectorID: i.state.maxAllocatedVectorID,
		base:                 i.state.view,
	}
}

func (i *Index) validateCandidate(candidate stateCandidate) error {
	i.stateMu.RLock()
	defer i.stateMu.RUnlock()
	if candidate.preconditions.revision != i.state.revision ||
		candidate.preconditions.segmentID != i.state.nextSegmentID ||
		candidate.preconditions.maxAllocatedVectorID != i.state.maxAllocatedVectorID ||
		candidate.preconditions.base != i.state.view {
		return ErrInternalState
	}
	return nil
}

// publishToMemory is non-failing after validation while the caller continues
// to hold the mutation gate.
func (i *Index) publishToMemory(candidate stateCandidate) {
	i.stateMu.Lock()
	defer i.stateMu.Unlock()
	i.state.view = candidate.view
	i.state.locations = candidate.locations
	if candidate.resetPending {
		i.state.pending.reset()
	}
	if candidate.allocatedSegment {
		i.state.nextSegmentID++
	}
}
