package hnsw

type buildWorkspace struct {
	seenEpoch []uint32
	epoch     uint32

	pendingCandidates     []candidate
	nearestCandidates     []candidate
	levelStartCandidates  []candidate
	reverseLinkCandidates []candidate
	// rejectedNeighbors is selection-only scratch. Selected neighbors use a
	// caller-owned buffer because they become persistent graph links.
	rejectedNeighbors []buildNeighbor

	reverseDistanceChecks uint64
	reverseSkippedChecks  uint64
}

func (w *buildWorkspace) nextEpoch() {
	w.epoch++
	if w.epoch == 0 {
		clear(w.seenEpoch)
		w.epoch = 1
	}
}

func (w *buildWorkspace) markSeen(node nodeOrdinal) bool {
	if w.seenEpoch[node] == w.epoch {
		return false
	}
	w.seenEpoch[node] = w.epoch
	return true
}
