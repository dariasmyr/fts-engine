package hnsw

type buildWorkspace struct {
	seenEpoch []uint32
	epoch     uint32

	frontier  []searchCandidate
	results   []searchCandidate
	entries   []nodeOrdinal
	neighbors []searchCandidate
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
