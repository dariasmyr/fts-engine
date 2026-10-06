package hnsw

import "sync"

const (
	maxDenseSearchNodes       = 1 << 18
	minDenseSearchNodes       = 1 << 12
	denseSearchVisitRatio     = 8
	maxPooledSparseSearchRows = 1 << 16
	maxPooledSearchDimensions = 1 << 16
	sparseSearchInitialRows   = 256
)

type searchWorkspacePool struct {
	pool sync.Pool
}

type searchWorkspace struct {
	dense bool
	epoch uint32

	candidates     []searchCandidate
	candidateEpoch []uint32
	seenEpoch      []uint32

	sparseCandidates map[nodeOrdinal]searchCandidate
	sparseSeen       map[nodeOrdinal]struct{}

	scoredNodes   []nodeOrdinal
	preparedQuery []float32
	vector        []float32
	frontier      []searchCandidate
	results       []searchCandidate
	trackScored   bool
}

func newSearchWorkspacePool() *searchWorkspacePool {
	return &searchWorkspacePool{}
}

func (p *searchWorkspacePool) acquire() *searchWorkspace {
	var workspace *searchWorkspace
	if p != nil {
		workspace, _ = p.pool.Get().(*searchWorkspace)
	}
	if workspace == nil {
		workspace = &searchWorkspace{}
	}
	return workspace
}

func (p *searchWorkspacePool) release(workspace *searchWorkspace) {
	if p == nil || workspace == nil {
		return
	}
	workspace.prepareForPool()
	if !workspace.retainable() {
		return
	}
	p.pool.Put(workspace)
}

func (w *searchWorkspace) reset(nodeCount, dimensions, visitLimit, efSearch int) {
	w.prepareQuery(dimensions)
	w.resetSearch(nodeCount, dimensions, visitLimit, efSearch)
}

func (w *searchWorkspace) prepareQuery(dimensions int) {
	w.preparedQuery = resetFloat32s(w.preparedQuery, dimensions)
}

func (w *searchWorkspace) resetSearch(nodeCount, dimensions, visitLimit, efSearch int) {
	w.scoredNodes = w.scoredNodes[:0]
	w.trackScored = true
	w.frontier = resetSearchCandidates(w.frontier, min(nodeCount, efSearch))
	w.results = resetSearchCandidates(w.results, min(nodeCount, efSearch))
	w.vector = resetFloat32s(w.vector, dimensions)

	denseLimit := maxDenseSearchNodes
	if visitLimit < maxDenseSearchNodes/denseSearchVisitRatio {
		denseLimit = max(minDenseSearchNodes, visitLimit*denseSearchVisitRatio)
	}
	if nodeCount <= maxDenseSearchNodes && nodeCount <= denseLimit {
		w.dense = true
		w.sparseCandidates = nil
		w.sparseSeen = nil
		w.candidates = resetCandidateSlots(w.candidates, nodeCount)
		w.candidateEpoch = resetUint32s(w.candidateEpoch, nodeCount)
		w.seenEpoch = resetUint32s(w.seenEpoch, nodeCount)
		w.epoch++
		if w.epoch == 0 {
			clear(w.candidateEpoch)
			clear(w.seenEpoch)
			w.epoch = 1
		}
		return
	}

	w.dense = false
	w.candidates = nil
	w.candidateEpoch = nil
	w.seenEpoch = nil
	initialRows := min(nodeCount, visitLimit, sparseSearchInitialRows)
	if w.sparseCandidates == nil {
		w.sparseCandidates = make(map[nodeOrdinal]searchCandidate, initialRows)
	} else {
		clear(w.sparseCandidates)
	}
	if w.sparseSeen == nil {
		w.sparseSeen = make(map[nodeOrdinal]struct{}, initialRows)
	} else {
		clear(w.sparseSeen)
	}
}

func (w *searchWorkspace) candidate(node nodeOrdinal) (searchCandidate, bool) {
	if w.dense {
		if uint64(node) >= uint64(len(w.candidates)) || w.candidateEpoch[node] != w.epoch {
			return searchCandidate{}, false
		}
		return w.candidates[node], true
	}
	candidate, ok := w.sparseCandidates[node]
	return candidate, ok
}

func (w *searchWorkspace) storeCandidate(candidate searchCandidate) {
	if w.dense {
		w.candidates[candidate.node] = candidate
		w.candidateEpoch[candidate.node] = w.epoch
	} else {
		w.sparseCandidates[candidate.node] = candidate
	}
	if w.trackScored {
		w.scoredNodes = append(w.scoredNodes, candidate.node)
	}
}

func (w *searchWorkspace) stopTrackingScored() {
	w.trackScored = false
	w.scoredNodes = w.scoredNodes[:0]
}

func (w *searchWorkspace) markSeen(node nodeOrdinal) bool {
	if w.dense {
		if w.seenEpoch[node] == w.epoch {
			return false
		}
		w.seenEpoch[node] = w.epoch
		return true
	}
	if _, exists := w.sparseSeen[node]; exists {
		return false
	}
	w.sparseSeen[node] = struct{}{}
	return true
}

func (w *searchWorkspace) retainable() bool {
	if cap(w.preparedQuery) > maxPooledSearchDimensions || cap(w.vector) > maxPooledSearchDimensions {
		return false
	}
	if w.dense {
		return true
	}
	return cap(w.scoredNodes) <= maxPooledSparseSearchRows &&
		cap(w.frontier) <= maxPooledSparseSearchRows &&
		cap(w.results) <= maxPooledSparseSearchRows
}

func (w *searchWorkspace) prepareForPool() {
	w.scoredNodes = w.scoredNodes[:0]
	w.frontier = w.frontier[:0]
	w.results = w.results[:0]
	if w.dense {
		w.sparseCandidates = nil
		w.sparseSeen = nil
		return
	}
	w.candidates = nil
	w.candidateEpoch = nil
	w.seenEpoch = nil
	if len(w.sparseCandidates) > maxPooledSparseSearchRows {
		w.sparseCandidates = nil
	} else {
		clear(w.sparseCandidates)
	}
	if len(w.sparseSeen) > maxPooledSparseSearchRows {
		w.sparseSeen = nil
	} else {
		clear(w.sparseSeen)
	}
}

func resetSearchCandidates(values []searchCandidate, capacity int) []searchCandidate {
	if cap(values) < capacity {
		return make([]searchCandidate, 0, capacity)
	}
	return values[:0]
}

func resetCandidateSlots(values []searchCandidate, length int) []searchCandidate {
	if cap(values) < length {
		return make([]searchCandidate, length)
	}
	return values[:length]
}

func resetUint32s(values []uint32, length int) []uint32 {
	if cap(values) < length {
		return make([]uint32, length)
	}
	return values[:length]
}

func resetFloat32s(values []float32, length int) []float32 {
	if cap(values) < length {
		return make([]float32, length)
	}
	return values[:length]
}
