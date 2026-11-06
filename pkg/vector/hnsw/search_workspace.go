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
	nodes nodeSearchState

	scoredNodes   []nodeOrdinal
	preparedQuery []float32
	vector        []float32
	frontier      []searchCandidate
	results       []searchCandidate
	trackScored   bool
}

type nodeSearchState struct {
	dense bool
	epoch uint32

	candidates     []searchCandidate
	candidateEpoch []uint32
	seenEpoch      []uint32

	sparseCandidates map[nodeOrdinal]searchCandidate
	sparseSeen       map[nodeOrdinal]struct{}
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
		w.nodes.dense = true
		w.nodes.sparseCandidates = nil
		w.nodes.sparseSeen = nil
		w.nodes.candidates = resetCandidateSlots(w.nodes.candidates, nodeCount)
		w.nodes.candidateEpoch = resetUint32s(w.nodes.candidateEpoch, nodeCount)
		w.nodes.seenEpoch = resetUint32s(w.nodes.seenEpoch, nodeCount)
		w.nodes.epoch++
		if w.nodes.epoch == 0 {
			clear(w.nodes.candidateEpoch)
			clear(w.nodes.seenEpoch)
			w.nodes.epoch = 1
		}
		return
	}

	w.nodes.dense = false
	w.nodes.candidates = nil
	w.nodes.candidateEpoch = nil
	w.nodes.seenEpoch = nil
	initialRows := min(nodeCount, visitLimit, sparseSearchInitialRows)
	if w.nodes.sparseCandidates == nil {
		w.nodes.sparseCandidates = make(map[nodeOrdinal]searchCandidate, initialRows)
	} else {
		clear(w.nodes.sparseCandidates)
	}
	if w.nodes.sparseSeen == nil {
		w.nodes.sparseSeen = make(map[nodeOrdinal]struct{}, initialRows)
	} else {
		clear(w.nodes.sparseSeen)
	}
}

func (w *searchWorkspace) candidate(node nodeOrdinal) (searchCandidate, bool) {
	if w.nodes.dense {
		if uint64(node) >= uint64(len(w.nodes.candidates)) || w.nodes.candidateEpoch[node] != w.nodes.epoch {
			return searchCandidate{}, false
		}
		return w.nodes.candidates[node], true
	}
	candidate, ok := w.nodes.sparseCandidates[node]
	return candidate, ok
}

func (w *searchWorkspace) storeCandidate(candidate searchCandidate) {
	if w.nodes.dense {
		w.nodes.candidates[candidate.node] = candidate
		w.nodes.candidateEpoch[candidate.node] = w.nodes.epoch
	} else {
		w.nodes.sparseCandidates[candidate.node] = candidate
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
	if w.nodes.dense {
		if w.nodes.seenEpoch[node] == w.nodes.epoch {
			return false
		}
		w.nodes.seenEpoch[node] = w.nodes.epoch
		return true
	}
	if _, exists := w.nodes.sparseSeen[node]; exists {
		return false
	}
	w.nodes.sparseSeen[node] = struct{}{}
	return true
}

func (w *searchWorkspace) retainable() bool {
	if cap(w.preparedQuery) > maxPooledSearchDimensions || cap(w.vector) > maxPooledSearchDimensions {
		return false
	}
	if w.nodes.dense {
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
	if w.nodes.dense {
		w.nodes.sparseCandidates = nil
		w.nodes.sparseSeen = nil
		return
	}
	w.nodes.candidates = nil
	w.nodes.candidateEpoch = nil
	w.nodes.seenEpoch = nil
	if len(w.nodes.sparseCandidates) > maxPooledSparseSearchRows {
		w.nodes.sparseCandidates = nil
	} else {
		clear(w.nodes.sparseCandidates)
	}
	if len(w.nodes.sparseSeen) > maxPooledSparseSearchRows {
		w.nodes.sparseSeen = nil
	} else {
		clear(w.nodes.sparseSeen)
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
