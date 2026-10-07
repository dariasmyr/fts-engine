package hnsw

import "testing"

func (w *searchWorkspace) reset(nodeCount, dimensions, visitLimit, efSearch int) {
	w.prepareQuery(dimensions)
	w.resetSearch(nodeCount, dimensions, visitLimit, efSearch)
}

func TestSearchWorkspaceDenseEpochReset(t *testing.T) {
	workspace := &searchWorkspace{}
	workspace.reset(16, 4, 16, 8)
	candidate := searchCandidate{node: 7, distance: 1}
	workspace.storeCandidate(candidate)
	if got, ok := workspace.candidate(candidate.node); !ok || got != candidate {
		t.Fatalf("candidate = (%+v, %t), want (%+v, true)", got, ok, candidate)
	}
	if !workspace.markSeen(candidate.node) || workspace.markSeen(candidate.node) {
		t.Fatal("dense seen marker did not reject a duplicate")
	}

	workspace.reset(16, 4, 16, 8)
	if _, ok := workspace.candidate(candidate.node); ok {
		t.Fatal("candidate survived workspace epoch reset")
	}
	if !workspace.markSeen(candidate.node) {
		t.Fatal("seen marker survived workspace epoch reset")
	}
}

func TestSearchWorkspaceUsesSparseStateForLargeIndexes(t *testing.T) {
	workspace := &searchWorkspace{}
	workspace.reset(maxDenseSearchNodes+1, 4, 1_000, 64)
	if workspace.nodes.dense {
		t.Fatal("large workspace used dense node state")
	}
	candidate := searchCandidate{node: maxDenseSearchNodes, distance: 1}
	workspace.storeCandidate(candidate)
	if got, ok := workspace.candidate(candidate.node); !ok || got != candidate {
		t.Fatalf("candidate = (%+v, %t), want (%+v, true)", got, ok, candidate)
	}
	if !workspace.markSeen(candidate.node) || workspace.markSeen(candidate.node) {
		t.Fatal("sparse seen marker did not reject a duplicate")
	}
}

func TestSearchWorkspaceUsesSparseStateForSmallVisitBudget(t *testing.T) {
	workspace := &searchWorkspace{}
	workspace.reset(100_000, 4, 100, 64)
	if workspace.nodes.dense {
		t.Fatal("workspace used dense state for a small touched-row budget")
	}
}

func TestSparseSearchWorkspaceRejectsOversizedHeapBuffers(t *testing.T) {
	workspace := &searchWorkspace{
		frontier: make([]searchCandidate, 0, maxPooledSparseSearchRows+1),
	}
	if workspace.retainable() {
		t.Fatal("workspace with oversized frontier was retained")
	}
	workspace.frontier = nil
	workspace.results = make([]searchCandidate, 0, maxPooledSparseSearchRows+1)
	if workspace.retainable() {
		t.Fatal("workspace with oversized results was retained")
	}
}

func TestSearchWorkspaceRejectsOversizedVectorBuffers(t *testing.T) {
	workspace := &searchWorkspace{
		nodes:         nodeSearchState{dense: true},
		preparedQuery: make([]float32, 0, maxPooledSearchDimensions+1),
	}
	if workspace.retainable() {
		t.Fatal("workspace with oversized prepared query was retained")
	}
	workspace.preparedQuery = nil
	workspace.vector = make([]float32, 0, maxPooledSearchDimensions+1)
	if workspace.retainable() {
		t.Fatal("workspace with oversized vector scratch was retained")
	}
}

func TestSearchWorkspaceDropsInactiveRepresentations(t *testing.T) {
	workspace := &searchWorkspace{}
	workspace.reset(16, 4, 16, 8)
	if workspace.nodes.candidates == nil {
		t.Fatal("dense workspace has no dense candidate storage")
	}
	workspace.reset(maxDenseSearchNodes+1, 4, 100, 8)
	if workspace.nodes.candidates != nil || workspace.nodes.candidateEpoch != nil || workspace.nodes.seenEpoch != nil {
		t.Fatal("sparse workspace retained inactive dense storage")
	}
	workspace.reset(16, 4, 16, 8)
	if workspace.nodes.sparseCandidates != nil || workspace.nodes.sparseSeen != nil {
		t.Fatal("dense workspace retained inactive sparse storage")
	}
}

func TestSearchWorkspaceDropsOversizedSparseMaps(t *testing.T) {
	workspace := &searchWorkspace{}
	workspace.reset(maxDenseSearchNodes+1, 4, maxPooledSparseSearchRows+1, 8)
	for i := range maxPooledSparseSearchRows + 1 {
		node := nodeOrdinal(i)
		workspace.storeCandidate(searchCandidate{node: node})
		workspace.markSeen(node)
	}

	workspace.prepareForPool()
	if workspace.nodes.sparseCandidates != nil || workspace.nodes.sparseSeen != nil {
		t.Fatal("oversized sparse maps were retained")
	}
}

func TestSearchWorkspaceRetainsBoundedSparseMaps(t *testing.T) {
	workspace := &searchWorkspace{}
	workspace.reset(maxDenseSearchNodes+1, 4, 100, 8)
	workspace.storeCandidate(searchCandidate{node: 1})
	workspace.markSeen(1)
	candidates := workspace.nodes.sparseCandidates
	seen := workspace.nodes.sparseSeen

	workspace.prepareForPool()
	if candidates == nil || seen == nil || workspace.nodes.sparseCandidates == nil || workspace.nodes.sparseSeen == nil {
		t.Fatal("bounded sparse maps were dropped")
	}
	if len(workspace.nodes.sparseCandidates) != 0 || len(workspace.nodes.sparseSeen) != 0 {
		t.Fatal("bounded sparse maps were not cleared")
	}
}
