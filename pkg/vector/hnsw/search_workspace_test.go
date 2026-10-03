package hnsw

import "testing"

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
	if workspace.dense {
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
		dense:         true,
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
