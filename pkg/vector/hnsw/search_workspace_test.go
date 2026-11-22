package hnsw

import (
	"math"
	"testing"
)

func TestSearchWorkspaceDenseResetDoesNotLeakState(t *testing.T) {
	t.Parallel()

	workspace := &searchWorkspace{}
	workspace.resetSearch(32, 2, 32, 8)
	if !workspace.nodes.dense {
		t.Fatal("workspace selected sparse state, want dense")
	}
	workspace.storeCandidate(candidate{node: 7, distance: 1})
	if !workspace.markSeen(7) {
		t.Fatal("first markSeen() = false")
	}

	workspace.prepareForPool()
	workspace.resetSearch(32, 2, 32, 8)
	if _, exists := workspace.candidate(7); exists {
		t.Fatal("candidate from previous search survived reset")
	}
	if !workspace.markSeen(7) {
		t.Fatal("seen marker from previous search survived reset")
	}
}

func TestSearchWorkspaceDenseEpochRolloverClearsState(t *testing.T) {
	t.Parallel()

	workspace := &searchWorkspace{}
	workspace.resetSearch(8, 1, 8, 4)
	workspace.nodes.epoch = math.MaxUint32
	workspace.nodes.candidateEpoch[3] = 1
	workspace.nodes.seenEpoch[3] = 1

	workspace.resetSearch(8, 1, 8, 4)
	if workspace.nodes.epoch != 1 {
		t.Fatalf("epoch = %d, want 1", workspace.nodes.epoch)
	}
	if _, exists := workspace.candidate(3); exists {
		t.Fatal("candidate survived epoch rollover")
	}
	if !workspace.markSeen(3) {
		t.Fatal("seen marker survived epoch rollover")
	}
}

func TestSearchWorkspaceSparseResetDoesNotLeakState(t *testing.T) {
	t.Parallel()

	workspace := &searchWorkspace{}
	workspace.resetSearch(maxDenseSearchNodes+1, 2, 8, 4)
	if workspace.nodes.dense {
		t.Fatal("workspace selected dense state, want sparse")
	}
	workspace.storeCandidate(candidate{node: 7, distance: 1})
	if !workspace.markSeen(7) {
		t.Fatal("first markSeen() = false")
	}
	workspace.prepareForPool()
	workspace.resetSearch(maxDenseSearchNodes+1, 2, 8, 4)
	if _, exists := workspace.candidate(7); exists {
		t.Fatal("candidate from previous search survived reuse")
	}
	if !workspace.markSeen(7) {
		t.Fatal("seen marker from previous search survived reuse")
	}
}
