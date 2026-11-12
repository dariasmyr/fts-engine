package hnsw

import (
	"fmt"
	"testing"
	"unsafe"
)

func BenchmarkSearchWorkspacePool(b *testing.B) {
	tests := []struct {
		name       string
		nodes      int
		visitLimit int
	}{
		{name: "dense-small", nodes: 1 << 12, visitLimit: 1 << 12},
		{name: "dense-threshold", nodes: maxDenseSearchNodes, visitLimit: maxDenseSearchNodes},
		{name: "sparse-by-visit", nodes: 1 << 17, visitLimit: 256},
		{name: "sparse-by-size", nodes: maxDenseSearchNodes + 1, visitLimit: 1 << 12},
	}
	for _, test := range tests {
		b.Run(fmt.Sprintf("%s/nodes=%d/visits=%d", test.name, test.nodes, test.visitLimit), func(b *testing.B) {
			pool := newSearchWorkspacePool()
			var retainedBytes uintptr
			b.ReportAllocs()
			for b.Loop() {
				workspace := pool.acquire()
				workspace.resetSearch(test.nodes, 384, test.visitLimit, 64)
				retainedBytes = benchmarkWorkspaceRetainedBytes(workspace)
				pool.release(workspace)
			}
			b.ReportMetric(float64(test.nodes), "nodes")
			b.ReportMetric(float64(retainedBytes), "retained-bytes")
		})
	}
}

func benchmarkWorkspaceRetainedBytes(workspace *searchWorkspace) uintptr {
	return uintptr(cap(workspace.nodes.candidates))*unsafe.Sizeof(searchCandidate{}) +
		uintptr(cap(workspace.nodes.candidateEpoch))*unsafe.Sizeof(uint32(0)) +
		uintptr(cap(workspace.nodes.seenEpoch))*unsafe.Sizeof(uint32(0)) +
		uintptr(cap(workspace.scoredNodes))*unsafe.Sizeof(nodeOrdinal(0)) +
		uintptr(cap(workspace.preparedQuery))*unsafe.Sizeof(float32(0)) +
		uintptr(cap(workspace.vector))*unsafe.Sizeof(float32(0)) +
		uintptr(cap(workspace.frontier))*unsafe.Sizeof(searchCandidate{}) +
		uintptr(cap(workspace.results))*unsafe.Sizeof(searchCandidate{})
}
