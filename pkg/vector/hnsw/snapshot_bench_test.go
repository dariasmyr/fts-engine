package hnsw

import (
	"testing"

	"github.com/dariasmyr/fts-engine/v2/internal/hnswmodel"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

const snapshotBenchmarkNodes = 100_000

var (
	snapshotBenchmarkSnapshot hnswmodel.Snapshot
	snapshotBenchmarkStats    TopologyStats
)

func BenchmarkTopologyInspection(b *testing.B) {
	index := newSnapshotBenchmarkIndex(b, snapshotBenchmarkNodes)

	b.Run("Snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			snapshotBenchmarkSnapshot = Snapshot(index)
		}
	})
	b.Run("Stats", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			snapshotBenchmarkStats = index.TopologyStats()
		}
	})
}

func newSnapshotBenchmarkIndex(b *testing.B, nodes int) *Index {
	b.Helper()
	space, err := vector.NewSpace(16, vector.MetricL2Squared)
	if err != nil {
		b.Fatal(err)
	}
	upperLayers := nodes / 16
	return &Index{topology: topology{
		space: space,
		build: buildInfo{
			maxNeighbors:          16,
			levelZeroMaxNeighbors: 32,
			efConstruction:        64,
			seed:                  7,
		},
		levels:           make([]uint8, nodes),
		level0Offsets:    make([]uint32, nodes+1),
		level0Neighbors:  make([]nodeOrdinal, nodes*16),
		upperNodeOffsets: make([]uint32, nodes+1),
		upperLinkOffsets: make([]uint32, upperLayers+1),
		upperNeighbors:   make([]nodeOrdinal, upperLayers*8),
	}}
}
