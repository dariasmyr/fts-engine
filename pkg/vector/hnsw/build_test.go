package hnsw

import (
	"context"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/internal/hnswmodel"
	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestBuildProducesDeterministicValidTopology(t *testing.T) {
	t.Parallel()

	values := make([][]float32, 32)
	for i := range values {
		values[i] = []float32{float32(i % 8), float32(i / 8)}
	}
	space, err := vector.NewSpace(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.New(space, values)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 4, EfConstruction: 32, Seed: 7},
		Search: SearchConfig{EfSearch: 16, VisitLimit: len(values)},
	}

	first, err := Build(context.Background(), store, config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(context.Background(), store, config)
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshot := Snapshot(first)
	if secondSnapshot := Snapshot(second); !reflect.DeepEqual(secondSnapshot, firstSnapshot) {
		t.Fatal("Build() topology differs for identical source, config, and seed")
	}
	assertTopologyInvariants(t, firstSnapshot)
}

func assertTopologyInvariants(t *testing.T, snapshot hnswmodel.Snapshot) {
	t.Helper()

	nodes := len(snapshot.Levels)
	if nodes == 0 || !snapshot.HasEntry || int(snapshot.Entry) >= nodes {
		t.Fatalf("entry = (%d, %t), nodes = %d", snapshot.Entry, snapshot.HasEntry, nodes)
	}
	if len(snapshot.Level0Offsets) != nodes+1 || len(snapshot.UpperNodeOffsets) != nodes+1 || len(snapshot.UpperLinkOffsets) == 0 {
		t.Fatalf(
			"offset lengths = level0:%d upper-nodes:%d upper-links:%d, nodes = %d",
			len(snapshot.Level0Offsets),
			len(snapshot.UpperNodeOffsets),
			len(snapshot.UpperLinkOffsets),
			nodes,
		)
	}
	if snapshot.Level0Offsets[0] != 0 || int(snapshot.Level0Offsets[nodes]) != len(snapshot.Level0Links) {
		t.Fatal("level-0 offsets do not cover level-0 links")
	}
	if snapshot.UpperNodeOffsets[0] != 0 || int(snapshot.UpperLinkOffsets[len(snapshot.UpperLinkOffsets)-1]) != len(snapshot.UpperLinks) {
		t.Fatal("upper offsets do not cover upper links")
	}

	maxLevel := uint8(0)
	placements := 0
	for node, level := range snapshot.Levels {
		maxLevel = max(maxLevel, level)
		if int(snapshot.UpperNodeOffsets[node]) != placements {
			t.Fatalf("node %d upper offset = %d, want %d", node, snapshot.UpperNodeOffsets[node], placements)
		}
		placements += int(level)
		assertNeighborList(t, snapshot, node, 0)
		for upperLevel := 1; upperLevel <= int(level); upperLevel++ {
			assertNeighborList(t, snapshot, node, upperLevel)
		}
	}
	if int(snapshot.UpperNodeOffsets[nodes]) != placements || len(snapshot.UpperLinkOffsets) != placements+1 {
		t.Fatalf("upper placements = %d, offsets = %d", placements, len(snapshot.UpperLinkOffsets))
	}
	if snapshot.Levels[snapshot.Entry] != maxLevel {
		t.Fatalf("entry level = %d, max level = %d", snapshot.Levels[snapshot.Entry], maxLevel)
	}
	assertLevel0Connected(t, snapshot)
}

func assertNeighborList(t *testing.T, snapshot hnswmodel.Snapshot, node, level int) {
	t.Helper()

	var neighbors []uint32
	limit := snapshot.MaxNeighbors
	if level == 0 {
		start := snapshot.Level0Offsets[node]
		end := snapshot.Level0Offsets[node+1]
		if end < start || int(end) > len(snapshot.Level0Links) {
			t.Fatalf("node %d has invalid level-0 range [%d:%d]", node, start, end)
		}
		neighbors = snapshot.Level0Links[start:end]
		limit = snapshot.LevelZeroMaxNeighbors
	} else {
		placement := snapshot.UpperNodeOffsets[node] + uint32(level-1)
		start := snapshot.UpperLinkOffsets[placement]
		end := snapshot.UpperLinkOffsets[placement+1]
		if end < start || int(end) > len(snapshot.UpperLinks) {
			t.Fatalf("node %d has invalid level-%d range [%d:%d]", node, level, start, end)
		}
		neighbors = snapshot.UpperLinks[start:end]
	}
	if len(neighbors) > limit {
		t.Fatalf("node %d level %d degree = %d, limit = %d", node, level, len(neighbors), limit)
	}
	seen := make(map[uint32]struct{}, len(neighbors))
	for _, neighbor := range neighbors {
		if int(neighbor) >= len(snapshot.Levels) || int(neighbor) == node || int(snapshot.Levels[neighbor]) < level {
			t.Fatalf("node %d level %d has invalid neighbor %d", node, level, neighbor)
		}
		if _, exists := seen[neighbor]; exists {
			t.Fatalf("node %d level %d has duplicate neighbor %d", node, level, neighbor)
		}
		seen[neighbor] = struct{}{}
	}
}

func assertLevel0Connected(t *testing.T, snapshot hnswmodel.Snapshot) {
	t.Helper()

	reachable := make([]bool, len(snapshot.Levels))
	reachable[snapshot.Entry] = true
	queue := []uint32{snapshot.Entry}
	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		start := snapshot.Level0Offsets[node]
		end := snapshot.Level0Offsets[node+1]
		for _, neighbor := range snapshot.Level0Links[start:end] {
			if !reachable[neighbor] {
				reachable[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	if visited != len(snapshot.Levels) {
		t.Fatalf("level-0 reachable nodes = %d, want %d", visited, len(snapshot.Levels))
	}
}
