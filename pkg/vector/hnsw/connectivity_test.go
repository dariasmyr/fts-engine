package hnsw

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestRepairLevel0ConnectivityLeavesConnectedGraphUnchanged(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(t, []float32{0, 1, 2}, [][]nodeOrdinal{{1}, {0, 2}, {1}}, 2)
	before := cloneLevel0Links(b.graph)
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := b.repairLevel0Connectivity(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if stats.addedLinks != 0 || stats.replacedLinks != 0 {
		t.Fatalf("repair stats = %+v, want no changes", stats)
	}
	if got := cloneLevel0Links(b.graph); !reflect.DeepEqual(got, before) {
		t.Fatalf("connected topology changed: got %v, want %v", got, before)
	}
}

func TestReversePruningCanRemoveLastReachableIncomingLink(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(
		t,
		[]float32{0, 100, 1, 2, 3, 4},
		[][]nodeOrdinal{{1, 2, 3, 4}, {0}, {0}, {0}, {0}, nil},
		2,
	)
	if err := b.addReverseLink(t.Context(), 0, 5, 0); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(b.graph.nodes[0].links[0], nodeOrdinal(1)) {
		t.Fatal("reverse pruning retained the deliberately distant link 0 -> 1")
	}
	if !slices.Contains(b.graph.nodes[1].links[0], nodeOrdinal(0)) {
		t.Fatal("test setup lost the opposite directed link 1 -> 0")
	}
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	if state.isReachable[1] {
		t.Fatal("node 1 remains reachable after its last reachable incoming link was pruned")
	}
	if _, err := b.repairLevel0Connectivity(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	assertMutableGraphReachable(t, b.graph)
}

func TestStandardConstructionRepairsOnlyObservedDisconnection(t *testing.T) {
	t.Parallel()

	const vectorCount = 2048
	space, err := vector.NewSpace(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	b := newBuilder(
		BuildConfig{MaxNeighbors: 2, EfConstruction: 16, Seed: 1},
		space,
		vectorCount,
		vectorCount*space.Dimensions(),
	)
	for range vectorCount {
		if err := b.add(t.Context(), []float32{1, 1}); err != nil {
			t.Fatal(err)
		}
	}
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.unreachableNodes) == 0 {
		t.Fatal("test setup did not reproduce reverse-pruning disconnection")
	}
	stats, err := b.repairLevel0Connectivity(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if stats.initiallyUnreachable != len(state.unreachableNodes) || stats.addedLinks == 0 {
		t.Fatalf("repair stats = %+v, initial unreachable = %d", stats, len(state.unreachableNodes))
	}
	t.Logf("initial unreachable=%d, added links=%d, replaced links=%d", stats.initiallyUnreachable, stats.addedLinks, stats.replacedLinks)
	assertMutableGraphReachable(t, b.graph)
}

func TestRepairLevel0ConnectivityConnectsDirectedComponent(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(
		t,
		[]float32{0, 1, 10, 11, 12},
		[][]nodeOrdinal{{1}, {0}, {3}, {4}, {2}},
		2,
	)
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state.unreachableNodes, []nodeOrdinal{2, 3, 4}) {
		t.Fatalf("unreachable = %v, want [2 3 4]", state.unreachableNodes)
	}
	stats, err := b.repairLevel0Connectivity(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if stats.addedLinks != 1 {
		t.Fatalf("repair links = %d, want 1", stats.addedLinks)
	}
	assertMutableGraphReachable(t, b.graph)
}

func TestRepairLevel0ConnectivityConnectsMultipleComponents(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(
		t,
		[]float32{0, 1, 10, 11, 20, 21},
		[][]nodeOrdinal{{1}, {0}, {3}, {2}, {5}, {4}},
		2,
	)
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := b.repairLevel0Connectivity(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if stats.addedLinks != 2 {
		t.Fatalf("repair links = %d, want 2", stats.addedLinks)
	}
	assertMutableGraphReachable(t, b.graph)
}

func TestRepairLevel0ConnectivitySkipsFullOwnerWithoutSafeEdge(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(
		t,
		[]float32{0, 2, 3, 4, 5, 0.1},
		[][]nodeOrdinal{{1, 2, 3, 4}, nil, nil, nil, nil, nil},
		2,
	)
	before := slices.Clone(b.graph.nodes[0].links[0])
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.repairLevel0Connectivity(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(b.graph.nodes[0].links[0], before) {
		t.Fatalf("full owner tree links changed: got %v, want %v", b.graph.nodes[0].links[0], before)
	}
	found := false
	for owner := 1; owner <= 4; owner++ {
		found = found || slices.Contains(b.graph.nodes[owner].links[0], nodeOrdinal(5))
	}
	if !found {
		t.Fatal("repair did not use another reachable owner")
	}
	assertMutableGraphReachable(t, b.graph)
}

func TestRepairLevel0ConnectivityReplacesOnlyNonTreeEdge(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(
		t,
		[]float32{10, 0, 1, 2, 3, 0.1},
		[][]nodeOrdinal{{1}, {2, 3, 4, 0}, nil, nil, nil, nil},
		2,
	)
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := b.repairLevel0Connectivity(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	if stats.replacedLinks != 1 {
		t.Fatalf("repair replacements = %d, want 1", stats.replacedLinks)
	}
	for _, node := range []nodeOrdinal{2, 3, 4} {
		if !slices.Contains(b.graph.nodes[1].links[0], node) {
			t.Fatalf("repair removed reachability-tree edge 1 -> %d", node)
		}
	}
	assertMutableGraphReachable(t, b.graph)
}

func TestRepairLevel0ConnectivityReturnsErrorWithoutSafeCapacity(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(t, []float32{0, 1}, [][]nodeOrdinal{nil, nil}, 0)
	b.config.EfConstruction = 1
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.repairLevel0Connectivity(t.Context(), state)
	if !errors.Is(err, errConnectivityRepair) {
		t.Fatalf("repair error = %v, want %v", err, errConnectivityRepair)
	}
}

func TestLevel0ConnectivityHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	b := newConnectivityTestBuilder(t, []float32{0, 1}, [][]nodeOrdinal{{1}, {0}}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := validateLevel0Reachability(ctx, b.graph); !errors.Is(err, context.Canceled) {
		t.Fatalf("validation error = %v, want context.Canceled", err)
	}

	b = newConnectivityTestBuilder(t, []float32{0, 1}, [][]nodeOrdinal{nil, nil}, 2)
	state, err := validateLevel0Reachability(t.Context(), b.graph)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.repairLevel0Connectivity(ctx, state); !errors.Is(err, context.Canceled) {
		t.Fatalf("repair error = %v, want context.Canceled", err)
	}
}

func TestBuildHandlesEmptyAndSingleNodeConnectivity(t *testing.T) {
	t.Parallel()

	space, err := vector.NewSpace(1, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		Build:  BuildConfig{MaxNeighbors: 2, EfConstruction: 4, Seed: 1},
		Search: SearchConfig{EfSearch: 4, VisitLimit: 4},
	}
	for _, values := range [][]float32{nil, {1}} {
		store, err := memorystore.NewPrepared(space, values)
		if err != nil {
			t.Fatal(err)
		}
		index, err := Build(t.Context(), store, config)
		if err != nil {
			t.Fatal(err)
		}
		if index.Len() != len(values) {
			t.Fatalf("Build() len = %d, want %d", index.Len(), len(values))
		}
	}
}

func newConnectivityTestBuilder(
	t testing.TB,
	values []float32,
	links [][]nodeOrdinal,
	maxNeighbors int,
) *builder {
	t.Helper()
	space, err := vector.NewSpace(1, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	b := newBuilder(
		BuildConfig{MaxNeighbors: maxNeighbors, EfConstruction: max(1, maxNeighbors), Seed: 1},
		space,
		len(values),
		len(values),
	)
	copy(b.graph.values, values)
	for _, nodeLinks := range links {
		b.graph.nodes = append(b.graph.nodes, mutableNode{
			links: [][]nodeOrdinal{slices.Clone(nodeLinks)},
		})
	}
	b.graph.entry = 0
	b.graph.hasEntry = len(values) > 0
	return b
}

func cloneLevel0Links(graph graphData) [][]nodeOrdinal {
	result := make([][]nodeOrdinal, len(graph.nodes))
	for node := range graph.nodes {
		result[node] = slices.Clone(graph.nodes[node].links[0])
	}
	return result
}

func assertMutableGraphReachable(t *testing.T, graph graphData) {
	t.Helper()
	state, err := validateLevel0Reachability(t.Context(), graph)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.unreachableNodes) != 0 {
		t.Fatalf("unreachable nodes after repair: %v", state.unreachableNodes)
	}
}
