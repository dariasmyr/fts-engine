package hnsw

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func builderTestConfig(seed uint64, _ int) BuildConfig {
	return BuildConfig{
		MaxNeighbors:   2,
		EfConstruction: 8,
		Seed:           seed,
	}
}

func newTestBuilder(t *testing.T, seed uint64, count int) *builder {
	t.Helper()
	calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	return newBuilder(builderTestConfig(seed, count), calculator, count, count*2)
}

func addTestVector(t *testing.T, builder *builder, ordinal vector.Ordinal, value []float32) nodeOrdinal {
	t.Helper()
	if ordinal != vector.Ordinal(len(builder.graph.nodes)) {
		t.Fatalf("non-sequential ordinal %d at node %d", ordinal, len(builder.graph.nodes))
	}
	err := builder.add(value)
	if err != nil {
		t.Fatalf("add(%d, %v): %v", ordinal, value, err)
	}
	return nodeOrdinal(ordinal)
}

func freezeTestBuilder(t testing.TB, builder *builder) (*Index, error) {
	t.Helper()
	source, err := memorystore.NewPrepared(builder.calculator, builder.graph.values)
	if err != nil {
		t.Fatal(err)
	}
	return builder.freeze(source, readerTestSearchConfig())
}

func checkBuilder(builder *builder) (GraphStats, error) {
	graph := cloneGraphData(builder.graph)
	graph.values = graph.values[:len(graph.nodes)*builder.calculator.Dimensions()]
	return validateGraphData(builder.calculator, builder.config.info(), graph)
}

func cloneGraphData(graph graphData) graphData {
	clone := graphData{values: append([]float32(nil), graph.values...), entry: graph.entry, hasEntry: graph.hasEntry}
	clone.nodes = make([]mutableNode, len(graph.nodes))
	for nodeIndex, node := range graph.nodes {
		clone.nodes[nodeIndex] = mutableNode{level: node.level, links: make([][]nodeOrdinal, len(node.links))}
		for level, links := range node.links {
			clone.nodes[nodeIndex].links[level] = append([]nodeOrdinal(nil), links...)
		}
	}
	return clone
}

func readerTopology(t *testing.T, reader *Index) [][][]nodeOrdinal {
	t.Helper()
	topology := make([][][]nodeOrdinal, reader.Len())
	for node := range reader.Len() {
		level, ok := reader.nodeLevel(nodeOrdinal(node))
		if !ok {
			t.Fatalf("missing node %d", node)
		}
		topology[node] = make([][]nodeOrdinal, int(level)+1)
		for currentLevel := 0; currentLevel <= int(level); currentLevel++ {
			neighbors, ok := reader.neighbors(nodeOrdinal(node), currentLevel)
			if !ok {
				t.Fatalf("missing node %d level %d", node, currentLevel)
			}
			topology[node][currentLevel] = neighbors
		}
	}
	return topology
}

func TestBuildConfigValidation(t *testing.T) {
	valid := []BuildConfig{
		builderTestConfig(0, 1),
		{MaxNeighbors: MaxSupportedNeighbors, EfConstruction: MaxEfConstruction},
	}
	for i, config := range valid {
		if err := config.validate(); err != nil {
			t.Errorf("valid config %d: %v", i, err)
		}
	}

	invalid := []struct {
		name   string
		config BuildConfig
		want   error
	}{
		{"max neighbors below minimum", BuildConfig{MaxNeighbors: 1, EfConstruction: 2}, ErrInvalidBuildConfig},
		{"max neighbors above maximum", BuildConfig{MaxNeighbors: MaxSupportedNeighbors + 1, EfConstruction: MaxSupportedNeighbors + 1}, ErrInvalidBuildConfig},
		{"ef below max neighbors", BuildConfig{MaxNeighbors: 3, EfConstruction: 2}, ErrInvalidBuildConfig},
		{"ef above maximum", BuildConfig{MaxNeighbors: 2, EfConstruction: MaxEfConstruction + 1}, ErrInvalidBuildConfig},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.validate()
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	limits := BuildLimits{MaxVectors: 2, MaxVectorBytes: 8}
	if _, err := limits.validateVectorAllocation(2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := (BuildLimits{MaxVectors: 1, MaxVectorBytes: 4}).validateVectorAllocation(2, 1); !errors.Is(err, ErrInvalidBuildConfig) {
		t.Fatalf("count limit error = %v", err)
	}
	if _, err := limits.validateVectorAllocation(2, math.MaxInt); !errors.Is(err, ErrInvalidBuildConfig) {
		t.Fatalf("allocation overflow error = %v", err)
	}
}

func TestBuilderCheckAfterEveryInsertionAndEntryTransitions(t *testing.T) {
	builder := newTestBuilder(t, 0, 6)
	values := [][]float32{{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 0}, {5, 0}}
	wantLevels := []uint8{0, 1, 5, 0, 3, 1}
	wantEntries := []nodeOrdinal{0, 1, 2, 2, 2, 2}

	for i, value := range values {
		node := addTestVector(t, builder, vector.Ordinal(i), value)
		if node != nodeOrdinal(i) {
			t.Fatalf("insertion %d node = %d, want %d", i, node, i)
		}
		if builder.graph.nodes[node].level != wantLevels[i] || builder.graph.entry != wantEntries[i] {
			t.Fatalf("insertion %d level/entry = %d/%d, want %d/%d", i, builder.graph.nodes[node].level, builder.graph.entry, wantLevels[i], wantEntries[i])
		}
		stats, err := checkBuilder(builder)
		if err != nil {
			t.Fatalf("Check after insertion %d: %v", i, err)
		}
		if stats.NodeCount != i+1 || stats.VectorCount != i+1 || stats.ReachableNodes != i+1 || stats.UnreachableNodes != 0 || stats.MaxLevel != int(wantLevels[wantEntries[i]]) {
			t.Fatalf("Check after insertion %d stats = %+v", i, stats)
		}
	}
}

func TestBuilderCompleteFreeze(t *testing.T) {
	builder := newTestBuilder(t, 11, 3)
	addTestVector(t, builder, 0, []float32{0, 10})
	addTestVector(t, builder, 1, []float32{1, 15})
	addTestVector(t, builder, 2, []float32{2, 20})
	reader, err := freezeTestBuilder(t, builder)
	if err != nil {
		t.Fatal(err)
	}
	if reader.Len() != 3 || reader.Report().Build != builder.config.info() || reader.Dimensions() != 2 || reader.Metric() != vector.MetricL2Squared {
		t.Fatalf("complete reader metadata = len:%d info:%+v dimensions:%d metric:%v", reader.Len(), reader.Report().Build, reader.Dimensions(), reader.Metric())
	}
	wantInfo := BuildInfo{BuildVersion: 1, LevelGeneratorVersion: 1, MaxNeighbors: 2, LevelZeroMaxNeighbors: 4, EfConstruction: 8, Seed: 11}
	if got := reader.Report().Build; got != wantInfo {
		t.Fatalf("BuildInfo = %+v, want %+v", got, wantInfo)
	}
	for ordinal, want := range [][]float32{{0, 10}, {1, 15}, {2, 20}} {
		got, ok := readPreparedVector(reader.vectors, vector.Ordinal(ordinal))
		if !ok || !slices.Equal(got, want) {
			t.Errorf("Vector(%d) = (%v, %v), want (%v, true)", ordinal, got, ok, want)
		}
	}
	if stats := reader.Report().Graph; stats.NodeCount != 3 || stats.ReachableNodes != 3 || stats.UnreachableNodes != 0 {
		t.Fatalf("frozen stats = %+v", stats)
	}
}

func TestBuilderInvalidSequentialAddIsAtomic(t *testing.T) {
	builder := newTestBuilder(t, 0, 3)
	control := newTestBuilder(t, 0, 3)
	addTestVector(t, builder, 0, []float32{0, 0})
	addTestVector(t, control, 0, []float32{0, 0})

	tests := []struct {
		name  string
		value []float32
		want  error
	}{
		{"dimension mismatch", []float32{1}, vector.ErrDimensionMismatch},
		{"nan vector", []float32{float32(math.NaN()), 0}, vector.ErrNonFiniteVector},
		{"infinite vector", []float32{float32(math.Inf(1)), 0}, vector.ErrNonFiniteVector},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			beforeGraph := cloneGraphData(builder.graph)
			beforeRNG := builder.rng
			err := builder.add(test.value)
			if !errors.Is(err, test.want) {
				t.Fatalf("add error = %v, want %v", err, test.want)
			}
			if !reflect.DeepEqual(builder.graph, beforeGraph) || builder.rng != beforeRNG {
				t.Fatalf("failed add mutated builder: graph=%+v rng=%+v", builder.graph, builder.rng)
			}
		})
	}

	for ordinal, value := range [][]float32{{1, 0}, {2, 0}} {
		addTestVector(t, builder, vector.Ordinal(ordinal+1), value)
		addTestVector(t, control, vector.Ordinal(ordinal+1), value)
	}
	got, err := freezeTestBuilder(t, builder)
	if err != nil {
		t.Fatal(err)
	}
	want, err := freezeTestBuilder(t, control)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(readerTopology(t, got), readerTopology(t, want)) || !slices.Equal(got.topology.levels, want.topology.levels) || got.topology.entry != want.topology.entry {
		t.Fatal("failed additions changed later deterministic topology")
	}

	beforeGraph := cloneGraphData(builder.graph)
	beforeRNG := builder.rng
	if err := builder.add([]float32{0, 0}); !errors.Is(err, errCapacityExceeded) {
		t.Fatalf("full add error = %v, want ErrCapacityExceeded", err)
	}
	if !reflect.DeepEqual(builder.graph, beforeGraph) || builder.rng != beforeRNG {
		t.Fatal("capacity failure mutated builder")
	}
}

func TestBuilderMaxDegreesDuplicatesAndClusteredVectors(t *testing.T) {
	builder := newTestBuilder(t, 99, 48)
	for i := 0; i < 48; i++ {
		value := []float32{float32(i % 4), float32((i / 4) % 3)}
		addTestVector(t, builder, vector.Ordinal(i), value)
		if _, err := checkBuilder(builder); err != nil {
			t.Fatalf("Check after duplicate/clustered insertion %d: %v", i, err)
		}
	}
	reader, err := freezeTestBuilder(t, builder)
	if err != nil {
		t.Fatal(err)
	}
	sawFullLevel0 := false
	sawFullUpper := false
	for node := 0; node < reader.Len(); node++ {
		level, _ := reader.nodeLevel(nodeOrdinal(node))
		for currentLevel := 0; currentLevel <= int(level); currentLevel++ {
			neighbors, ok := reader.neighbors(nodeOrdinal(node), currentLevel)
			if !ok {
				t.Fatalf("missing node %d level %d", node, currentLevel)
			}
			limit := reader.Report().Build.neighborLimit(currentLevel)
			if len(neighbors) > limit {
				t.Errorf("node %d level %d degree = %d, max %d", node, currentLevel, len(neighbors), limit)
			}
			if len(neighbors) == limit && currentLevel == 0 {
				sawFullLevel0 = true
			}
			if len(neighbors) == limit && currentLevel > 0 {
				sawFullUpper = true
			}
			seen := make(map[nodeOrdinal]bool, len(neighbors))
			for _, neighbor := range neighbors {
				if neighbor == nodeOrdinal(node) || seen[neighbor] {
					t.Errorf("node %d level %d invalid neighbors %v", node, currentLevel, neighbors)
				}
				seen[neighbor] = true
			}
		}
	}
	if !sawFullLevel0 || !sawFullUpper {
		t.Fatalf("degree limits were not exercised: level0=%v upper=%v", sawFullLevel0, sawFullUpper)
	}
}

func TestBuilderFixedSeedTopologyDeterminism(t *testing.T) {
	values := [][]float32{{0, 0}, {1, 0}, {0, 1}, {1, 1}, {2, 0}, {0, 2}, {2, 2}, {1, 2}, {2, 1}}
	build := func() *Index {
		builder := newTestBuilder(t, 0x12345678, len(values))
		for ordinal, value := range values {
			addTestVector(t, builder, vector.Ordinal(ordinal), value)
		}
		reader, err := freezeTestBuilder(t, builder)
		if err != nil {
			t.Fatal(err)
		}
		return reader
	}

	first := build()
	second := build()
	if first.topology.entry != second.topology.entry || !slices.Equal(first.topology.levels, second.topology.levels) || !reflect.DeepEqual(readerTopology(t, first), readerTopology(t, second)) {
		t.Fatal("same seed and insertion order produced different topology")
	}
}

func TestBuilderSeedPermutationsRemainValid(t *testing.T) {
	values := [][]float32{{0, 0}, {1, 0}, {0, 1}, {1, 1}, {2, 0}, {0, 2}, {2, 2}, {1, 2}}
	for _, seed := range []uint64{0, 1, math.MaxUint64} {
		builder := newTestBuilder(t, seed, len(values))
		for ordinal, value := range values {
			addTestVector(t, builder, vector.Ordinal(ordinal), value)
			stats, err := checkBuilder(builder)
			if err != nil || stats.NodeCount != ordinal+1 || stats.UnreachableNodes != 0 {
				t.Fatalf("seed %d insertion %d Check = (%+v, %v)", seed, ordinal, stats, err)
			}
		}
		reader, err := freezeTestBuilder(t, builder)
		if err != nil {
			t.Fatalf("seed %d Freeze: %v", seed, err)
		}
		if stats := reader.Report().Graph; stats.NodeCount != len(values) || stats.UnreachableNodes != 0 {
			t.Fatalf("seed %d stats = %+v", seed, stats)
		}
		for ordinal, want := range values {
			got, ok := readPreparedVector(reader.vectors, vector.Ordinal(ordinal))
			if !ok || !slices.Equal(got, want) {
				t.Fatalf("seed %d Vector(%d) = (%v, %v), want %v", seed, ordinal, got, ok, want)
			}
		}
	}
}

func TestBuilderFreezeIndependenceAndIdempotence(t *testing.T) {
	builder := newTestBuilder(t, 7, 4)
	for ordinal, value := range [][]float32{{0, 0}, {1, 0}, {0, 1}, {1, 1}} {
		addTestVector(t, builder, vector.Ordinal(ordinal), value)
	}
	first, err := freezeTestBuilder(t, builder)
	if err != nil {
		t.Fatal(err)
	}
	second, err := freezeTestBuilder(t, builder)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || first.topology.entry != second.topology.entry || !slices.Equal(first.topology.levels, second.topology.levels) || !reflect.DeepEqual(readerTopology(t, first), readerTopology(t, second)) {
		t.Fatal("repeated Freeze was not independent and idempotent")
	}

	firstValue, _ := readPreparedVector(first.vectors, 0)
	firstValue[0] = 99
	first.topology.level0Neighbors[0] = nodeOrdinal(first.Len() - 1)
	if got, _ := readPreparedVector(second.vectors, 0); !slices.Equal(got, []float32{0, 0}) {
		t.Fatalf("frozen readers share values: %v", got)
	}
	third, err := freezeTestBuilder(t, builder)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := readPreparedVector(third.vectors, 0); !slices.Equal(got, []float32{0, 0}) || !reflect.DeepEqual(readerTopology(t, third), readerTopology(t, second)) {
		t.Fatal("frozen reader mutation affected builder or later Freeze")
	}
}

func TestBuilderDiversifiedSelection(t *testing.T) {
	builder := newTestBuilder(t, 1, 4)
	for ordinal, value := range [][]float32{{0, 0}, {1, 0}, {1.1, 0}, {0, 2}} {
		addTestVector(t, builder, vector.Ordinal(ordinal), value)
	}

	got := builder.selectNeighbors([]searchCandidate{
		{node: 3, distance: builder.distanceNodes(0, 3)},
		{node: 2, distance: builder.distanceNodes(0, 2)},
		{node: 1, distance: builder.distanceNodes(0, 1)},
	}, 3)
	want := []nodeOrdinal{1, 3}
	if !slices.Equal(got, want) {
		t.Fatalf("diversified selection = %v, want %v", got, want)
	}
}

func TestBuilderOwnerRelativePruningCanBeAsymmetric(t *testing.T) {
	builder := newTestBuilder(t, 1, 4)
	for ordinal, value := range [][]float32{{0, 0}, {1, 0}, {0.9, 0}, {-2, 0}} {
		addTestVector(t, builder, vector.Ordinal(ordinal), value)
	}
	for node := range builder.graph.nodes {
		builder.graph.nodes[node].level = 1
		builder.graph.nodes[node].links = [][]nodeOrdinal{{}, {}}
	}
	builder.graph.nodes[0].links[1] = []nodeOrdinal{2, 3}
	builder.graph.nodes[1].links[1] = []nodeOrdinal{3}

	builder.addReverseLink(0, 1, 1)
	builder.addReverseLink(1, 0, 1)
	if got, want := builder.graph.nodes[0].links[1], []nodeOrdinal{2, 3}; !slices.Equal(got, want) {
		t.Fatalf("owner 0 pruned links = %v, want %v", got, want)
	}
	if got, want := builder.graph.nodes[1].links[1], []nodeOrdinal{0, 3}; !slices.Equal(got, want) {
		t.Fatalf("owner 1 links = %v, want %v", got, want)
	}
	if slices.Contains(builder.graph.nodes[0].links[1], nodeOrdinal(1)) || !slices.Contains(builder.graph.nodes[1].links[1], nodeOrdinal(0)) {
		t.Fatalf("expected asymmetric owner-relative links: 0=%v 1=%v", builder.graph.nodes[0].links[1], builder.graph.nodes[1].links[1])
	}
	if _, err := checkBuilder(builder); err != nil {
		t.Fatalf("asymmetric pruned graph is invalid: %v", err)
	}
}

func TestBuilderDiversificationAcceptsEqualBoundary(t *testing.T) {
	builder := newTestBuilder(t, 1, 3)
	for ordinal, value := range [][]float32{{0, 0}, {2, 0}, {1, 2}} {
		addTestVector(t, builder, vector.Ordinal(ordinal), value)
	}
	candidates := []searchCandidate{
		{node: 1, distance: builder.distanceNodes(0, 1)},
		{node: 2, distance: builder.distanceNodes(0, 2)},
	}
	if got, want := builder.selectNeighbors(candidates, 2), []nodeOrdinal{1, 2}; !slices.Equal(got, want) {
		t.Fatalf("equal-boundary diversified selection = %v, want %v", got, want)
	}
}

func TestBuilderFreezeProducesSearchableReader(t *testing.T) {
	const count = 8
	builder := newTestBuilder(t, 42, count)
	for ordinal := range count {
		addTestVector(t, builder, vector.Ordinal(ordinal), []float32{float32(ordinal), 0})
	}
	reader, err := freezeTestBuilder(t, builder)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Search(context.Background(), []float32{0, 0}, count, vector.SearchOptions{EfSearch: count, VisitLimit: count})
	if err != nil {
		t.Fatal(err)
	}
	want := make([]vector.Hit, count)
	for ordinal := range count {
		want[ordinal] = vector.Hit{Ordinal: vector.Ordinal(ordinal), Distance: float64(ordinal * ordinal)}
	}
	if !slices.Equal(result.Hits, want) || result.Incomplete {
		t.Fatalf("builder reader search = %+v, want %+v", result, want)
	}
}

func TestBuilderFreezeReaderSearchEdgeCases(t *testing.T) {
	emptyConfig := builderTestConfig(1, 1)
	l2, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	empty := newBuilder(emptyConfig, l2, 0, 0)
	emptyReader, err := freezeTestBuilder(t, empty)
	if err != nil {
		t.Fatal(err)
	}
	emptyResult, err := emptyReader.Search(context.Background(), []float32{1, 0}, 1, vector.SearchOptions{})
	if err != nil || len(emptyResult.Hits) != 0 || emptyResult.Incomplete {
		t.Fatalf("empty builder search = (%+v, %v)", emptyResult, err)
	}

	cosineConfig := builderTestConfig(2, 1)
	cosineCalculator, err := vector.NewCalculator(2, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	cosine := newBuilder(cosineConfig, cosineCalculator, 1, 2)
	if err := cosine.add([]float32{1, 0}); err != nil {
		t.Fatal(err)
	}
	cosineReader, err := freezeTestBuilder(t, cosine)
	if err != nil {
		t.Fatal(err)
	}
	cosineResult, err := cosineReader.Search(context.Background(), []float32{1, 0}, 1, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, cosineResult.Hits, []vector.Hit{{Ordinal: 0, Distance: 0}})
}

func TestBuilderRandomizedCheckAfterEveryInsertion(t *testing.T) {
	for seed := uint64(0); seed < 20; seed++ {
		const count = 24
		builder := newTestBuilder(t, seed, count)
		rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
		for ordinal := range count {
			value := []float32{rng.Float32()*20 - 10, rng.Float32()*20 - 10}
			addTestVector(t, builder, vector.Ordinal(ordinal), value)
			stats, err := checkBuilder(builder)
			if err != nil {
				t.Fatalf("seed %d insertion %d: %v", seed, ordinal, err)
			}
			if stats.NodeCount != ordinal+1 || stats.ReachableNodes+stats.UnreachableNodes != stats.NodeCount {
				t.Fatalf("seed %d insertion %d stats = %+v", seed, ordinal, stats)
			}
		}
	}
}

func FuzzBuilderInsertFreeze(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3, 4})
	f.Add([]byte{255, 1, 255, 1, 0, 0, 7, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			data = data[:64]
		}
		count := len(data) / 2
		if count == 0 {
			return
		}
		calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
		if err != nil {
			t.Fatal(err)
		}
		builder := newBuilder(builderTestConfig(uint64(data[0]), count), calculator, count, count*2)
		for ordinal := range count {
			value := []float32{float32(int8(data[ordinal*2])), float32(int8(data[ordinal*2+1]))}
			if err := builder.add(value); err != nil {
				t.Fatal(err)
			}
			if _, err := checkBuilder(builder); err != nil {
				t.Fatal(err)
			}
		}
		reader, err := freezeTestBuilder(t, builder)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Search(context.Background(), []float32{0, 0}, 1, vector.SearchOptions{}); err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzBuilderFloatInputs(f *testing.F) {
	f.Add(uint32(0))
	f.Add(math.Float32bits(1.25))
	f.Add(math.Float32bits(float32(math.Inf(1))))
	f.Add(math.Float32bits(float32(math.NaN())))
	f.Fuzz(func(t *testing.T, bits uint32) {
		config := builderTestConfig(1, 1)
		calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
		if err != nil {
			t.Fatal(err)
		}
		builder := newBuilder(config, calculator, 1, 2)
		value := math.Float32frombits(bits)
		err = builder.add([]float32{value, 0})
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			if !errors.Is(err, vector.ErrNonFiniteVector) {
				t.Fatalf("non-finite value %08x error = %v", bits, err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := checkBuilder(builder); err != nil {
			t.Fatal(err)
		}
		reader, err := freezeTestBuilder(t, builder)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Search(context.Background(), []float32{value, 0}, 1, vector.SearchOptions{}); err != nil {
			t.Fatal(err)
		}
	})
}
