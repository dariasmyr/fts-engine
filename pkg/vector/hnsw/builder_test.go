package hnsw

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func builderTestConfig(seed uint64, maxVectors int) BuildConfig {
	return BuildConfig{
		Dimensions:     2,
		Metric:         vector.MetricL2Squared,
		MaxVectors:     maxVectors,
		MaxVectorBytes: 1 << 20,
		MaxNeighbors:   2,
		EfConstruction: 8,
		Seed:           seed,
	}
}

func newTestBuilder(t *testing.T, seed uint64, count int) *Builder {
	t.Helper()
	builder, err := NewBuilder(builderTestConfig(seed, max(1, count)), readerTestSearchConfig(), count)
	if err != nil {
		t.Fatal(err)
	}
	return builder
}

func addTestVector(t *testing.T, builder *Builder, ordinal vector.Ordinal, value []float32) NodeOrdinal {
	t.Helper()
	node, err := builder.Add(context.Background(), ordinal, value)
	if err != nil {
		t.Fatalf("Add(%d, %v): %v", ordinal, value, err)
	}
	return node
}

func readerTopology(t *testing.T, reader *Searcher) [][][]NodeOrdinal {
	t.Helper()
	topology := make([][][]NodeOrdinal, reader.NodeCount())
	for node := range reader.NodeCount() {
		level, ok := reader.NodeLevel(NodeOrdinal(node))
		if !ok {
			t.Fatalf("missing node %d", node)
		}
		topology[node] = make([][]NodeOrdinal, int(level)+1)
		for currentLevel := 0; currentLevel <= int(level); currentLevel++ {
			neighbors, ok := reader.Neighbors(NodeOrdinal(node), currentLevel)
			if !ok {
				t.Fatalf("missing node %d level %d", node, currentLevel)
			}
			topology[node][currentLevel] = neighbors
		}
	}
	return topology
}

func TestBuildConfigValidation(t *testing.T) {
	valid := []struct {
		config BuildConfig
		count  int
	}{
		{builderTestConfig(0, 1), 0},
		{builderTestConfig(0, 1), 1},
		{BuildConfig{Dimensions: 1, Metric: vector.MetricCosine, MaxVectors: 1, MaxVectorBytes: 4, MaxNeighbors: MaxSupportedNeighbors, EfConstruction: MaxEfConstruction}, 1},
	}
	for i, test := range valid {
		if _, err := NewBuilder(test.config, readerTestSearchConfig(), test.count); err != nil {
			t.Errorf("valid config %d: %v", i, err)
		}
	}

	invalid := []struct {
		name   string
		config BuildConfig
		count  int
		want   error
	}{
		{"dimensions", BuildConfig{Metric: vector.MetricL2Squared, MaxVectors: 1, MaxNeighbors: 2, EfConstruction: 2}, 0, vector.ErrInvalidDimensions},
		{"metric", BuildConfig{Dimensions: 1, MaxVectors: 1, MaxNeighbors: 2, EfConstruction: 2}, 0, vector.ErrUnsupportedMetric},
		{"max vectors zero", BuildConfig{Dimensions: 1, Metric: vector.MetricL2Squared, MaxNeighbors: 2, EfConstruction: 2}, 0, ErrInvalidBuildConfig},
		{"negative count", BuildConfig{Dimensions: 1, Metric: vector.MetricL2Squared, MaxVectors: 1, MaxNeighbors: 2, EfConstruction: 2}, -1, ErrInvalidBuildConfig},
		{"count above max", BuildConfig{Dimensions: 1, Metric: vector.MetricL2Squared, MaxVectors: 1, MaxNeighbors: 2, EfConstruction: 2}, 2, ErrInvalidBuildConfig},
		{"max neighbors below minimum", BuildConfig{Dimensions: 1, Metric: vector.MetricL2Squared, MaxVectors: 1, MaxNeighbors: 1, EfConstruction: 2}, 0, ErrInvalidBuildConfig},
		{"max neighbors above maximum", BuildConfig{Dimensions: 1, Metric: vector.MetricL2Squared, MaxVectors: 1, MaxNeighbors: MaxSupportedNeighbors + 1, EfConstruction: MaxSupportedNeighbors + 1}, 0, ErrInvalidBuildConfig},
		{"ef below max neighbors", BuildConfig{Dimensions: 1, Metric: vector.MetricL2Squared, MaxVectors: 1, MaxNeighbors: 3, EfConstruction: 2}, 0, ErrInvalidBuildConfig},
		{"ef above maximum", BuildConfig{Dimensions: 1, Metric: vector.MetricL2Squared, MaxVectors: 1, MaxNeighbors: 2, EfConstruction: MaxEfConstruction + 1}, 0, ErrInvalidBuildConfig},
		{"allocation overflow", BuildConfig{Dimensions: math.MaxInt, Metric: vector.MetricL2Squared, MaxVectors: 2, MaxNeighbors: 2, EfConstruction: 2}, 2, ErrInvalidBuildConfig},
		{"byte allocation limit", BuildConfig{Dimensions: 2, Metric: vector.MetricL2Squared, MaxVectors: 1, MaxVectorBytes: 7, MaxNeighbors: 2, EfConstruction: 2}, 1, ErrInvalidBuildConfig},
		{"addressable byte overflow", BuildConfig{Dimensions: math.MaxInt/4 + 1, Metric: vector.MetricL2Squared, MaxVectors: 1, MaxVectorBytes: math.MaxUint64, MaxNeighbors: 2, EfConstruction: 2}, 1, ErrInvalidBuildConfig},
	}
	if strconv.IntSize == 64 {
		maxUint32 := uint64(math.MaxUint32)
		invalid = append(invalid, struct {
			name   string
			config BuildConfig
			count  int
			want   error
		}{"max vectors uint32", BuildConfig{Dimensions: 1, Metric: vector.MetricL2Squared, MaxVectors: int(maxUint32), MaxNeighbors: 2, EfConstruction: 2}, 0, ErrInvalidBuildConfig})
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewBuilder(test.config, readerTestSearchConfig(), test.count)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	if _, err := NewBuilder(builderTestConfig(0, 1), SearchConfig{}, 1); !errors.Is(err, ErrInvalidSearchConfig) {
		t.Fatalf("invalid search config error = %v", err)
	}
}

func TestBuilderCheckAfterEveryInsertionAndEntryTransitions(t *testing.T) {
	builder := newTestBuilder(t, 0, 6)
	values := [][]float32{{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 0}, {5, 0}}
	wantLevels := []uint8{0, 1, 5, 0, 3, 1}
	wantEntries := []NodeOrdinal{0, 1, 2, 2, 2, 2}

	for i, value := range values {
		node := addTestVector(t, builder, vector.Ordinal(i), value)
		if node != NodeOrdinal(i) {
			t.Fatalf("insertion %d node = %d, want %d", i, node, i)
		}
		if builder.graph.nodes[node].level != wantLevels[i] || builder.graph.entry != wantEntries[i] {
			t.Fatalf("insertion %d level/entry = %d/%d, want %d/%d", i, builder.graph.nodes[node].level, builder.graph.entry, wantLevels[i], wantEntries[i])
		}
		stats, err := builder.Check()
		if err != nil {
			t.Fatalf("Check after insertion %d: %v", i, err)
		}
		if stats.NodeCount != i+1 || stats.VectorCount != i+1 || stats.ReachableNodes != i+1 || stats.UnreachableNodes != 0 || stats.MaxLevel != int(wantLevels[wantEntries[i]]) {
			t.Fatalf("Check after insertion %d stats = %+v", i, stats)
		}
	}
}

func TestBuilderCompleteAndIncompleteFreeze(t *testing.T) {
	incomplete := newTestBuilder(t, 11, 2)
	addTestVector(t, incomplete, 0, []float32{1, 2})
	if reader, err := incomplete.Freeze(); reader != nil || !errors.Is(err, ErrBuilderIncomplete) {
		t.Fatalf("incomplete Freeze = (%v, %v), want (nil, ErrBuilderIncomplete)", reader, err)
	}
	if stats, err := incomplete.Check(); err != nil || stats.NodeCount != 1 {
		t.Fatalf("incomplete Check = (%+v, %v)", stats, err)
	}

	builder := newTestBuilder(t, 11, 3)
	addTestVector(t, builder, 2, []float32{2, 20})
	addTestVector(t, builder, 0, []float32{0, 10})
	addTestVector(t, builder, 1, []float32{1, 15})
	reader, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if reader.Len() != 3 || reader.BuildInfo() != builder.buildInfo || reader.Dimensions() != 2 || reader.Metric() != vector.MetricL2Squared {
		t.Fatalf("complete reader metadata = len:%d info:%+v dimensions:%d metric:%v", reader.Len(), reader.BuildInfo(), reader.Dimensions(), reader.Metric())
	}
	wantInfo := BuildInfo{BuildVersion: 1, LevelGeneratorVersion: 1, MaxNeighbors: 2, LevelZeroMaxNeighbors: 4, EfConstruction: 8, Seed: 11}
	if got := reader.BuildInfo(); got != wantInfo {
		t.Fatalf("BuildInfo = %+v, want %+v", got, wantInfo)
	}
	for ordinal, want := range [][]float32{{0, 10}, {1, 15}, {2, 20}} {
		got, ok := readPreparedVector(reader.VectorSource(), vector.Ordinal(ordinal))
		if !ok || !slices.Equal(got, want) {
			t.Errorf("Vector(%d) = (%v, %v), want (%v, true)", ordinal, got, ok, want)
		}
	}
	if stats := reader.GraphStats(); stats.NodeCount != 3 || stats.ReachableNodes != 3 || stats.UnreachableNodes != 0 {
		t.Fatalf("frozen stats = %+v", stats)
	}
}

func TestBuilderInvalidAddIsAtomic(t *testing.T) {
	builder := newTestBuilder(t, 0, 3)
	control := newTestBuilder(t, 0, 3)
	addTestVector(t, builder, 0, []float32{0, 0})
	addTestVector(t, control, 0, []float32{0, 0})

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name    string
		ctx     context.Context
		ordinal vector.Ordinal
		value   []float32
		want    error
	}{
		{"nil context", nil, 1, []float32{1, 0}, vector.ErrNilContext},
		{"cancelled context", cancelled, 1, []float32{1, 0}, context.Canceled},
		{"duplicate ordinal", context.Background(), 0, []float32{9, 9}, ErrDuplicateOrdinal},
		{"ordinal out of range", context.Background(), 3, []float32{1, 0}, vector.ErrOrdinalOutOfRange},
		{"dimension mismatch", context.Background(), 1, []float32{1}, vector.ErrDimensionMismatch},
		{"nan vector", context.Background(), 1, []float32{float32(math.NaN()), 0}, vector.ErrNonFiniteVector},
		{"infinite vector", context.Background(), 1, []float32{float32(math.Inf(1)), 0}, vector.ErrNonFiniteVector},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			beforeGraph := cloneGraphData(builder.graph)
			beforePresent := append([]bool(nil), builder.present...)
			beforeRNG := builder.rng
			node, err := builder.Add(test.ctx, test.ordinal, test.value)
			if node != 0 || !errors.Is(err, test.want) {
				t.Fatalf("Add = (%d, %v), want (0, %v)", node, err, test.want)
			}
			if !reflect.DeepEqual(builder.graph, beforeGraph) || !slices.Equal(builder.present, beforePresent) || builder.rng != beforeRNG {
				t.Fatalf("failed Add mutated builder: graph=%+v present=%v rng=%+v", builder.graph, builder.present, builder.rng)
			}
		})
	}

	for ordinal, value := range [][]float32{{1, 0}, {2, 0}} {
		addTestVector(t, builder, vector.Ordinal(ordinal+1), value)
		addTestVector(t, control, vector.Ordinal(ordinal+1), value)
	}
	got, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	want, err := control.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(readerTopology(t, got), readerTopology(t, want)) || !slices.Equal(got.topology.levels, want.topology.levels) || got.topology.entry != want.topology.entry {
		t.Fatal("failed additions changed later deterministic topology")
	}

	beforeGraph := cloneGraphData(builder.graph)
	beforeRNG := builder.rng
	if _, err := builder.Add(context.Background(), 0, []float32{0, 0}); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("full Add error = %v, want ErrCapacityExceeded", err)
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
		if _, err := builder.Check(); err != nil {
			t.Fatalf("Check after duplicate/clustered insertion %d: %v", i, err)
		}
	}
	reader, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	sawFullLevel0 := false
	sawFullUpper := false
	for node := 0; node < reader.NodeCount(); node++ {
		level, _ := reader.NodeLevel(NodeOrdinal(node))
		for currentLevel := 0; currentLevel <= int(level); currentLevel++ {
			neighbors, ok := reader.Neighbors(NodeOrdinal(node), currentLevel)
			if !ok {
				t.Fatalf("missing node %d level %d", node, currentLevel)
			}
			limit := reader.BuildInfo().neighborLimit(currentLevel)
			if len(neighbors) > limit {
				t.Errorf("node %d level %d degree = %d, max %d", node, currentLevel, len(neighbors), limit)
			}
			if len(neighbors) == limit && currentLevel == 0 {
				sawFullLevel0 = true
			}
			if len(neighbors) == limit && currentLevel > 0 {
				sawFullUpper = true
			}
			seen := make(map[NodeOrdinal]bool, len(neighbors))
			for _, neighbor := range neighbors {
				if neighbor == NodeOrdinal(node) || seen[neighbor] {
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
	build := func() *Searcher {
		builder := newTestBuilder(t, 0x12345678, len(values))
		for ordinal, value := range values {
			addTestVector(t, builder, vector.Ordinal(ordinal), value)
		}
		reader, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		return reader
	}

	first := build()
	second := build()
	if first.topology.entry != second.topology.entry || !slices.Equal(first.topology.levels, second.topology.levels) || !slices.Equal(first.topology.nodeToVector, second.topology.nodeToVector) || !reflect.DeepEqual(readerTopology(t, first), readerTopology(t, second)) {
		t.Fatal("same seed and insertion order produced different topology")
	}
}

func TestBuilderSeedAndOrderPermutationsRemainValid(t *testing.T) {
	values := [][]float32{{0, 0}, {1, 0}, {0, 1}, {1, 1}, {2, 0}, {0, 2}, {2, 2}, {1, 2}}
	orders := [][]vector.Ordinal{
		{0, 1, 2, 3, 4, 5, 6, 7},
		{7, 6, 5, 4, 3, 2, 1, 0},
		{3, 0, 6, 1, 7, 2, 5, 4},
	}
	for _, seed := range []uint64{0, 1, math.MaxUint64} {
		for orderIndex, order := range orders {
			builder := newTestBuilder(t, seed, len(values))
			for insertion, ordinal := range order {
				addTestVector(t, builder, ordinal, values[ordinal])
				stats, err := builder.Check()
				if err != nil || stats.NodeCount != insertion+1 || stats.UnreachableNodes != 0 {
					t.Fatalf("seed %d order %d insertion %d Check = (%+v, %v)", seed, orderIndex, insertion, stats, err)
				}
			}
			reader, err := builder.Freeze()
			if err != nil {
				t.Fatalf("seed %d order %d Freeze: %v", seed, orderIndex, err)
			}
			if stats := reader.GraphStats(); stats.NodeCount != len(values) || stats.UnreachableNodes != 0 {
				t.Fatalf("seed %d order %d stats = %+v", seed, orderIndex, stats)
			}
			for ordinal, want := range values {
				got, ok := readPreparedVector(reader.VectorSource(), vector.Ordinal(ordinal))
				if !ok || !slices.Equal(got, want) {
					t.Fatalf("seed %d order %d Vector(%d) = (%v, %v), want %v", seed, orderIndex, ordinal, got, ok, want)
				}
			}
		}
	}
}

func TestBuilderFreezeIndependenceAndIdempotence(t *testing.T) {
	builder := newTestBuilder(t, 7, 4)
	for ordinal, value := range [][]float32{{0, 0}, {1, 0}, {0, 1}, {1, 1}} {
		addTestVector(t, builder, vector.Ordinal(ordinal), value)
	}
	first, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	second, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || first.topology.entry != second.topology.entry || !slices.Equal(first.topology.levels, second.topology.levels) || !reflect.DeepEqual(readerTopology(t, first), readerTopology(t, second)) {
		t.Fatal("repeated Freeze was not independent and idempotent")
	}

	firstValue, _ := readPreparedVector(first.VectorSource(), 0)
	firstValue[0] = 99
	first.topology.level0Neighbors[0] = NodeOrdinal(first.NodeCount() - 1)
	if got, _ := readPreparedVector(second.VectorSource(), 0); !slices.Equal(got, []float32{0, 0}) {
		t.Fatalf("frozen readers share values: %v", got)
	}
	third, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := readPreparedVector(third.VectorSource(), 0); !slices.Equal(got, []float32{0, 0}) || !reflect.DeepEqual(readerTopology(t, third), readerTopology(t, second)) {
		t.Fatal("frozen reader mutation affected builder or later Freeze")
	}
}

func TestBuilderDiversifiedSelection(t *testing.T) {
	builder := newTestBuilder(t, 1, 4)
	for ordinal, value := range [][]float32{{0, 0}, {1, 0}, {1.1, 0}, {0, 2}} {
		addTestVector(t, builder, vector.Ordinal(ordinal), value)
	}

	got := builder.selectNeighbors(0, []NodeOrdinal{3, 2, 1, 2, 0}, 3)
	want := []NodeOrdinal{1, 3}
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
		builder.graph.nodes[node].links = [][]NodeOrdinal{{}, {}}
	}
	builder.graph.nodes[0].links[1] = []NodeOrdinal{2, 3}
	builder.graph.nodes[1].links[1] = []NodeOrdinal{3}

	builder.addReverseLink(0, 1, 1)
	builder.addReverseLink(1, 0, 1)
	if got, want := builder.graph.nodes[0].links[1], []NodeOrdinal{2, 3}; !slices.Equal(got, want) {
		t.Fatalf("owner 0 pruned links = %v, want %v", got, want)
	}
	if got, want := builder.graph.nodes[1].links[1], []NodeOrdinal{0, 3}; !slices.Equal(got, want) {
		t.Fatalf("owner 1 links = %v, want %v", got, want)
	}
	if slices.Contains(builder.graph.nodes[0].links[1], NodeOrdinal(1)) || !slices.Contains(builder.graph.nodes[1].links[1], NodeOrdinal(0)) {
		t.Fatalf("expected asymmetric owner-relative links: 0=%v 1=%v", builder.graph.nodes[0].links[1], builder.graph.nodes[1].links[1])
	}
	if _, err := builder.Check(); err != nil {
		t.Fatalf("asymmetric pruned graph is invalid: %v", err)
	}
}

func TestBuilderDiversificationAcceptsEqualBoundary(t *testing.T) {
	builder := newTestBuilder(t, 1, 3)
	for ordinal, value := range [][]float32{{0, 0}, {2, 0}, {1, 2}} {
		addTestVector(t, builder, vector.Ordinal(ordinal), value)
	}
	if got, want := builder.selectNeighbors(0, []NodeOrdinal{1, 2}, 2), []NodeOrdinal{1, 2}; !slices.Equal(got, want) {
		t.Fatalf("equal-boundary diversified selection = %v, want %v", got, want)
	}
}

func TestBuilderFreezeProducesSearchableReader(t *testing.T) {
	const count = 8
	builder := newTestBuilder(t, 42, count)
	for ordinal := range count {
		addTestVector(t, builder, vector.Ordinal(ordinal), []float32{float32(ordinal), 0})
	}
	reader, err := builder.Freeze()
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
	searchConfig := readerTestSearchConfig()
	emptyConfig := builderTestConfig(1, 1)
	empty, err := NewBuilder(emptyConfig, searchConfig, 0)
	if err != nil {
		t.Fatal(err)
	}
	emptyReader, err := empty.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	emptyResult, err := emptyReader.Search(context.Background(), []float32{1, 0}, 1, vector.SearchOptions{})
	if err != nil || len(emptyResult.Hits) != 0 || emptyResult.Incomplete {
		t.Fatalf("empty builder search = (%+v, %v)", emptyResult, err)
	}

	cosineConfig := builderTestConfig(2, 1)
	cosineConfig.Metric = vector.MetricCosine
	cosine, err := NewBuilder(cosineConfig, searchConfig, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cosine.Add(context.Background(), 0, []float32{10, 0}); err != nil {
		t.Fatal(err)
	}
	cosineReader, err := cosine.Freeze()
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
			stats, err := builder.Check()
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
		builder, err := NewBuilder(builderTestConfig(uint64(data[0]), count), readerTestSearchConfig(), count)
		if err != nil {
			t.Fatal(err)
		}
		for ordinal := range count {
			value := []float32{float32(int8(data[ordinal*2])), float32(int8(data[ordinal*2+1]))}
			if _, err := builder.Add(context.Background(), vector.Ordinal(ordinal), value); err != nil {
				t.Fatal(err)
			}
			if _, err := builder.Check(); err != nil {
				t.Fatal(err)
			}
		}
		reader, err := builder.Freeze()
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
		builder, err := NewBuilder(config, readerTestSearchConfig(), 1)
		if err != nil {
			t.Fatal(err)
		}
		value := math.Float32frombits(bits)
		_, err = builder.Add(context.Background(), 0, []float32{value, 0})
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			if !errors.Is(err, vector.ErrNonFiniteVector) {
				t.Fatalf("non-finite value %08x error = %v", bits, err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := builder.Check(); err != nil {
			t.Fatal(err)
		}
		reader, err := builder.Freeze()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reader.Search(context.Background(), []float32{value, 0}, 1, vector.SearchOptions{}); err != nil {
			t.Fatal(err)
		}
	})
}
