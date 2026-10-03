package hnsw

import (
	"context"
	"errors"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func readerTestSearchConfig() SearchConfig {
	return SearchConfig{
		DefaultEfSearch:   8,
		MaxEfSearch:       32,
		DefaultVisitLimit: 64,
		MaxVisitLimit:     128,
		MaxK:              16,
	}
}

func readPreparedVector(source vectorstore.PreparedVectorStore, ordinal vector.Ordinal) ([]float32, bool) {
	if source == nil || uint64(ordinal) >= uint64(source.Len()) {
		return nil, false
	}
	value := make([]float32, source.Dimensions())
	if err := source.ReadVectorInto(context.Background(), ordinal, value); err != nil {
		return nil, false
	}
	return value, true
}

func readerTestBuildInfo() BuildInfo {
	return BuildInfo{
		BuildVersion:          BuildVersion,
		LevelGeneratorVersion: LevelGeneratorVersion,
		MaxNeighbors:          2,
		LevelZeroMaxNeighbors: 4,
		EfConstruction:        8,
		Seed:                  23,
	}
}

func readerTestSpace(t *testing.T, dimensions int, metric vector.Metric) vector.Calculator {
	t.Helper()
	space, err := vector.NewCalculator(dimensions, metric)
	if err != nil {
		t.Fatal(err)
	}
	return space
}

func readerTestGraph() graphData {
	return graphData{
		values: []float32{
			0, 0,
			1, 0,
			2, 0,
			3, 0,
		},
		nodes: []mutableNode{
			{vectorOrdinal: 2, level: 2, links: [][]nodeOrdinal{{1, 2}, {2}, {}}},
			{vectorOrdinal: 0, level: 0, links: [][]nodeOrdinal{{0}}},
			{vectorOrdinal: 3, level: 1, links: [][]nodeOrdinal{{0, 3}, {0}}},
			{vectorOrdinal: 1, level: 0, links: [][]nodeOrdinal{{}}},
		},
		entry:    0,
		hasEntry: true,
	}
}

func readerTestSource(t testing.TB, calculator vector.Calculator, graph graphData) vectorstore.PreparedVectorStore {
	t.Helper()
	source, err := vectorstore.NewPreparedMemoryVectorStore(calculator, graph.values)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func newReaderForTest(t *testing.T, calculator vector.Calculator, graph graphData) *Index {
	t.Helper()
	reader, err := newIndexFromGraph(calculator, readerTestSearchConfig(), readerTestBuildInfo(), graph, readerTestSource(t, calculator, graph))
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func TestSearchConfigValidation(t *testing.T) {
	valid := []SearchConfig{
		readerTestSearchConfig(),
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 1, MaxVisitLimit: 1, MaxK: 1},
	}
	for i, config := range valid {
		if err := config.validate(); err != nil {
			t.Errorf("valid config %d: %v", i, err)
		}
	}

	invalid := []SearchConfig{
		{},
		{DefaultEfSearch: 0, MaxEfSearch: 1, DefaultVisitLimit: 1, MaxVisitLimit: 1, MaxK: 1},
		{DefaultEfSearch: 2, MaxEfSearch: 1, DefaultVisitLimit: 1, MaxVisitLimit: 1, MaxK: 1},
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 0, MaxVisitLimit: 1, MaxK: 1},
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 2, MaxVisitLimit: 1, MaxK: 1},
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 1, MaxVisitLimit: 1, MaxK: 0},
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 1, MaxVisitLimit: 1, MaxK: 2},
	}
	for i, config := range invalid {
		if err := config.validate(); !errors.Is(err, ErrInvalidSearchConfig) {
			t.Errorf("invalid config %d error = %v, want ErrInvalidSearchConfig", i, err)
		}
	}

	space := readerTestSpace(t, 1, vector.MetricL2Squared)
	graph := graphData{}
	_, err := newIndexFromGraph(space, SearchConfig{}, readerTestBuildInfo(), graph, readerTestSource(t, space, graph))
	if !errors.Is(err, ErrInvalidSearchConfig) {
		t.Fatalf("newHNSWIndexFromGraph invalid search config error = %v", err)
	}
}

func TestReaderEmptyAndSingleton(t *testing.T) {
	space := readerTestSpace(t, 2, vector.MetricL2Squared)
	empty := newReaderForTest(t, space, graphData{})
	if empty.Len() != 0 || empty.Dimensions() != 2 || empty.Metric() != vector.MetricL2Squared || empty.vectors.Normalization() != vector.NormalizationNone || empty.Report().Search.MaxK != 16 {
		t.Fatalf("empty reader metadata = len:%d dimensions:%d metric:%v normalization:%v maxK:%d", empty.Len(), empty.Dimensions(), empty.Metric(), empty.vectors.Normalization(), empty.Report().Search.MaxK)
	}
	if _, _, ok := empty.entryPoint(); ok {
		t.Fatal("empty reader has an entry point")
	}
	if _, ok := empty.nodeLevel(0); ok {
		t.Fatal("empty reader has node 0")
	}
	if _, ok := empty.neighbors(0, 0); ok {
		t.Fatal("empty reader has adjacency for node 0")
	}
	if _, ok := readPreparedVector(empty.vectors, 0); ok {
		t.Fatal("empty reader has vector 0")
	}
	if got := empty.Report().Graph; !reflect.DeepEqual(got, GraphStats{MaxLevel: -1}) {
		t.Fatalf("empty stats = %+v, want MaxLevel -1", got)
	}
	result, err := empty.Search(context.Background(), []float32{0, 0}, 1, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 || result.Incomplete || result.Stats.Termination != vector.TerminationComplete {
		t.Fatalf("empty search result = %+v", result)
	}

	cosine := readerTestSpace(t, 2, vector.MetricCosine)
	singleton := newReaderForTest(t, cosine, graphData{
		values:   []float32{0.6, 0.8},
		nodes:    []mutableNode{{vectorOrdinal: 0, links: [][]nodeOrdinal{{}}}},
		entry:    0,
		hasEntry: true,
	})
	if singleton.Len() != 1 || singleton.vectors.Normalization() != vector.NormalizationUnitLength {
		t.Fatalf("singleton metadata = len:%d normalization:%v", singleton.Len(), singleton.vectors.Normalization())
	}
	if node, level, ok := singleton.entryPoint(); !ok || node != 0 || level != 0 {
		t.Fatalf("singleton entry = (%d, %d, %v)", node, level, ok)
	}
	if neighbors, ok := singleton.neighbors(0, 0); !ok || len(neighbors) != 0 {
		t.Fatalf("singleton neighbors = %v, %v", neighbors, ok)
	}
	result, err = singleton.Search(context.Background(), []float32{3, 4}, 1, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Ordinal != 0 || math.Abs(result.Hits[0].Distance) > 1e-7 {
		t.Fatalf("singleton search result = %+v", result)
	}
}

func TestReaderExplicitLevelsMembershipPackingAndMapping(t *testing.T) {
	reader := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())

	if got, want := reader.topology.level0Offsets, []uint32{0, 2, 3, 5, 5}; !slices.Equal(got, want) {
		t.Fatalf("level0 offsets = %v, want %v", got, want)
	}
	if got, want := reader.topology.level0Neighbors, []nodeOrdinal{1, 2, 0, 0, 3}; !slices.Equal(got, want) {
		t.Fatalf("level0 neighbors = %v, want %v", got, want)
	}
	if got, want := reader.topology.upperNodeOffsets, []uint32{0, 2, 2, 3, 3}; !slices.Equal(got, want) {
		t.Fatalf("upper node offsets = %v, want %v", got, want)
	}
	if got, want := reader.topology.upperLinkOffsets, []uint32{0, 1, 1, 2}; !slices.Equal(got, want) {
		t.Fatalf("upper link offsets = %v, want %v", got, want)
	}
	if got, want := reader.topology.upperNeighbors, []nodeOrdinal{2, 0}; !slices.Equal(got, want) {
		t.Fatalf("upper neighbors = %v, want %v", got, want)
	}

	levels := []uint8{2, 0, 1, 0}
	for node, want := range levels {
		if got, ok := reader.nodeLevel(nodeOrdinal(node)); !ok || got != want {
			t.Errorf("NodeLevel(%d) = (%d, %v), want (%d, true)", node, got, ok, want)
		}
	}
	if _, ok := reader.nodeLevel(4); ok {
		t.Fatal("out-of-range node has a level")
	}
	for _, test := range []struct {
		node  nodeOrdinal
		level int
		want  []nodeOrdinal
		ok    bool
	}{
		{0, 0, []nodeOrdinal{1, 2}, true},
		{0, 1, []nodeOrdinal{2}, true},
		{0, 2, []nodeOrdinal{}, true},
		{1, 0, []nodeOrdinal{0}, true},
		{2, 0, []nodeOrdinal{0, 3}, true},
		{2, 1, []nodeOrdinal{0}, true},
		{3, 0, []nodeOrdinal{}, true},
		{0, 3, nil, false},
		{1, 1, nil, false},
		{4, 0, nil, false},
		{0, -1, nil, false},
	} {
		got, ok := reader.neighbors(test.node, test.level)
		if ok != test.ok || !slices.Equal(got, test.want) {
			t.Errorf("Neighbors(%d, %d) = (%v, %v), want (%v, %v)", test.node, test.level, got, ok, test.want, test.ok)
		}
	}

	if got, want := reader.topology.nodeToVector, []vector.Ordinal{2, 0, 3, 1}; !slices.Equal(got, want) {
		t.Fatalf("node mapping = %v, want %v", got, want)
	}
	for ordinal, want := range [][]float32{{0, 0}, {1, 0}, {2, 0}, {3, 0}} {
		got, ok := readPreparedVector(reader.vectors, vector.Ordinal(ordinal))
		if !ok || !slices.Equal(got, want) {
			t.Errorf("Vector(%d) = (%v, %v), want (%v, true)", ordinal, got, ok, want)
		}
	}
	for node, wantOrdinal := range []vector.Ordinal{2, 0, 3, 1} {
		_, gotOrdinal, ok := reader.vectorByNode(nodeOrdinal(node))
		if !ok || gotOrdinal != wantOrdinal {
			t.Errorf("vectorByNode(%d) ordinal = (%d, %v), want (%d, true)", node, gotOrdinal, ok, wantOrdinal)
		}
	}

	wantStats := GraphStats{
		NodeCount: 4, VectorCount: 4, MaxLevel: 2,
		LevelNodeCounts: []int{4, 2, 1}, LevelLinkCounts: []int{5, 2, 0},
		ReachableNodes: 4, ZeroDegreeNodes: 1,
	}
	if got := reader.Report().Graph; !reflect.DeepEqual(got, wantStats) {
		t.Fatalf("stats = %+v, want %+v", got, wantStats)
	}
	result, err := reader.Search(context.Background(), []float32{0, 0}, 4, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantHits := []vector.Hit{{Ordinal: 0, Distance: 0}, {Ordinal: 1, Distance: 1}, {Ordinal: 2, Distance: 4}, {Ordinal: 3, Distance: 9}}
	if !slices.Equal(result.Hits, wantHits) {
		t.Fatalf("mapped search hits = %+v, want %+v", result.Hits, wantHits)
	}
}

func TestReaderRejectsInvalidGraphData(t *testing.T) {
	l2 := readerTestSpace(t, 1, vector.MetricL2Squared)
	cosine := readerTestSpace(t, 2, vector.MetricCosine)
	validInfo := readerTestBuildInfo()
	validSearch := readerTestSearchConfig()

	tests := []struct {
		name       string
		calculator vector.Calculator
		info       BuildInfo
		graph      graphData
		want       error
	}{
		{"invalid build version", l2, BuildInfo{BuildVersion: BuildVersion + 1, LevelGeneratorVersion: LevelGeneratorVersion, MaxNeighbors: 2, LevelZeroMaxNeighbors: 4, EfConstruction: 2}, graphData{}, ErrInvalidBuildConfig},
		{"invalid generator version", l2, BuildInfo{BuildVersion: BuildVersion, LevelGeneratorVersion: LevelGeneratorVersion + 1, MaxNeighbors: 2, LevelZeroMaxNeighbors: 4, EfConstruction: 2}, graphData{}, ErrInvalidBuildConfig},
		{"invalid max neighbors", l2, BuildInfo{BuildVersion: BuildVersion, LevelGeneratorVersion: LevelGeneratorVersion, MaxNeighbors: 1, LevelZeroMaxNeighbors: 2, EfConstruction: 2}, graphData{}, ErrInvalidBuildConfig},
		{"invalid level-zero max neighbors", l2, BuildInfo{BuildVersion: BuildVersion, LevelGeneratorVersion: LevelGeneratorVersion, MaxNeighbors: 2, LevelZeroMaxNeighbors: 3, EfConstruction: 2}, graphData{}, ErrInvalidBuildConfig},
		{"invalid ef", l2, BuildInfo{BuildVersion: BuildVersion, LevelGeneratorVersion: LevelGeneratorVersion, MaxNeighbors: 2, LevelZeroMaxNeighbors: 4, EfConstruction: 1}, graphData{}, ErrInvalidBuildConfig},
		{"empty with entry", l2, validInfo, graphData{hasEntry: true}, errInvalidGraph},
		{"value dimensions", l2, validInfo, graphData{values: []float32{1, 2}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"missing entry", l2, validInfo, graphData{values: []float32{1}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}}, errInvalidGraph},
		{"entry out of range", l2, validInfo, graphData{values: []float32{1}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, entry: 1, hasEntry: true}, errInvalidGraph},
		{"entry below max level", l2, validInfo, graphData{values: []float32{1, 2}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}, {level: 1, vectorOrdinal: 1, links: [][]nodeOrdinal{{}, {}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"level above maximum", l2, validInfo, graphData{values: []float32{1}, nodes: []mutableNode{{level: MaxLevel + 1, links: make([][]nodeOrdinal, MaxLevel+2)}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"level link count short", l2, validInfo, graphData{values: []float32{1}, nodes: []mutableNode{{level: 1, links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"level link count long", l2, validInfo, graphData{values: []float32{1}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}, {}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"vector ordinal out of range", l2, validInfo, graphData{values: []float32{1}, nodes: []mutableNode{{vectorOrdinal: 1, links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"duplicate vector ordinal", l2, validInfo, graphData{values: []float32{1, 2}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}, {links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"link out of range", l2, validInfo, graphData{values: []float32{1}, nodes: []mutableNode{{links: [][]nodeOrdinal{{1}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"self link", l2, validInfo, graphData{values: []float32{1}, nodes: []mutableNode{{links: [][]nodeOrdinal{{0}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"duplicate link", l2, validInfo, graphData{values: []float32{1, 2}, nodes: []mutableNode{{links: [][]nodeOrdinal{{1, 1}}}, {vectorOrdinal: 1, links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"upper link to absent level", l2, validInfo, graphData{values: []float32{1, 2}, nodes: []mutableNode{{level: 1, links: [][]nodeOrdinal{{}, {1}}}, {vectorOrdinal: 1, links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"level0 degree", l2, validInfo, graphData{values: []float32{0, 1, 2, 3, 4, 5}, nodes: []mutableNode{{links: [][]nodeOrdinal{{1, 2, 3, 4, 5}}}, {vectorOrdinal: 1, links: [][]nodeOrdinal{{}}}, {vectorOrdinal: 2, links: [][]nodeOrdinal{{}}}, {vectorOrdinal: 3, links: [][]nodeOrdinal{{}}}, {vectorOrdinal: 4, links: [][]nodeOrdinal{{}}}, {vectorOrdinal: 5, links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"upper degree", l2, validInfo, graphData{values: []float32{0, 1, 2, 3}, nodes: []mutableNode{{level: 1, links: [][]nodeOrdinal{{}, {1, 2, 3}}}, {vectorOrdinal: 1, level: 1, links: [][]nodeOrdinal{{}, {}}}, {vectorOrdinal: 2, level: 1, links: [][]nodeOrdinal{{}, {}}}, {vectorOrdinal: 3, level: 1, links: [][]nodeOrdinal{{}, {}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"nan prepared vector", l2, validInfo, graphData{values: []float32{float32(math.NaN())}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"infinite prepared vector", l2, validInfo, graphData{values: []float32{float32(math.Inf(1))}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"negative zero prepared vector", l2, validInfo, graphData{values: []float32{math.Float32frombits(1 << 31)}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"unnormalized cosine vector", cosine, validInfo, graphData{values: []float32{3, 4}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
		{"zero cosine vector", cosine, validInfo, graphData{values: []float32{0, 0}, nodes: []mutableNode{{links: [][]nodeOrdinal{{}}}}, entry: 0, hasEntry: true}, errInvalidGraph},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, sourceErr := vectorstore.NewPreparedMemoryVectorStore(test.calculator, make([]float32, len(test.graph.nodes)*test.calculator.Dimensions()))
			if sourceErr != nil {
				t.Fatal(sourceErr)
			}
			_, err := newIndexFromGraph(test.calculator, validSearch, test.info, test.graph, source)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestReaderReturnsImmutableCopies(t *testing.T) {
	graph := readerTestGraph()
	reader := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), graph)

	graph.values[0] = 99
	graph.nodes[0].links[0][0] = 3
	if got, _ := readPreparedVector(reader.vectors, 0); !slices.Equal(got, []float32{0, 0}) {
		t.Fatalf("reader retained input values: %v", got)
	}
	if got, _ := reader.neighbors(0, 0); !slices.Equal(got, []nodeOrdinal{1, 2}) {
		t.Fatalf("reader retained input links: %v", got)
	}

	value, _ := readPreparedVector(reader.vectors, 0)
	value[0] = 77
	valueAgain, _ := readPreparedVector(reader.vectors, 0)
	if !slices.Equal(valueAgain, []float32{0, 0}) {
		t.Fatalf("Vector returned mutable storage: %v", valueAgain)
	}
	neighbors, _ := reader.neighbors(0, 0)
	neighbors[0] = 3
	neighborsAgain, _ := reader.neighbors(0, 0)
	if !slices.Equal(neighborsAgain, []nodeOrdinal{1, 2}) {
		t.Fatalf("Neighbors returned mutable storage: %v", neighborsAgain)
	}
	stats := reader.Report().Graph
	stats.LevelNodeCounts[0] = 99
	stats.LevelLinkCounts[0] = 99
	statsAgain := reader.Report().Graph
	if !slices.Equal(statsAgain.LevelNodeCounts, []int{4, 2, 1}) || !slices.Equal(statsAgain.LevelLinkCounts, []int{5, 2, 0}) {
		t.Fatalf("GraphStats returned mutable storage: %+v", statsAgain)
	}
}

func TestNewReaderFromGraphRetainsPreparedSource(t *testing.T) {
	source := &graphPreparedSource{
		values:        [][]float32{{0, 0}, {1, 0}, {2, 0}, {3, 0}},
		dimensions:    2,
		metric:        vector.MetricL2Squared,
		normalization: vector.NormalizationNone,
	}
	reader, err := newIndexFromGraph(readerTestSpace(t, 2, vector.MetricL2Squared), readerTestSearchConfig(), readerTestBuildInfo(), readerTestGraph(), source)
	if err != nil {
		t.Fatal(err)
	}
	source.reads = 0

	if _, ok := readPreparedVector(reader.vectors, 1); !ok {
		t.Fatal("reader.Vectors row 1 failed")
	}
	if source.reads != 1 {
		t.Fatalf("reader did not retain prepared source, reads = %d", source.reads)
	}
}

func TestReaderConcurrentSearch(t *testing.T) {
	reader := newReaderForTest(t, readerTestSpace(t, 2, vector.MetricL2Squared), readerTestGraph())
	want := []vector.Hit{{Ordinal: 0, Distance: 0}, {Ordinal: 1, Distance: 1}, {Ordinal: 2, Distance: 4}, {Ordinal: 3, Distance: 9}}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				result, err := reader.Search(context.Background(), []float32{0, 0}, 4, vector.SearchOptions{})
				if err != nil {
					errs <- err
					return
				}
				if !slices.Equal(result.Hits, want) {
					errs <- errors.New("concurrent Reader.Search result mismatch")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
