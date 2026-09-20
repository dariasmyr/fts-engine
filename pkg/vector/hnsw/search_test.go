package hnsw

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func testSpace(t *testing.T, dimensions int, metric vector.Metric) vector.Space {
	t.Helper()
	space, err := vector.NewSpace(dimensions, metric)
	if err != nil {
		t.Fatal(err)
	}
	return space
}

func testSearchConfig() SearchConfig {
	return SearchConfig{
		DefaultEfSearch: 3, MaxEfSearch: 8,
		DefaultVisitLimit: 32, MaxVisitLimit: 64,
		MaxK: 8,
	}
}

func testBuildInfo() BuildInfo {
	return BuildInfo{
		BuildVersion:          BuildVersion,
		LevelGeneratorVersion: LevelGeneratorVersion,
		MaxNeighbors:          2,
		LevelZeroMaxNeighbors: 4,
		EfConstruction:        8,
	}
}

func newTestReader(t *testing.T, space vector.Space, config SearchConfig, graph graphData) *Searcher {
	t.Helper()
	reader, err := newSearcherFromGraph(space, config, testBuildInfo(), graph)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func requireHits(t *testing.T, got, want []vector.Hit) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("hits = %+v, want %+v", got, want)
	}
}

func requireStats(t *testing.T, got, want vector.SearchStats) {
	t.Helper()
	if got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
}

func TestSearchGreedyUpperLevelStopsAtLocalMinimum(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{10, 4, 6},
		nodes: []mutableNode{
			{level: 1, links: [][]NodeOrdinal{{}, {1}}},
			{vectorOrdinal: 1, level: 1, links: [][]NodeOrdinal{{}, {2}}},
			{vectorOrdinal: 2, level: 1, links: [][]NodeOrdinal{{}, {}}},
		},
		entry: 0, hasEntry: true,
	})

	result, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{{Ordinal: 1, Distance: 16}})
	requireStats(t, result.Stats, vector.SearchStats{
		VisitedNodes: 3, ExpandedNodes: 3, DistanceComputations: 3,
		Termination: vector.TerminationComplete,
	})
	if result.Incomplete {
		t.Fatal("complete upper-level search marked incomplete")
	}
}

func TestSearchLevelZeroBeamEscapesLocalMinimumOverDirectedLinks(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{2, 3, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{2}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})

	narrow, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{EfSearch: 1})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, narrow.Hits, []vector.Hit{{Ordinal: 0, Distance: 4}})
	requireStats(t, narrow.Stats, vector.SearchStats{
		VisitedNodes: 2, ExpandedNodes: 1, DistanceComputations: 2,
		Termination: vector.TerminationComplete,
	})

	beam, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{EfSearch: 2})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, beam.Hits, []vector.Hit{{Ordinal: 2, Distance: 1}})
	requireStats(t, beam.Stats, vector.SearchStats{
		VisitedNodes: 3, ExpandedNodes: 3, DistanceComputations: 3,
		Termination: vector.TerminationComplete,
	})
}

func TestSearchExpandsEqualDistanceRouteToCloserNode(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{1, -1, 0},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{2}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})

	result, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{EfSearch: 1})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{{Ordinal: 2, Distance: 0}})
}

func TestSearchGreedyDoesNotMoveAcrossEqualDistancePlateau(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{-1, 1, 0},
		nodes: []mutableNode{
			{level: 1, links: [][]NodeOrdinal{{}, {}}},
			{vectorOrdinal: 1, level: 1, links: [][]NodeOrdinal{{2}, {0}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 1, hasEntry: true,
	})

	result, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{{Ordinal: 2, Distance: 0}})
}

func TestSearchScoresMultiplyReachableNodeOnce(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	config := testSearchConfig()
	config.DefaultEfSearch = 4
	reader := newTestReader(t, space, config, graphData{
		values: []float32{4, 3, 2, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1, 2}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{3}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{3}}},
			{vectorOrdinal: 3, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})

	result, err := reader.Search(context.Background(), []float32{0}, 4, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{
		{Ordinal: 3, Distance: 1},
		{Ordinal: 2, Distance: 4},
		{Ordinal: 1, Distance: 9},
		{Ordinal: 0, Distance: 16},
	})
	requireStats(t, result.Stats, vector.SearchStats{
		VisitedNodes: 4, ExpandedNodes: 2, DistanceComputations: 4,
		Termination: vector.TerminationComplete,
	})
}

func TestSearchUsesRejectedNodesAsRoutesButNeverReturnsThem(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{3, 2, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{2}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})
	filter, err := vector.NewBitSet(3, 2)
	if err != nil {
		t.Fatal(err)
	}

	result, err := reader.Search(context.Background(), []float32{0}, 3, vector.SearchOptions{ResultFilter: filter})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{{Ordinal: 2, Distance: 1}})
	requireStats(t, result.Stats, vector.SearchStats{
		VisitedNodes: 3, ExpandedNodes: 2, DistanceComputations: 3, RejectedNodes: 2,
		Termination: vector.TerminationComplete,
	})
}

func TestSearchSelectiveFilterCompletesBeforeVisitLimit(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{3, 2, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{2}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})
	filter, err := vector.NewBitSet(3, 1)
	if err != nil {
		t.Fatal(err)
	}

	result, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{
		ResultFilter: filter,
		VisitLimit:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{{Ordinal: 1, Distance: 4}})
	requireStats(t, result.Stats, vector.SearchStats{
		VisitedNodes: 2, ExpandedNodes: 1, DistanceComputations: 2, RejectedNodes: 1,
		Termination: vector.TerminationComplete,
	})
	if result.Incomplete {
		t.Fatal("filter-complete result marked incomplete")
	}
}

func TestSearchIgnoresResultFilterDuringUpperLevelNavigation(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{10, 5, 1},
		nodes: []mutableNode{
			{level: 1, links: [][]NodeOrdinal{{}, {1}}},
			{vectorOrdinal: 1, level: 1, links: [][]NodeOrdinal{{2}, {}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})
	filter, err := vector.NewBitSet(3, 2)
	if err != nil {
		t.Fatal(err)
	}

	result, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{ResultFilter: filter})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{{Ordinal: 2, Distance: 1}})
	if result.Stats.RejectedNodes != 2 || result.Stats.VisitedNodes != 3 || result.Stats.DistanceComputations != 3 {
		t.Fatalf("upper-level filtered navigation stats = %+v", result.Stats)
	}
}

func TestSearchDeterministicEqualDistanceTies(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{-1, 1, 2},
		nodes: []mutableNode{
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{2, 1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{}}},
			{links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})
	wantHits := []vector.Hit{{Ordinal: 0, Distance: 1}, {Ordinal: 1, Distance: 1}}
	wantStats := vector.SearchStats{
		VisitedNodes: 3, ExpandedNodes: 1, DistanceComputations: 3,
		Termination: vector.TerminationComplete,
	}

	for run := 0; run < 25; run++ {
		result, err := reader.Search(context.Background(), []float32{0}, 2, vector.SearchOptions{})
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		requireHits(t, result.Hits, wantHits)
		requireStats(t, result.Stats, wantStats)
	}
}

func TestSearchVisitLimitReturnsPartialResultAndCounters(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{3, 2, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{2}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})

	result, err := reader.Search(context.Background(), []float32{0}, 2, vector.SearchOptions{VisitLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{{Ordinal: 1, Distance: 4}, {Ordinal: 0, Distance: 9}})
	requireStats(t, result.Stats, vector.SearchStats{
		VisitedNodes: 2, ExpandedNodes: 2, DistanceComputations: 2,
		Termination: vector.TerminationVisitLimit,
	})
	if !result.Incomplete {
		t.Fatal("visit-limited result is not marked incomplete")
	}
}

func TestSearchUpperLevelVisitLimitReturnsAcceptedPartialHits(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{3, 2, 1},
		nodes: []mutableNode{
			{level: 1, links: [][]NodeOrdinal{{}, {1, 2}}},
			{vectorOrdinal: 1, level: 1, links: [][]NodeOrdinal{{}, {}}},
			{vectorOrdinal: 2, level: 1, links: [][]NodeOrdinal{{}, {}}},
		},
		entry: 0, hasEntry: true,
	})

	result, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{VisitLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{{Ordinal: 1, Distance: 4}})
	requireStats(t, result.Stats, vector.SearchStats{
		VisitedNodes: 2, ExpandedNodes: 1, DistanceComputations: 2,
		Termination: vector.TerminationVisitLimit,
	})
	if !result.Incomplete {
		t.Fatal("upper-level visit-limited result is not marked incomplete")
	}
}

func TestSearchDefaultsAndRequestedLimits(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	config := SearchConfig{
		DefaultEfSearch: 2, MaxEfSearch: 3,
		DefaultVisitLimit: 2, MaxVisitLimit: 3,
		MaxK: 3,
	}
	reader := newTestReader(t, space, config, graphData{
		values: []float32{2, 3, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{2}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})

	defaulted, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, defaulted.Hits, []vector.Hit{{Ordinal: 0, Distance: 4}})
	if !defaulted.Incomplete || defaulted.Stats.Termination != vector.TerminationVisitLimit || defaulted.Stats.VisitedNodes != 2 {
		t.Fatalf("default limits result = %+v", defaulted)
	}

	requested, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{EfSearch: 1, VisitLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, requested.Hits, []vector.Hit{{Ordinal: 0, Distance: 4}})
	requireStats(t, requested.Stats, vector.SearchStats{
		VisitedNodes: 2, ExpandedNodes: 1, DistanceComputations: 2,
		Termination: vector.TerminationComplete,
	})

	wide, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{EfSearch: 2, VisitLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, wide.Hits, []vector.Hit{{Ordinal: 2, Distance: 1}})
	if wide.Incomplete {
		t.Fatal("explicit limits unexpectedly incomplete")
	}
}

type cancelOnErrContext struct {
	context.Context
	done     chan struct{}
	cancelAt int
	calls    int
	canceled bool
}

func newCancelOnErrContext(cancelAt int) *cancelOnErrContext {
	return &cancelOnErrContext{Context: context.Background(), done: make(chan struct{}), cancelAt: cancelAt}
}

func (c *cancelOnErrContext) Done() <-chan struct{} { return c.done }

func (c *cancelOnErrContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	c.calls++
	if c.calls >= c.cancelAt {
		c.canceled = true
		close(c.done)
		return context.Canceled
	}
	return nil
}

func TestSearchNilPreCancelledAndMidSearchCancellation(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{3, 2, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{2}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})

	if _, err := reader.Search(nil, []float32{0}, 1, vector.SearchOptions{}); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil context error = %v", err)
	}
	preCancelled, preCancel := context.WithCancel(context.Background())
	preCancel()
	if _, err := reader.Search(preCancelled, []float32{0}, 1, vector.SearchOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled error = %v", err)
	}

	ctx := newCancelOnErrContext(2)
	result, err := reader.Search(ctx, []float32{0}, 1, vector.SearchOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-search cancellation error = %v", err)
	}
	if result.Hits != nil || result.Stats != (vector.SearchStats{}) || result.Incomplete {
		t.Fatalf("mid-search cancellation result = %+v, want zero result", result)
	}

	const neighborCount = 256
	values := make([]float32, neighborCount+1)
	nodes := make([]mutableNode, neighborCount+1)
	neighbors := make([]NodeOrdinal, neighborCount)
	for i := range nodes {
		values[i] = float32(i + 1)
		nodes[i] = mutableNode{vectorOrdinal: vector.Ordinal(i), links: [][]NodeOrdinal{{}}}
		if i > 0 {
			neighbors[i-1] = NodeOrdinal(i)
		}
	}
	nodes[0].links[0] = neighbors
	config := testSearchConfig()
	config.DefaultVisitLimit = neighborCount + 1
	config.MaxVisitLimit = neighborCount + 1
	largeReader, err := newSearcherFromGraph(space, config, BuildInfo{
		BuildVersion:          BuildVersion,
		LevelGeneratorVersion: LevelGeneratorVersion,
		MaxNeighbors:          128,
		LevelZeroMaxNeighbors: 256,
		EfConstruction:        128,
	}, graphData{values: values, nodes: nodes, entry: 0, hasEntry: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx = newCancelOnErrContext(3)
	if _, err := largeReader.Search(ctx, []float32{0}, 1, vector.SearchOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("large-adjacency cancellation error = %v", err)
	}
}

type malformedFilter struct {
	total   uint32
	allowed int
}

func (f malformedFilter) Allows(vector.Ordinal) bool { return true }
func (f malformedFilter) AllowedOrdinalCount() int   { return f.allowed }
func (f malformedFilter) TotalOrdinalCount() uint32  { return f.total }

func TestSearchRejectsInvalidKOptionsConfigFilterAndReader(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	config := testSearchConfig()
	reader := newTestReader(t, space, config, graphData{
		values: []float32{1},
		nodes:  []mutableNode{{links: [][]NodeOrdinal{{}}}},
		entry:  0, hasEntry: true,
	})

	for _, k := range []int{-1, 0, config.MaxK + 1} {
		if _, err := reader.Search(context.Background(), []float32{0}, k, vector.SearchOptions{}); !errors.Is(err, vector.ErrInvalidK) {
			t.Errorf("k=%d error = %v", k, err)
		}
	}
	for name, options := range map[string]vector.SearchOptions{
		"negative ef":          {EfSearch: -1},
		"negative visit limit": {VisitLimit: -1},
		"ef above max":         {EfSearch: config.MaxEfSearch + 1},
		"visit above max":      {VisitLimit: config.MaxVisitLimit + 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := reader.Search(context.Background(), []float32{0}, 1, options); !errors.Is(err, vector.ErrInvalidSearchOptions) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	invalidConfigs := []SearchConfig{
		{},
		{DefaultEfSearch: 2, MaxEfSearch: 1, DefaultVisitLimit: 1, MaxVisitLimit: 1, MaxK: 1},
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 0, MaxVisitLimit: 1, MaxK: 1},
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 2, MaxVisitLimit: 1, MaxK: 1},
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 1, MaxVisitLimit: 1, MaxK: 0},
		{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 1, MaxVisitLimit: 1, MaxK: 2},
	}
	for i, invalid := range invalidConfigs {
		invalidReader := *reader
		invalidReader.topology.searchConfig = invalid
		if _, err := invalidReader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{}); !errors.Is(err, ErrInvalidSearchConfig) {
			t.Errorf("invalid config %d error = %v", i, err)
		}
	}

	wrongSize := vector.NewFullBitSet(2)
	if _, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{ResultFilter: wrongSize}); !errors.Is(err, vector.ErrResultFilterSizeMismatch) {
		t.Fatalf("wrong filter size error = %v", err)
	}
	for _, allowed := range []int{-1, 2} {
		filter := malformedFilter{total: 1, allowed: allowed}
		if _, err := reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{ResultFilter: filter}); !errors.Is(err, vector.ErrInvalidSearchOptions) {
			t.Errorf("filter allowed count %d error = %v", allowed, err)
		}
	}

	unvalidated := *reader
	unvalidated.topology.validated = false
	if _, err := unvalidated.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{}); !errors.Is(err, ErrInvalidGraph) {
		t.Fatalf("unvalidated reader error = %v", err)
	}
	badLevel := *reader
	badLevel.topology.levels = append([]uint8(nil), reader.topology.levels...)
	badLevel.topology.levels[0] = MaxLevel + 1
	if _, err := badLevel.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{}); !errors.Is(err, ErrInvalidGraph) {
		t.Fatalf("excessive graph level error = %v", err)
	}
	defensiveReader := newTestReader(t, space, config, graphData{
		values: []float32{2, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})
	badNeighbor := *defensiveReader
	badNeighbor.topology.level0Neighbors = append([]NodeOrdinal(nil), defensiveReader.topology.level0Neighbors...)
	badNeighbor.topology.level0Neighbors[0] = 2
	if _, err := badNeighbor.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{}); !errors.Is(err, ErrInvalidGraph) {
		t.Fatalf("out-of-range neighbor error = %v", err)
	}
}

func TestSearchRejectsInvalidQueries(t *testing.T) {
	l2 := testSpace(t, 2, vector.MetricL2Squared)
	l2Reader := newTestReader(t, l2, testSearchConfig(), graphData{
		values: []float32{1, 1},
		nodes:  []mutableNode{{links: [][]NodeOrdinal{{}}}},
		entry:  0, hasEntry: true,
	})
	for name, test := range map[string]struct {
		query []float32
		want  error
	}{
		"dimensions":        {query: []float32{1}, want: vector.ErrDimensionMismatch},
		"nan":               {query: []float32{float32(math.NaN()), 0}, want: vector.ErrNonFiniteVector},
		"positive infinity": {query: []float32{float32(math.Inf(1)), 0}, want: vector.ErrNonFiniteVector},
		"negative infinity": {query: []float32{float32(math.Inf(-1)), 0}, want: vector.ErrNonFiniteVector},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := l2Reader.Search(context.Background(), test.query, 1, vector.SearchOptions{}); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	cosine := testSpace(t, 2, vector.MetricCosine)
	cosineReader := newTestReader(t, cosine, testSearchConfig(), graphData{
		values: []float32{1, 0},
		nodes:  []mutableNode{{links: [][]NodeOrdinal{{}}}},
		entry:  0, hasEntry: true,
	})
	if _, err := cosineReader.Search(context.Background(), []float32{0, 0}, 1, vector.SearchOptions{}); !errors.Is(err, vector.ErrZeroNorm) {
		t.Fatalf("zero cosine query error = %v", err)
	}
}

func TestSearchEmptyReaderCancellationAndZeroAllowedFilter(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	empty := newTestReader(t, space, testSearchConfig(), graphData{})

	result, err := empty.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{})
	requireStats(t, result.Stats, vector.SearchStats{Termination: vector.TerminationComplete})
	if result.Incomplete {
		t.Fatal("empty reader result marked incomplete")
	}

	ctx := newCancelOnErrContext(2)
	result, err = empty.Search(ctx, []float32{0}, 1, vector.SearchOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("empty reader cancellation error = %v", err)
	}
	if result.Hits != nil || result.Stats != (vector.SearchStats{}) || result.Incomplete {
		t.Fatalf("empty reader cancellation result = %+v, want zero result", result)
	}

	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{1, 2},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})
	none, err := vector.NewBitSet(2)
	if err != nil {
		t.Fatal(err)
	}
	result, err = reader.Search(context.Background(), []float32{0}, 1, vector.SearchOptions{ResultFilter: none})
	if err != nil {
		t.Fatal(err)
	}
	requireHits(t, result.Hits, []vector.Hit{})
	requireStats(t, result.Stats, vector.SearchStats{Termination: vector.TerminationComplete})
}

func TestSearchConcurrentReaderUseHasRequestLocalState(t *testing.T) {
	space := testSpace(t, 1, vector.MetricL2Squared)
	reader := newTestReader(t, space, testSearchConfig(), graphData{
		values: []float32{3, 2, 1},
		nodes: []mutableNode{
			{links: [][]NodeOrdinal{{1}}},
			{vectorOrdinal: 1, links: [][]NodeOrdinal{{2}}},
			{vectorOrdinal: 2, links: [][]NodeOrdinal{{}}},
		},
		entry: 0, hasEntry: true,
	})
	filter, err := vector.NewBitSet(3, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []vector.Hit{{Ordinal: 2, Distance: 1}, {Ordinal: 1, Distance: 4}}

	var wg sync.WaitGroup
	errorsCh := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := reader.Search(context.Background(), []float32{0}, 2, vector.SearchOptions{ResultFilter: filter})
			if err != nil {
				errorsCh <- err
				return
			}
			if !slices.Equal(result.Hits, want) {
				errorsCh <- errors.New("concurrent Reader.Search result mismatch")
			}
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
}
