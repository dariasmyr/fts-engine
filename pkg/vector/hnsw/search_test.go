package hnsw

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type atomicCountingPreparedStore struct {
	vector.PreparedVectorStore
	reads atomic.Int64
}

func (s *atomicCountingPreparedStore) ReadVectorInto(
	ctx context.Context,
	ordinal vector.Ordinal,
	dst []float32,
) error {
	s.reads.Add(1)
	return s.PreparedVectorStore.ReadVectorInto(ctx, ordinal, dst)
}

func TestResolveSearchRequestAppliesDefaultsOverridesAndBounds(t *testing.T) {
	t.Parallel()

	defaults := SearchConfig{EfSearch: 8, VisitLimit: 9}
	tests := []struct {
		name             string
		count            int
		k                int
		config           QueryConfig
		wantConfiguredEf int
		wantAppliedEf    int
		wantVisit        int
	}{
		{name: "defaults", count: 10, k: 3, wantConfiguredEf: 8, wantAppliedEf: 8, wantVisit: 9},
		{name: "request overrides", count: 10, k: 2, config: QueryConfig{EfSearch: 5, VisitLimit: 6}, wantConfiguredEf: 5, wantAppliedEf: 5, wantVisit: 6},
		{name: "k raises ef", count: 10, k: 7, config: QueryConfig{EfSearch: 2}, wantConfiguredEf: 2, wantAppliedEf: 7, wantVisit: 9},
		{name: "vector count caps execution budget", count: 4, k: 2, config: QueryConfig{EfSearch: 20, VisitLimit: 20}, wantConfiguredEf: 20, wantAppliedEf: 4, wantVisit: 4},
		{name: "empty index applies zero", count: 0, k: 2, config: QueryConfig{EfSearch: 20}, wantConfiguredEf: 20, wantAppliedEf: 0, wantVisit: 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveSearchRequest(defaults, tt.count, tt.k, tt.config)
			if err != nil {
				t.Fatal(err)
			}
			if got.configuredEfSearch != tt.wantConfiguredEf || got.appliedEfSearch != tt.wantAppliedEf || got.visitLimit != tt.wantVisit {
				t.Fatalf("request budgets = configured ef:%d applied ef:%d visit:%d, want %d/%d/%d", got.configuredEfSearch, got.appliedEfSearch, got.visitLimit, tt.wantConfiguredEf, tt.wantAppliedEf, tt.wantVisit)
			}
		})
	}
}

func TestSearchReportsVisitLimit(t *testing.T) {
	t.Parallel()

	index := newCompleteTestIndex(t, [][]float32{{10}, {3}, {2}, {1}})
	result, err := index.Search(context.Background(), []float32{0}, 1, QueryConfig{EfSearch: 4, VisitLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Incomplete || result.Stats.Termination != TerminationVisitLimit {
		t.Fatalf("termination = %q, incomplete = %t", result.Stats.Termination, result.Incomplete)
	}
	if result.Stats.VisitedNodes != 1 || result.Stats.DistanceComputations != 1 {
		t.Fatalf("stats = %+v, want one visited node and distance computation", result.Stats)
	}
}

func TestSearchEfSearchWidensBeam(t *testing.T) {
	t.Parallel()

	index := newLinkedTestIndex(
		t,
		[][]float32{{10}, {1}, {5}, {0}},
		[][]nodeOrdinal{{1, 2}, {0}, {0, 3}, {2}},
	)
	narrow, err := index.Search(context.Background(), []float32{0}, 1, QueryConfig{EfSearch: 1, VisitLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	wide, err := index.Search(context.Background(), []float32{0}, 1, QueryConfig{EfSearch: 2, VisitLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(narrow.Hits) != 1 || narrow.Hits[0].Ordinal != 1 {
		t.Fatalf("efSearch=1 hits = %+v, want local minimum ordinal 1", narrow.Hits)
	}
	if len(wide.Hits) != 1 || wide.Hits[0].Ordinal != 3 {
		t.Fatalf("efSearch=2 hits = %+v, want global minimum ordinal 3", wide.Hits)
	}
	if wide.Stats.VisitedNodes <= narrow.Stats.VisitedNodes {
		t.Fatalf("visited nodes = narrow:%d wide:%d, want wider beam to visit more", narrow.Stats.VisitedNodes, wide.Stats.VisitedNodes)
	}
}

func TestSearchUsesRejectedNodesForFilteredNavigation(t *testing.T) {
	t.Parallel()

	index := newCompleteTestIndex(t, [][]float32{{10}, {3}, {2}, {1}})
	filter, err := vector.NewBitSet(4, vector.Ordinal(3))
	if err != nil {
		t.Fatal(err)
	}
	result, err := index.Search(
		context.Background(),
		[]float32{0},
		1,
		QueryConfig{EfSearch: 4, VisitLimit: 4, ResultFilter: filter},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Ordinal != 3 {
		t.Fatalf("filtered hits = %+v, want ordinal 3", result.Hits)
	}
	if result.Stats.RejectedNodes == 0 {
		t.Fatalf("stats = %+v, want rejected navigation nodes", result.Stats)
	}

	wrongSize, err := vector.NewBitSet(3, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = index.Search(context.Background(), []float32{0}, 1, QueryConfig{ResultFilter: wrongSize})
	if !errors.Is(err, vector.ErrResultFilterSizeMismatch) {
		t.Fatalf("filter size error = %v, want ErrResultFilterSizeMismatch", err)
	}
}

func TestSearchAndSearchPreparedAgree(t *testing.T) {
	t.Parallel()

	index := newCompleteTestIndex(t, [][]float32{{-2}, {-1}, {1}, {2}})
	query := []float32{0.25}
	prepared, err := index.PrepareQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := index.Search(context.Background(), query, 3, QueryConfig{EfSearch: 4, VisitLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := index.SearchPrepared(context.Background(), prepared, 3, QueryConfig{EfSearch: 4, VisitLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ready, raw) {
		t.Fatalf("SearchPrepared() = %+v, Search() = %+v", ready, raw)
	}
}

func TestSearchMemoryFastPathMatchesGenericFallback(t *testing.T) {
	for _, metric := range []vector.Metric{vector.MetricCosine, vector.MetricL2Squared} {
		t.Run(metric.String(), func(t *testing.T) {
			t.Parallel()

			const dimensions = 8
			values := make([][]float32, 64)
			for row := range values {
				values[row] = make([]float32, dimensions)
				for dimension := range dimensions {
					values[row][dimension] = float32((row+1)*(dimension+3)%29+1) / 30
				}
			}
			space, err := vector.NewSpace(dimensions, metric)
			if err != nil {
				t.Fatal(err)
			}
			store, err := memorystore.New(space, values)
			if err != nil {
				t.Fatal(err)
			}
			config := Config{
				Build:  BuildConfig{MaxNeighbors: 8, EfConstruction: 32, Seed: 7},
				Search: SearchConfig{EfSearch: 32, VisitLimit: len(values)},
			}
			fast, err := Build(t.Context(), store, config)
			if err != nil {
				t.Fatal(err)
			}
			if fast.borrowedVectors == nil {
				t.Fatal("memory-backed index did not retain a borrowed matrix")
			}
			before := slices.Clone(fast.borrowedVectors)
			counting := &atomicCountingPreparedStore{PreparedVectorStore: store}
			fallback, err := Restore(Snapshot(fast), counting, config)
			if err != nil {
				t.Fatal(err)
			}
			if fallback.borrowedVectors != nil {
				t.Fatal("generic store unexpectedly enabled the memory fast path")
			}
			query := values[17]
			prepared, err := fast.PrepareQuery(query)
			if err != nil {
				t.Fatal(err)
			}
			request := QueryConfig{EfSearch: 32, VisitLimit: len(values)}

			fastRaw, err := fast.Search(t.Context(), query, 10, request)
			if err != nil {
				t.Fatal(err)
			}
			fallbackRaw, err := fallback.Search(t.Context(), query, 10, request)
			if err != nil {
				t.Fatal(err)
			}
			fastPrepared, err := fast.SearchPrepared(t.Context(), prepared, 10, request)
			if err != nil {
				t.Fatal(err)
			}
			fallbackPrepared, err := fallback.SearchPrepared(t.Context(), prepared, 10, request)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(fastRaw, fallbackRaw) || !reflect.DeepEqual(fastPrepared, fallbackPrepared) {
				t.Fatalf("fast path differs from fallback: raw fast=%+v fallback=%+v; prepared fast=%+v fallback=%+v", fastRaw, fallbackRaw, fastPrepared, fallbackPrepared)
			}
			wantReads := int64(fallbackRaw.Stats.DistanceComputations + fallbackPrepared.Stats.DistanceComputations)
			if got := counting.reads.Load(); got != wantReads {
				t.Fatalf("fallback reads = %d, want one per distance computation (%d)", got, wantReads)
			}
			if !slices.Equal(fast.borrowedVectors, before) {
				t.Fatal("search modified the borrowed matrix")
			}
		})
	}
}

func TestSearchMemoryFastPathIsConcurrentSafe(t *testing.T) {
	const (
		workers    = 8
		iterations = 20
	)
	index := newCompleteTestIndex(t, [][]float32{{-3}, {-2}, {-1}, {0}, {1}, {2}, {3}})
	query := []float32{0.25}
	prepared, err := index.PrepareQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	want, err := index.SearchPrepared(t.Context(), prepared, 4, QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := range iterations {
				var got SearchResult
				var searchErr error
				if (worker+iteration)%2 == 0 {
					got, searchErr = index.Search(t.Context(), query, 4, QueryConfig{})
				} else {
					got, searchErr = index.SearchPrepared(t.Context(), prepared, 4, QueryConfig{})
				}
				if searchErr != nil {
					errCh <- searchErr
					return
				}
				if !reflect.DeepEqual(got, want) {
					errCh <- errors.New("concurrent search result differs from baseline")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestSearchOrdersEqualDistancesByOrdinal(t *testing.T) {
	t.Parallel()

	index := newCompleteTestIndex(t, [][]float32{{-1}, {1}, {-1}, {1}})
	result, err := index.Search(context.Background(), []float32{0}, 4, QueryConfig{EfSearch: 4, VisitLimit: 4})
	if err != nil {
		t.Fatal(err)
	}
	for ordinal, hit := range result.Hits {
		if hit.Ordinal != vector.Ordinal(ordinal) || hit.Distance != 1 {
			t.Fatalf("hit %d = %+v, want ordinal %d at distance 1", ordinal, hit, ordinal)
		}
	}
}

func newCompleteTestIndex(t *testing.T, values [][]float32) *Index {
	t.Helper()

	links := make([][]nodeOrdinal, len(values))
	for node := range values {
		links[node] = make([]nodeOrdinal, 0, len(values)-1)
		for neighbor := range values {
			if neighbor != node {
				links[node] = append(links[node], nodeOrdinal(neighbor))
			}
		}
	}
	return newLinkedTestIndex(t, values, links)
}

func newLinkedTestIndex(t *testing.T, values [][]float32, links [][]nodeOrdinal) *Index {
	t.Helper()
	if len(links) != len(values) {
		t.Fatalf("link rows = %d, vector rows = %d", len(links), len(values))
	}

	space, err := vector.NewSpace(1, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.New(space, values)
	if err != nil {
		t.Fatal(err)
	}
	nodes := len(values)
	offsets := make([]uint32, nodes+1)
	neighbors := []nodeOrdinal{}
	for node, nodeLinks := range links {
		offsets[node] = uint32(len(neighbors))
		neighbors = append(neighbors, nodeLinks...)
	}
	offsets[nodes] = uint32(len(neighbors))
	config := Config{
		Build:  BuildConfig{MaxNeighbors: max(2, nodes), EfConstruction: max(2, nodes), Seed: 1},
		Search: SearchConfig{EfSearch: max(1, nodes), VisitLimit: max(1, nodes)},
	}
	index, err := newIndex(topology{
		space:            space,
		build:            config.Build.info(),
		levels:           make([]uint8, nodes),
		entry:            0,
		hasEntry:         nodes > 0,
		level0Offsets:    offsets,
		level0Neighbors:  neighbors,
		upperNodeOffsets: make([]uint32, nodes+1),
		upperLinkOffsets: []uint32{0},
	}, store, config)
	if err != nil {
		t.Fatal(err)
	}
	return index
}
