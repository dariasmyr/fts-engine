package hnsw

import (
	"context"
	"math"
	"reflect"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Index is an immutable HNSW topology paired with authoritative vector storage.
type Index struct {
	topology   topology
	vectors    vector.PreparedVectorStore
	search     SearchConfig
	storage    StorageStats
	workspaces *searchWorkspacePool
}

func newIndex(topology topology, vectors vector.PreparedVectorStore, search SearchConfig) (*Index, error) {
	if vectors == nil || isNilPreparedVectorStore(vectors) || vectors.Len() != len(topology.levels) ||
		vectors.Dimensions() != topology.calculator.Dimensions() || vectors.Metric() != topology.calculator.Metric() ||
		vectors.Normalization() != topology.calculator.Normalization() {
		return nil, ErrBuildSourceMismatch
	}
	if err := search.validate(); err != nil {
		return nil, err
	}
	index := &Index{
		topology:   topology,
		vectors:    vectors,
		search:     search,
		workspaces: newSearchWorkspacePool(),
	}
	index.storage = calculateStorageStats(index)
	return index, nil
}

func newBuiltIndex(
	calculator vector.Calculator,
	search SearchConfig,
	buildInfo BuildInfo,
	graph graphData,
	source vector.PreparedVectorStore,
) (*Index, error) {
	topology, err := packGraph(calculator, buildInfo, graph, graphStatisticsForTrustedGraph(graph))
	if err != nil {
		return nil, err
	}
	return newIndex(topology, source, search)
}

// newIndexFromGraph is the deep-validation constructor used by tests and
// diagnostic paths that start from mutable graph data.
func newIndexFromGraph(
	calculator vector.Calculator,
	search SearchConfig,
	buildInfo BuildInfo,
	graph graphData,
	source vector.PreparedVectorStore,
) (*Index, error) {
	if err := search.validate(); err != nil {
		return nil, err
	}
	stats, err := validateGraphData(calculator, buildInfo, graph)
	if err != nil {
		return nil, err
	}
	topology, err := packGraph(calculator, buildInfo, graph, stats)
	if err != nil {
		return nil, err
	}
	return newIndex(topology, source, search)
}

func (i *Index) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	return search(ctx, i, query, nil, k, options)
}

// PrepareQuery creates a query reusable across HNSW indexes with matching
// dimensions, metric, and normalization.
func (i *Index) PrepareQuery(query []float32) (vector.PreparedQuery, error) {
	if i == nil {
		return vector.PreparedQuery{}, errInvalidGraph
	}
	return i.topology.calculator.PrepareQuery(query)
}

// SearchPrepared searches without preparing the same query again.
func (i *Index) SearchPrepared(ctx context.Context, query vector.PreparedQuery, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	return search(ctx, i, nil, &query, k, options)
}

func (i *Index) Len() int {
	if i == nil {
		return 0
	}
	return len(i.topology.levels)
}

func (i *Index) Dimensions() int {
	if i == nil {
		return 0
	}
	return i.topology.calculator.Dimensions()
}

func (i *Index) Metric() vector.Metric {
	if i == nil {
		return 0
	}
	return i.topology.calculator.Metric()
}

// ValidateSource performs an explicit deep comparison with the vector store
// bound to the index. It is not part of normal search or persistence paths.
func (i *Index) ValidateSource(ctx context.Context, source vector.PreparedVectorStore) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if i == nil || source == nil || isNilPreparedVectorStore(source) ||
		source.Len() != i.Len() || source.Dimensions() != i.Dimensions() ||
		source.Metric() != i.Metric() || source.Normalization() != i.topology.calculator.Normalization() {
		return ErrBuildSourceMismatch
	}
	left, right := reflect.ValueOf(i.vectors), reflect.ValueOf(source)
	if left.Type() == right.Type() && left.Kind() == reflect.Pointer && left.Pointer() == right.Pointer() {
		return nil
	}
	bound := make([]float32, i.Dimensions())
	candidate := make([]float32, i.Dimensions())
	for row := range i.Len() {
		if err := ctx.Err(); err != nil {
			return err
		}
		ordinal := vector.Ordinal(row)
		if err := i.vectors.ReadVectorInto(ctx, ordinal, bound); err != nil {
			return err
		}
		if err := source.ReadVectorInto(ctx, ordinal, candidate); err != nil {
			return err
		}
		for component := range bound {
			if math.Float32bits(bound[component]) != math.Float32bits(candidate[component]) {
				return ErrBuildSourceMismatch
			}
		}
	}
	return nil
}
