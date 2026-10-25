package hnsw

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Index is an immutable HNSW topology paired with authoritative prepared vectors.
type Index struct {
	topology   topology
	vectors    vector.PreparedVectorStore
	search     SearchConfig
	workspaces *searchWorkspacePool
}

func newIndex(topology topology, vectors vector.PreparedVectorStore, search SearchConfig) (*Index, error) {
	if vectors == nil || vectors.Len() != len(topology.levels) ||
		vectors.Dimensions() != topology.calculator.Dimensions() ||
		vectors.Metric() != topology.calculator.Metric() ||
		vectors.Normalization() != topology.calculator.Normalization() {
		return nil, ErrInvalidSource
	}
	if err := search.validate(); err != nil {
		return nil, err
	}
	return &Index{
		topology:   topology,
		vectors:    vectors,
		search:     search,
		workspaces: newSearchWorkspacePool(),
	}, nil
}

func (i *Index) Search(ctx context.Context, query []float32, k int, options SearchOptions) (SearchResult, error) {
	return search(ctx, i, query, nil, k, options)
}

func (i *Index) PrepareQuery(query []float32) (vector.PreparedQuery, error) {
	if i == nil {
		return vector.PreparedQuery{}, errInvalidGraph
	}
	return i.topology.calculator.PrepareQuery(query)
}

func (i *Index) SearchPrepared(ctx context.Context, query vector.PreparedQuery, k int, options SearchOptions) (SearchResult, error) {
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

func (i *Index) Normalization() vector.Normalization {
	if i == nil {
		return 0
	}
	return i.topology.calculator.Normalization()
}
