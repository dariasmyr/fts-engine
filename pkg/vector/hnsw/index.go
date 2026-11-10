package hnsw

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
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
		vectors.Dimensions() != topology.space.Dimensions() ||
		vectors.Metric() != topology.space.Metric() ||
		vectors.Normalization() != topology.space.Normalization() {
		return nil, ErrInvalidSource
	}
	if err := search.Validate(); err != nil {
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

func (i *Index) PrepareQuery(query []float32) (vector.PreparedVector, error) {
	if i == nil {
		return vector.PreparedVector{}, errInvalidGraph
	}
	return i.topology.space.Prepare(query)
}

func (i *Index) SearchPrepared(ctx context.Context, query vector.PreparedVector, k int, options SearchOptions) (SearchResult, error) {
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
	return i.topology.space.Dimensions()
}

func (i *Index) Metric() vector.Metric {
	if i == nil {
		return 0
	}
	return i.topology.space.Metric()
}

func (i *Index) Normalization() vector.Normalization {
	if i == nil {
		return 0
	}
	return i.topology.space.Normalization()
}
