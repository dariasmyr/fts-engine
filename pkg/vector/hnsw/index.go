package hnsw

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Index is an immutable HNSW topology paired with authoritative prepared vectors.
type Index struct {
	topology   topology
	vectors    vector.PreparedVectorStore
	config     Config
	workspaces *searchWorkspacePool
}

func newIndex(topology topology, vectors vector.PreparedVectorStore, config Config) (*Index, error) {
	if vectors == nil || vectors.Len() != len(topology.levels) ||
		vectors.Dimensions() != topology.space.Dimensions() ||
		vectors.Metric() != topology.space.Metric() ||
		vectors.Normalization() != topology.space.Normalization() {
		return nil, ErrInvalidSource
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Index{
		topology:   topology,
		vectors:    vectors,
		config:     config,
		workspaces: newSearchWorkspacePool(),
	}, nil
}

func (i *Index) Search(ctx context.Context, query []float32, k int, config QueryConfig) (SearchResult, error) {
	return search(ctx, i, query, nil, k, config)
}

func (i *Index) PrepareQuery(query []float32) (vector.PreparedVector, error) {
	if i == nil {
		return vector.PreparedVector{}, errInvalidGraph
	}
	return i.topology.space.Prepare(query)
}

func (i *Index) SearchPrepared(ctx context.Context, query vector.PreparedVector, k int, config QueryConfig) (SearchResult, error) {
	return search(ctx, i, nil, &query, k, config)
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
