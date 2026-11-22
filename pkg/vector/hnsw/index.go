package hnsw

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Index is an immutable HNSW topology paired with authoritative prepared vectors.
type Index struct {
	topology   topology
	matrix     *vector.Matrix
	config     Config
	workspaces *searchWorkspacePool
}

func newIndex(topology topology, matrix *vector.Matrix, config Config) (*Index, error) {
	if matrix == nil || matrix.Len() != len(topology.levels) || matrix.Spec() != topology.spec {
		return nil, ErrInvalidSource
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Index{
		topology:   topology,
		matrix:     matrix,
		config:     config,
		workspaces: newSearchWorkspacePool(),
	}, nil
}

// Matrix returns the immutable in-memory vector matrix retained by the index.
func (i *Index) Matrix() *vector.Matrix {
	if i == nil {
		return nil
	}
	return i.matrix
}

func (i *Index) Search(ctx context.Context, query []float32, k int, config QueryConfig) (SearchResult, error) {
	return search(ctx, i, query, nil, k, config)
}

func (i *Index) PrepareQuery(query []float32) (vector.Prepared, error) {
	if i == nil {
		return vector.Prepared{}, errInvalidGraph
	}
	return i.topology.spec.Prepare(query)
}

func (i *Index) SearchPrepared(ctx context.Context, query vector.Prepared, k int, config QueryConfig) (SearchResult, error) {
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
	return i.topology.spec.Dimensions()
}

func (i *Index) Metric() vector.Metric {
	if i == nil {
		return 0
	}
	return i.topology.spec.Metric()
}

func (i *Index) Normalization() vector.Normalization {
	if i == nil {
		return 0
	}
	return i.topology.spec.Normalization()
}
