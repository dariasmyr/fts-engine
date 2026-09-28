package flat

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// FlatIndex is an immutable exact-search index backed by a VectorStore.
type FlatIndex struct {
	store *VectorStore
	maxK  int
}

func newFlatIndex(calculator vector.Calculator, maxK int, values []float32) *FlatIndex {
	return &FlatIndex{store: newVectorStore(calculator, values), maxK: maxK}
}

func (flatIndex *FlatIndex) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	return Search(ctx, flatIndex.store.calculator, flatIndex.store.values, flatIndex.maxK, query, k, options)
}

// Compact returns an immutable flat index containing only rows allowed by filter.
// Retained rows preserve their original order and prepared float32 values.
func (flatIndex *FlatIndex) Compact(ctx context.Context, filter vector.ResultFilter) (*FlatIndex, error) {
	values, err := compactPrepared(ctx, flatIndex.store.Dimensions(), flatIndex.store.values, filter)
	if err != nil {
		return nil, err
	}
	return newFlatIndex(flatIndex.store.calculator, flatIndex.maxK, values), nil
}

func (flatIndex *FlatIndex) Len() int { return flatIndex.store.Len() }

func (flatIndex *FlatIndex) Dimensions() int { return flatIndex.store.Dimensions() }

func (flatIndex *FlatIndex) Metric() vector.Metric { return flatIndex.store.Metric() }

func (flatIndex *FlatIndex) Normalization() vector.Normalization {
	return flatIndex.store.Normalization()
}

func (flatIndex *FlatIndex) MaxK() int { return flatIndex.maxK }

// Vectors returns the authoritative vector storage paired with the flat index.
func (flatIndex *FlatIndex) Vectors() *VectorStore {
	if flatIndex == nil {
		return nil
	}
	return flatIndex.store
}

// Close is present for parity with future file-backed flat indexes.
func (flatIndex *FlatIndex) Close() error { return nil }
