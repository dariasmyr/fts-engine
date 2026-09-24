package flat

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Searcher is an immutable exact-search index backed by a VectorSource.
type Searcher struct {
	source *VectorSource
	maxK   int
}

func newReader(calculator vector.Calculator, maxK int, values []float32) *Searcher {
	return &Searcher{source: newVectorSource(calculator, values), maxK: maxK}
}

func (r *Searcher) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	return Search(ctx, r.source.calculator, r.source.values, r.maxK, query, k, options)
}

// Compact returns an immutable reader containing only rows allowed by filter.
// Retained rows preserve their original order and prepared float32 values.
func (r *Searcher) Compact(ctx context.Context, filter vector.ResultFilter) (*Searcher, error) {
	values, err := compactPrepared(ctx, r.source.Dimensions(), r.source.values, filter)
	if err != nil {
		return nil, err
	}
	return newReader(r.source.calculator, r.maxK, values), nil
}

func (r *Searcher) Len() int { return r.source.Len() }

func (r *Searcher) Dimensions() int { return r.source.Dimensions() }

func (r *Searcher) Metric() vector.Metric { return r.source.Metric() }

func (r *Searcher) Normalization() vector.Normalization { return r.source.Normalization() }

func (r *Searcher) MaxK() int { return r.maxK }

// VectorSource returns the authoritative vector storage paired with the index.
func (r *Searcher) VectorSource() *VectorSource {
	if r == nil {
		return nil
	}
	return r.source
}

// Close is present for parity with future file-backed readers.
func (r *Searcher) Close() error { return nil }
