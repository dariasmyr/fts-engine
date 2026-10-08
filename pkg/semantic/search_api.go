package semantic

import (
	"context"
	"slices"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Search searches the committed snapshot using already encoded query chunks.
// The chunk references are not used for ranking; vectors are prepared against
// the index embedding schema.
func (i *Index) Search(ctx context.Context, queries []EncodedChunk, k int, options SearchOptions) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}
	snapshot, err := i.snapshotForRead(ctx)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return snapshot.SearchEncoded(ctx, queries, k, options)
}

// SearchEncoded searches this immutable snapshot without invoking an Encoder.
func (s *Snapshot) SearchEncoded(ctx context.Context, queries []EncodedChunk, k int, options SearchOptions) (DocumentSearchResult, error) {
	if s == nil {
		return DocumentSearchResult{}, ErrInvalidSegment
	}
	if err := s.validateSearchLimits(ctx, k, options); err != nil {
		return DocumentSearchResult{}, err
	}
	if len(queries) == 0 || len(queries) > s.maxQueryChunks {
		return DocumentSearchResult{}, ErrInvalidQuery
	}
	type preparedInput struct {
		query vector.PreparedQuery
		key   []float32
	}
	inputs := make([]preparedInput, len(queries))
	for n, item := range queries {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return DocumentSearchResult{}, err
		}
		query, err := s.calculator.PrepareQuery(item.Vector)
		if err != nil {
			return DocumentSearchResult{}, err
		}
		key, err := s.calculator.Prepare(item.Vector)
		if err != nil {
			return DocumentSearchResult{}, err
		}
		inputs[n] = preparedInput{query: query, key: key}
	}
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}
	slices.SortFunc(inputs, func(a, b preparedInput) int {
		for i := range a.key {
			if a.key[i] < b.key[i] {
				return -1
			}
			if a.key[i] > b.key[i] {
				return 1
			}
		}
		return 0
	})
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}
	prepared := make([]vector.PreparedQuery, len(inputs))
	for n := range inputs {
		prepared[n] = inputs[n].query
	}
	return s.searchEncodedQueries(ctx, prepared, k, options)
}

func (s *Service) SearchDocuments(ctx context.Context, encoder Encoder, query fts.Document, maxResultCount int) (DocumentSearchResult, error) {
	return s.SearchDocumentsWithOptions(ctx, encoder, query, maxResultCount, SearchOptions{})
}

func (s *Service) SearchDocumentsWithOptions(ctx context.Context, encoder Encoder, query fts.Document, maxResultCount int, options SearchOptions) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}
	if err := s.validateEncoder(encoder); err != nil {
		return DocumentSearchResult{}, err
	}
	queries, err := encoder.Encode(ctx, query)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return s.index.Search(ctx, queries, maxResultCount, options)
}
