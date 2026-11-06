package semantic

import (
	"context"
	"slices"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// SearchEncoded searches an immutable snapshot using already encoded query
// chunks. Chunk references are not used for ranking; only the prepared vectors
// participate in similarity search.
func (s *Snapshot) SearchEncoded(
	ctx context.Context,
	queries []EncodedChunk,
	k int,
	options SearchOptions,
) (DocumentSearchResult, error) {
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
		if err := ctx.Err(); err != nil {
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

	// Preserve deterministic search order independently of the original query
	// chunk ordering.
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

func (s *Snapshot) searchEncodedQueries(
	ctx context.Context,
	queries []vector.PreparedQuery,
	k int,
	options SearchOptions,
) (DocumentSearchResult, error) {
	if s.liveCount == 0 {
		return DocumentSearchResult{
			Hits:  []DocumentHit{},
			Stats: hnsw.SearchStats{Termination: hnsw.TerminationComplete},
		}, nil
	}
	candidates, result, err := s.collectChunkCandidates(ctx, queries, options)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	result.Hits, result.DistinctDocuments, err = groupDocumentHits(
		ctx,
		candidates,
		k,
		s.maxChunksPerDocumentHit,
	)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return result, nil
}
