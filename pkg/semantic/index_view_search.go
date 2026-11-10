package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// Search validates and prepares raw query embeddings, then searches the
// immutable view. Chunk references are not used for ranking.
func (s *IndexView) Search(
	ctx context.Context,
	queryChunks []ChunkEmbedding,
	k int,
	config QueryConfig,
) (DocumentSearchResult, error) {
	if s == nil {
		return DocumentSearchResult{}, ErrInvalidSegment
	}
	if err := s.validateSearchLimits(ctx, k, config); err != nil {
		return DocumentSearchResult{}, err
	}
	if len(queryChunks) == 0 || len(queryChunks) > s.searchLimits.maxQueryChunks {
		return DocumentSearchResult{}, ErrInvalidQuery
	}

	preparedVectors := make([]vector.PreparedVector, len(queryChunks))
	for i, chunk := range queryChunks {
		if err := ctx.Err(); err != nil {
			return DocumentSearchResult{}, err
		}
		preparedVector, err := s.space.Prepare(chunk.Embedding)
		if err != nil {
			return DocumentSearchResult{}, err
		}
		preparedVectors[i] = preparedVector
	}

	return s.searchPreparedQueries(ctx, preparedVectors, k, config)
}

func (s *IndexView) searchPreparedQueries(
	ctx context.Context,
	queryVectors []vector.PreparedVector,
	k int,
	config QueryConfig,
) (DocumentSearchResult, error) {
	if s.liveCount == 0 {
		return DocumentSearchResult{
			Hits:  []DocumentHit{},
			Stats: hnsw.SearchStats{Termination: hnsw.TerminationComplete},
		}, nil
	}
	candidates, result, err := s.collectChunkCandidates(ctx, queryVectors, config)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	result.Hits, result.DistinctDocuments, err = groupDocumentHits(
		ctx,
		candidates,
		k,
		s.resultLimits.maxChunksPerDocumentHit,
	)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return result, nil
}
