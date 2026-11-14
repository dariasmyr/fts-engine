package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

const defaultCandidateChunks = 20

// segmentView binds an immutable physical segment to an immutable local
// liveness filter. The filter is view state, not mutable segment state.
type segmentView struct {
	segment  *segment
	liveness vector.BitSet
}

type chunkSearchResult struct {
	Hits       []ChunkHit
	Stats      hnsw.SearchStats
	Incomplete bool
}

func mergeSearchStats(total *hnsw.SearchStats, partial hnsw.SearchStats) {
	total.ConfiguredEfSearch = max(total.ConfiguredEfSearch, partial.ConfiguredEfSearch)
	total.AppliedEfSearch = max(total.AppliedEfSearch, partial.AppliedEfSearch)
	total.VisitedNodes += partial.VisitedNodes
	total.ExpandedNodes += partial.ExpandedNodes
	total.DistanceComputations += partial.DistanceComputations
	total.RejectedNodes += partial.RejectedNodes
	if partial.Termination == hnsw.TerminationVisitLimit || total.Termination == hnsw.TerminationVisitLimit {
		total.Termination = hnsw.TerminationVisitLimit
	} else if total.Termination == "" {
		total.Termination = partial.Termination
	}
}

func resolveCandidateBudget(candidateChunks, maxCandidates int) (int, error) {
	if candidateChunks == 0 {
		return min(defaultCandidateChunks, maxCandidates), nil
	}
	if candidateChunks < 0 || candidateChunks > maxCandidates {
		return 0, fmt.Errorf("%w: candidate chunks %d, max %d", ErrInvalidQueryConfig, candidateChunks, maxCandidates)
	}
	return candidateChunks, nil
}

func resolveEfSearch(efSearch int, defaults hnsw.SearchConfig) int {
	if efSearch == 0 {
		return defaults.EfSearch
	}
	return efSearch
}

func searchSegmentChunksPrepared(ctx context.Context, space vector.Space, view segmentView, query vector.PreparedVector, k int, searchOptions hnsw.QueryConfig) (chunkSearchResult, error) {
	if k <= 0 {
		return chunkSearchResult{}, fmt.Errorf("%w: got %d", vector.ErrInvalidK, k)
	}
	if ctx == nil {
		return chunkSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return chunkSearchResult{}, err
	}
	if err := space.ValidatePreparedVector(query); err != nil {
		return chunkSearchResult{}, err
	}
	if view.liveness.AllowedOrdinalCount() == 0 {
		return chunkSearchResult{Hits: []ChunkHit{}}, nil
	}
	searchOptions.ResultFilter = view.liveness
	result, err := view.segment.searchPrepared(ctx, query, k, searchOptions)
	if err != nil {
		return chunkSearchResult{}, err
	}
	hits := make([]ChunkHit, len(result.Hits))
	for i, hit := range result.Hits {
		row, ok := view.segment.rowAt(int(hit.Ordinal))
		if !ok {
			return chunkSearchResult{}, ErrInternalState
		}
		hits[i] = ChunkHit{Ref: row.Chunk, Distance: hit.Distance}
	}
	return chunkSearchResult{Hits: hits, Stats: result.Stats, Incomplete: result.Incomplete}, nil
}

func compareChunkHits(a, b ChunkHit) int {
	if a.Distance < b.Distance {
		return -1
	}
	if a.Distance > b.Distance {
		return 1
	}
	if a.Ref.ID < b.Ref.ID {
		return -1
	}
	if a.Ref.ID > b.Ref.ID {
		return 1
	}
	return 0
}

func compareDocumentHits(a, b DocumentHit) int {
	if a.Distance < b.Distance {
		return -1
	}
	if a.Distance > b.Distance {
		return 1
	}
	if a.DocID < b.DocID {
		return -1
	}
	if a.DocID > b.DocID {
		return 1
	}
	return 0
}
