package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

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
		return maxCandidates, nil
	}
	if candidateChunks < 0 {
		return 0, fmt.Errorf("%w: candidate chunks %d, max %d", ErrInvalidSearchOptions, candidateChunks, maxCandidates)
	}
	return min(candidateChunks, maxCandidates), nil
}

func searchSegmentChunksPrepared(ctx context.Context, calculator vector.Calculator, view segmentView, query vector.PreparedQuery, k int, searchOptions hnsw.SearchOptions) (chunkSearchResult, error) {
	if k <= 0 {
		return chunkSearchResult{}, fmt.Errorf("%w: got %d", vector.ErrInvalidK, k)
	}
	if ctx == nil {
		return chunkSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return chunkSearchResult{}, err
	}
	if err := calculator.ValidatePreparedQuery(query); err != nil {
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
