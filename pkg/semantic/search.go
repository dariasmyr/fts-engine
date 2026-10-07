package semantic

import (
	"context"
	"fmt"
	"slices"

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

func searchSegmentsChunksPrepared(ctx context.Context, calculator vector.Calculator, views []segmentView, query vector.PreparedQuery, k, maxResults int, searchOptions hnsw.SearchOptions) (chunkSearchResult, error) {
	if k <= 0 || k > maxResults {
		return chunkSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxResults)
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
	if len(views) == 0 {
		return chunkSearchResult{Hits: []ChunkHit{}}, nil
	}
	top := make(rankedHitHeap, 0, k)
	var stats hnsw.SearchStats
	incomplete := false
	for _, view := range views {
		if view.liveness.AllowedOrdinalCount() == 0 {
			continue
		}
		options := searchOptions
		if searchOptions.VisitLimit > 0 {
			remaining := searchOptions.VisitLimit - stats.VisitedNodes
			if remaining <= 0 {
				incomplete = true
				break
			}
			options.VisitLimit = remaining
		}
		options.ResultFilter = view.liveness
		result, err := view.segment.searchPrepared(ctx, query, k, options)
		if err != nil {
			return chunkSearchResult{}, err
		}
		mergeSearchStats(&stats, result.Stats)
		if result.Incomplete {
			incomplete = true
		}
		for _, hit := range result.Hits {
			row, ok := view.segment.rowAt(int(hit.Ordinal))
			if !ok {
				return chunkSearchResult{}, ErrInternalState
			}
			top.add(rankedHit{
				hit:       ChunkHit{Ref: row.Chunk, Distance: hit.Distance},
				component: view.segment.componentID(), ordinal: hit.Ordinal,
			}, k)
		}
	}
	slices.SortFunc(top, compareRankedHits)
	hits := make([]ChunkHit, len(top))
	for i, item := range top {
		hits[i] = item.hit
	}
	stats.Termination = hnsw.TerminationComplete
	if incomplete {
		stats.Termination = hnsw.TerminationVisitLimit
	}
	return chunkSearchResult{Hits: hits, Stats: stats, Incomplete: incomplete}, nil
}

type rankedHit struct {
	hit       ChunkHit
	component SegmentID
	ordinal   vector.Ordinal
}

type rankedHitHeap []rankedHit

func (h *rankedHitHeap) add(hit rankedHit, limit int) {
	if len(*h) < limit {
		*h = append(*h, hit)
		for child := len(*h) - 1; child > 0; {
			parent := (child - 1) / 2
			if compareRankedHits((*h)[child], (*h)[parent]) <= 0 {
				break
			}
			(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
			child = parent
		}
		return
	}
	if compareRankedHits(hit, (*h)[0]) >= 0 {
		return
	}
	(*h)[0] = hit
	for parent := 0; ; {
		left := parent*2 + 1
		if left >= len(*h) {
			return
		}
		worse := left
		right := left + 1
		if right < len(*h) && compareRankedHits((*h)[right], (*h)[left]) > 0 {
			worse = right
		}
		if compareRankedHits((*h)[worse], (*h)[parent]) <= 0 {
			return
		}
		(*h)[parent], (*h)[worse] = (*h)[worse], (*h)[parent]
		parent = worse
	}
}

func compareRankedHits(a, b rankedHit) int {
	if a.hit.Distance < b.hit.Distance {
		return -1
	}
	if a.hit.Distance > b.hit.Distance {
		return 1
	}
	if a.component < b.component {
		return -1
	}
	if a.component > b.component {
		return 1
	}
	if a.ordinal < b.ordinal {
		return -1
	}
	if a.ordinal > b.ordinal {
		return 1
	}
	return 0
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
