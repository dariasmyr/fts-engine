package semantic

import (
	"context"
	"fmt"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// visibleSegment binds an immutable physical segment to an immutable local
// liveness filter. The filter is view state, not mutable segment state.
type visibleSegment struct {
	segment *segment
	filter  vector.BitSet
}

type chunkSearchResult struct {
	Hits       []ChunkHit
	Stats      vector.SearchStats
	Incomplete bool
}

// SearchDocuments encodes every query chunk, searches each embedding, and
// merges the results by document using the best query-to-document distance.
func (s *Service) SearchDocuments(ctx context.Context, encoder Encoder, query fts.Document, maxResultCount int) (DocumentSearchResult, error) {
	return s.SearchDocumentsWithOptions(ctx, encoder, query, maxResultCount, SearchOptions{})
}

func (s *Service) SearchDocumentsWithOptions(ctx context.Context, encoder Encoder, query fts.Document, maxResultCount int, options SearchOptions) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}
	published, err := s.readView(ctx)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return published.SearchDocumentsWithOptions(ctx, encoder, query, maxResultCount, options)
}

func mergeSearchStats(total *vector.SearchStats, partial vector.SearchStats) {
	total.VisitedNodes += partial.VisitedNodes
	total.ExpandedNodes += partial.ExpandedNodes
	total.DistanceComputations += partial.DistanceComputations
	total.RejectedNodes += partial.RejectedNodes
	if partial.Termination == vector.TerminationVisitLimit || total.Termination == vector.TerminationVisitLimit {
		total.Termination = vector.TerminationVisitLimit
	} else if total.Termination == "" {
		total.Termination = partial.Termination
	}
}

func (s *Service) searchEncodedDocuments(ctx context.Context, query []float32, k int) (DocumentSearchResult, error) {
	return s.searchEncodedDocumentsWithOptions(ctx, query, k, SearchOptions{})
}

func (s *Service) searchEncodedDocumentsWithOptions(ctx context.Context, query []float32, k int, options SearchOptions) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}
	published, err := s.readView(ctx)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	if published == nil {
		return DocumentSearchResult{}, ErrInvalidSegment
	}
	if err := published.validateSearchLimits(ctx, k, options); err != nil {
		return DocumentSearchResult{}, err
	}
	prepared, err := published.calculator.PrepareQuery(query)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return published.searchEncodedQueries(ctx, []vector.PreparedQuery{prepared}, k, options)
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

func searchSegmentsChunks(ctx context.Context, calculator vector.Calculator, views []visibleSegment, query []float32, k, maxResults int, searchOptions vector.SearchOptions) (chunkSearchResult, error) {
	prepared, err := calculator.PrepareQuery(query)
	if err != nil {
		return chunkSearchResult{}, err
	}
	return searchSegmentsChunksPrepared(ctx, calculator, views, prepared, k, maxResults, searchOptions)
}

func searchSegmentsChunksPrepared(ctx context.Context, calculator vector.Calculator, views []visibleSegment, query vector.PreparedQuery, k, maxResults int, searchOptions vector.SearchOptions) (chunkSearchResult, error) {
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
	var stats vector.SearchStats
	incomplete := false
	for _, view := range views {
		if view.filter.AllowedOrdinalCount() == 0 {
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
		options.ResultFilter = view.filter
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
	stats.Termination = vector.TerminationComplete
	if incomplete {
		stats.Termination = vector.TerminationVisitLimit
	}
	return chunkSearchResult{Hits: hits, Stats: stats, Incomplete: incomplete}, nil
}

type rankedHit struct {
	hit       ChunkHit
	component uint64
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
