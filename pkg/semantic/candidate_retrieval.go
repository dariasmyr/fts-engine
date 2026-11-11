package semantic

import (
	"context"
	"slices"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// collectChunkCandidates is the ANN retrieval layer. It knows nothing about
// document grouping: it only spends request-wide ANN budgets and returns chunk
// hits.
func (s *IndexView) collectChunkCandidates(
	ctx context.Context,
	queryVectors []vector.PreparedVector,
	config QueryConfig,
) ([]ChunkHit, DocumentSearchResult, error) {
	type chunkKey struct {
		documentID fts.DocID
		chunkID    ChunkID
	}
	result := DocumentSearchResult{Stats: hnsw.SearchStats{Termination: hnsw.TerminationComplete}}
	candidateBudget, _ := resolveCandidateBudget(config.CandidateChunks, s.searchLimits.maxChunkCandidates)
	visitBudget := config.VisitLimit
	if visitBudget == 0 {
		visitBudget = s.searchConfig.VisitLimit
	}
	efSearch := config.EfSearch
	if efSearch == 0 {
		efSearch = s.searchConfig.EfSearch
	}

	active := make([]segmentView, 0, len(s.segments))
	segmentCapacities := make([]int, 0, len(s.segments))
	for i, view := range s.segments {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, DocumentSearchResult{}, err
		}
		if view.liveness.AllowedOrdinalCount() > 0 {
			active = append(active, view)
			segmentCapacities = append(segmentCapacities, view.segment.len())
		}
	}
	visitShares, err := allocateVisitShares(ctx, len(queryVectors), segmentCapacities, visitBudget)
	if err != nil {
		return nil, DocumentSearchResult{}, err
	}

	mergeCapacity := candidateMergeCapacity(
		s.liveCount,
		candidateBudget,
		len(queryVectors),
		len(active),
	)
	merged := make(map[chunkKey]ChunkHit, mergeCapacity)
	visitLimited := false
	mergedHits := 0
	for queryIndex, queryVector := range queryVectors {
		for segmentIndex, view := range active {
			if err := ctx.Err(); err != nil {
				return nil, DocumentSearchResult{}, err
			}
			share := visitShares[queryIndex][segmentIndex]
			if share == 0 {
				visitLimited = true
				continue
			}
			partial, err := searchSegmentChunksPrepared(
				ctx,
				s.space,
				view,
				queryVector,
				min(candidateBudget, view.liveness.AllowedOrdinalCount()),
				hnsw.QueryConfig{EfSearch: efSearch, VisitLimit: share},
			)
			if err != nil {
				return nil, DocumentSearchResult{}, err
			}
			mergeSearchStats(&result.Stats, partial.Stats)
			visitLimited = visitLimited || partial.Incomplete
			for _, hit := range partial.Hits {
				if err := contextcheck.PeriodicError(ctx, mergedHits); err != nil {
					return nil, DocumentSearchResult{}, err
				}
				mergedHits++
				key := chunkKey{documentID: hit.Ref.DocID, chunkID: hit.Ref.ID}
				previous, exists := merged[key]
				if !exists || compareChunkHits(hit, previous) < 0 {
					merged[key] = hit
				}
			}
		}
	}

	candidates := make([]ChunkHit, 0, len(merged))
	i := 0
	for _, hit := range merged {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, DocumentSearchResult{}, err
		}
		candidates = append(candidates, hit)
		i++
	}
	if err := ctx.Err(); err != nil {
		return nil, DocumentSearchResult{}, err
	}
	slices.SortFunc(candidates, compareChunkHits)
	if err := ctx.Err(); err != nil {
		return nil, DocumentSearchResult{}, err
	}
	candidateLimited := candidateBudget < s.liveCount
	if len(candidates) > candidateBudget {
		candidates = candidates[:candidateBudget]
		candidateLimited = true
	}
	result.CandidateChunks = len(candidates)
	result.GroupingIncomplete = visitLimited || candidateLimited
	switch {
	case visitLimited:
		result.Stats.Termination = hnsw.TerminationVisitLimit
	case candidateLimited:
		result.Stats.Termination = "candidate_limit"
	default:
		result.Stats.Termination = hnsw.TerminationComplete
	}
	return candidates, result, nil
}

func candidateMergeCapacity(liveCount, candidatesPerSearch, queryCount, segmentCount int) int {
	if liveCount <= 0 || candidatesPerSearch <= 0 || queryCount <= 0 || segmentCount <= 0 {
		return 0
	}
	capacity := min(liveCount, candidatesPerSearch)
	if capacity >= liveCount || queryCount > liveCount/capacity {
		return liveCount
	}
	capacity *= queryCount
	if capacity >= liveCount || segmentCount > liveCount/capacity {
		return liveCount
	}
	return capacity * segmentCount
}
