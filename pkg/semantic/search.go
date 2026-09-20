package semantic

import (
	"context"
	"fmt"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// segmentView binds an immutable physical segment to an immutable local
// liveness filter. The filter is view state, not mutable segment state.
type segmentView struct {
	segment *Segment
	filter  vector.BitSet
}

func searchSegmentChunks(ctx context.Context, view segmentView, query []float32, k, maxResults int) (chunkSearchResult, error) {
	if k <= 0 || k > maxResults {
		return chunkSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxResults)
	}
	result, err := view.segment.Search(ctx, query, k, vector.SearchOptions{ResultFilter: view.filter})
	if err != nil {
		return chunkSearchResult{}, err
	}
	hits := make([]ChunkHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		index := int(hit.Ordinal)
		row, ok := view.segment.rowAt(index)
		if !ok {
			return chunkSearchResult{}, ErrInternalState
		}
		hits = append(hits, ChunkHit{Ref: row.Chunk, Distance: hit.Distance})
	}
	return chunkSearchResult{Hits: hits, Stats: result.Stats, Incomplete: result.Incomplete}, nil
}

// SearchDocuments encodes every query chunk, searches each embedding, and
// merges the results by document using the best query-to-document distance.
func (s *Service) SearchDocuments(ctx context.Context, encoder Encoder, query Document, k int) (DocumentSearchResult, error) {
	return s.SearchDocumentsWithOptions(ctx, encoder, query, k, SearchOptions{})
}

func (s *Service) SearchDocumentsWithOptions(ctx context.Context, encoder Encoder, query Document, k int, options SearchOptions) (DocumentSearchResult, error) {
	if encoder == nil {
		return DocumentSearchResult{}, ErrInvalidConfig
	}
	queries, err := encoder.Encode(ctx, query)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	s.mu.RLock()
	published := s.published
	maxResults := s.config.MaxK
	maxCandidates := s.config.MaxChunkCandidates
	maxChunksPerDocumentHit := s.config.MaxChunksPerDocumentHit
	s.mu.RUnlock()
	merged := make(map[fts.DocID]DocumentHit)
	var result DocumentSearchResult
	for _, item := range queries {
		partial, err := s.searchEncodedDocumentsInPublished(ctx, published, maxResults, maxCandidates, maxChunksPerDocumentHit, item.Vector, k, options)
		if err != nil {
			return DocumentSearchResult{}, err
		}
		result.CandidateChunks += partial.CandidateChunks
		mergeSearchStats(&result.Stats, partial.Stats)
		result.GroupingIncomplete = result.GroupingIncomplete || partial.GroupingIncomplete
		for _, hit := range partial.Hits {
			current, exists := merged[hit.DocID]
			if !exists || hit.Distance < current.Distance {
				merged[hit.DocID] = hit
			}
		}
	}
	result.Hits = make([]DocumentHit, 0, len(merged))
	for _, hit := range merged {
		result.Hits = append(result.Hits, hit)
	}
	slices.SortFunc(result.Hits, func(a, b DocumentHit) int {
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
	})
	result.DistinctDocuments = len(result.Hits)
	if len(result.Hits) > k {
		result.Hits = result.Hits[:k]
	}
	return result, nil
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
	s.mu.RLock()
	published := s.published
	maxResults := s.config.MaxK
	maxCandidates := s.config.MaxChunkCandidates
	maxChunksPerDocumentHit := s.config.MaxChunksPerDocumentHit
	s.mu.RUnlock()
	return s.searchEncodedDocumentsInPublished(ctx, published, maxResults, maxCandidates, maxChunksPerDocumentHit, query, k, options)
}

func (s *Service) searchEncodedDocumentsInPublished(ctx context.Context, published *publishedIndex, maxResults, maxCandidates, maxChunksPerDocumentHit int, query []float32, k int, options SearchOptions) (DocumentSearchResult, error) {
	candidateBudget, err := resolveCandidateBudget(options.CandidateChunks, maxCandidates)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	if k <= 0 || k > maxResults {
		return DocumentSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxResults)
	}
	liveCount := published.liveCount
	if liveCount == 0 {
		if ctx == nil {
			return DocumentSearchResult{}, vector.ErrNilContext
		}
		if err := ctx.Err(); err != nil {
			return DocumentSearchResult{}, err
		}
		if _, err := s.space.Prepare(query); err != nil {
			return DocumentSearchResult{}, err
		}
		return DocumentSearchResult{Hits: []DocumentHit{}}, nil
	}
	budget := min(liveCount, candidateBudget)
	chunks, err := searchSegmentsChunks(ctx, s.space, published.segments, query, budget, candidateBudget, vector.SearchOptions{EfSearch: options.EfSearch, VisitLimit: options.VisitLimit})
	if err != nil {
		return DocumentSearchResult{}, err
	}
	documents := groupDocuments(chunks.Hits, maxChunksPerDocumentHit)
	distinctDocuments := len(documents)
	if len(documents) > k {
		documents = documents[:k]
	}
	return DocumentSearchResult{
		Hits: documents, CandidateChunks: len(chunks.Hits), DistinctDocuments: distinctDocuments,
		GroupingIncomplete: budget < liveCount || chunks.Incomplete, Stats: chunks.Stats,
	}, nil
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

func searchSegmentsChunks(ctx context.Context, space vector.Space, views []segmentView, query []float32, k, maxResults int, searchOptions vector.SearchOptions) (chunkSearchResult, error) {
	if k <= 0 || k > maxResults {
		return chunkSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxResults)
	}
	if ctx == nil {
		return chunkSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return chunkSearchResult{}, err
	}
	if _, err := space.Prepare(query); err != nil {
		return chunkSearchResult{}, err
	}
	if len(views) == 0 {
		return chunkSearchResult{Hits: []ChunkHit{}}, nil
	}
	type rankedHit struct {
		hit       ChunkHit
		component ComponentID
		ordinal   vector.Ordinal
	}
	all := make([]rankedHit, 0, len(views)*k)
	var stats vector.SearchStats
	incomplete := false
	for _, view := range views {
		if view.filter.AllowedOrdinalCount() == 0 {
			continue
		}
		options := searchOptions
		options.ResultFilter = view.filter
		result, err := view.segment.Search(ctx, query, k, options)
		if err != nil {
			return chunkSearchResult{}, err
		}
		stats.VisitedNodes += result.Stats.VisitedNodes
		stats.ExpandedNodes += result.Stats.ExpandedNodes
		stats.DistanceComputations += result.Stats.DistanceComputations
		stats.RejectedNodes += result.Stats.RejectedNodes
		if result.Incomplete {
			incomplete = true
		}
		for _, hit := range result.Hits {
			row, ok := view.segment.rowAt(int(hit.Ordinal))
			if !ok {
				return chunkSearchResult{}, ErrInternalState
			}
			all = append(all, rankedHit{
				hit:       ChunkHit{Ref: row.Chunk, Distance: hit.Distance},
				component: view.segment.ComponentID(), ordinal: hit.Ordinal,
			})
		}
	}
	slices.SortFunc(all, func(a, b rankedHit) int {
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
	})
	if len(all) > k {
		all = all[:k]
	}
	hits := make([]ChunkHit, len(all))
	for i, item := range all {
		hits[i] = item.hit
	}
	stats.Termination = vector.TerminationComplete
	if incomplete {
		stats.Termination = vector.TerminationVisitLimit
	}
	return chunkSearchResult{Hits: hits, Stats: stats, Incomplete: incomplete}, nil
}

func searchDocuments(ctx context.Context, view segmentView, space vector.Space, query []float32, k, maxResults, maxChunkCandidates, maxChunksPerDocumentHit int) (DocumentSearchResult, error) {
	if k <= 0 || k > maxResults {
		return DocumentSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxResults)
	}
	liveCount := view.segment.rowCount()
	liveCount = view.filter.AllowedOrdinalCount()
	if liveCount == 0 {
		if ctx == nil {
			return DocumentSearchResult{}, vector.ErrNilContext
		}
		if err := ctx.Err(); err != nil {
			return DocumentSearchResult{}, err
		}
		if _, err := space.Prepare(query); err != nil {
			return DocumentSearchResult{}, err
		}
		return DocumentSearchResult{Hits: []DocumentHit{}}, nil
	}
	budget := min(liveCount, maxChunkCandidates)
	chunks, err := searchSegmentChunks(ctx, view, query, budget, maxChunkCandidates)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	documents := groupDocuments(chunks.Hits, maxChunksPerDocumentHit)
	distinctDocuments := len(documents)
	if len(documents) > k {
		documents = documents[:k]
	}
	return DocumentSearchResult{
		Hits:               documents,
		CandidateChunks:    len(chunks.Hits),
		DistinctDocuments:  distinctDocuments,
		GroupingIncomplete: budget < liveCount || chunks.Incomplete,
		Stats:              chunks.Stats,
	}, nil
}

func groupDocuments(hits []ChunkHit, maxChunksPerDocumentHit int) []DocumentHit {
	type accumulator struct {
		distance float64
		chunks   []ChunkHit
		seen     map[chunk.ID]struct{}
	}
	byDoc := make(map[fts.DocID]*accumulator)
	for _, hit := range hits {
		group := byDoc[hit.Ref.DocID]
		if group == nil {
			group = &accumulator{distance: hit.Distance, seen: make(map[chunk.ID]struct{})}
			byDoc[hit.Ref.DocID] = group
		}
		if _, duplicate := group.seen[hit.Ref.ID]; duplicate {
			continue
		}
		group.seen[hit.Ref.ID] = struct{}{}
		if len(group.chunks) < maxChunksPerDocumentHit {
			group.chunks = append(group.chunks, hit)
		}
	}
	documents := make([]DocumentHit, 0, len(byDoc))
	for docID, group := range byDoc {
		documents = append(documents, DocumentHit{DocID: docID, Distance: group.distance, Chunks: group.chunks})
	}
	slices.SortFunc(documents, func(a, b DocumentHit) int {
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
	})
	return documents
}
