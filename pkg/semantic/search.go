package semantic

import (
	"context"
	"fmt"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type searchView struct {
	space                   vector.Space
	searcher                vector.Searcher
	filter                  vector.ResultFilter
	rows                    []VectorRow
	maxK                    int
	maxChunkCandidates      int
	maxChunksPerDocumentHit int
}

type segmentSearchView struct {
	segment *SealedSegment
	rows    []VectorRow
	filter  vector.ResultFilter
}

type rowFilter struct {
	allowed []bool
	count   int
}

func (f rowFilter) Allows(ord vector.Ordinal) bool {
	return uint64(ord) < uint64(len(f.allowed)) && f.allowed[ord]
}

func (f rowFilter) AllowedOrdinalCount() int { return f.count }

func (f rowFilter) TotalOrdinalCount() uint32 { return uint32(len(f.allowed)) }

func (s *Service) SearchChunks(ctx context.Context, query []float32, k int) (ChunkSearchResult, error) {
	return s.SearchChunksWithOptions(ctx, query, k, SearchOptions{})
}

func (s *Service) SearchChunksWithOptions(ctx context.Context, query []float32, k int, options SearchOptions) (ChunkSearchResult, error) {
	s.mu.RLock()
	views := s.segmentViewsLocked()
	maxK := s.config.MaxK
	s.mu.RUnlock()
	return searchSegmentsChunks(ctx, s.space, views, query, k, maxK, vector.SearchOptions{EfSearch: options.EfSearch, VisitLimit: options.VisitLimit})
}

func searchChunks(ctx context.Context, view searchView, query []float32, k int) (ChunkSearchResult, error) {
	return searchChunksUpTo(ctx, view, query, k, view.maxK)
}

func searchChunksUpTo(ctx context.Context, view searchView, query []float32, k, maxK int) (ChunkSearchResult, error) {
	if k <= 0 || k > maxK {
		return ChunkSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxK)
	}
	result, err := view.searcher.Search(ctx, query, k, vector.SearchOptions{ResultFilter: view.filter})
	if err != nil {
		return ChunkSearchResult{}, err
	}
	hits := make([]ChunkHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		index := int(hit.Ordinal)
		if index >= len(view.rows) {
			return ChunkSearchResult{}, ErrInternalState
		}
		hits = append(hits, ChunkHit{Ref: view.rows[index].Chunk, Distance: hit.Distance})
	}
	return ChunkSearchResult{Hits: hits, Stats: result.Stats, Incomplete: result.Incomplete}, nil
}

func (s *Service) SearchDocuments(ctx context.Context, query []float32, k int) (DocumentSearchResult, error) {
	return s.SearchDocumentsWithOptions(ctx, query, k, SearchOptions{})
}

func (s *Service) SearchDocumentsWithOptions(ctx context.Context, query []float32, k int, options SearchOptions) (DocumentSearchResult, error) {
	s.mu.RLock()
	views := s.segmentViewsLocked()
	maxK := s.config.MaxK
	maxCandidates := s.config.MaxChunkCandidates
	maxChunksPerDocumentHit := s.config.MaxChunksPerDocumentHit
	s.mu.RUnlock()
	if k <= 0 || k > maxK {
		return DocumentSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxK)
	}
	liveCount := 0
	for _, view := range views {
		if view.filter != nil {
			liveCount += view.filter.AllowedOrdinalCount()
		}
	}
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
	budget := min(liveCount, maxCandidates)
	chunks, err := searchSegmentsChunks(ctx, s.space, views, query, budget, maxCandidates, vector.SearchOptions{EfSearch: options.EfSearch, VisitLimit: options.VisitLimit})
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

func (s *Service) segmentViewsLocked() []segmentSearchView {
	if len(s.segments) == 0 {
		return nil
	}
	liveIDs := make(map[VectorID]struct{})
	for _, ids := range s.currentByDoc {
		for _, id := range ids {
			liveIDs[id] = struct{}{}
		}
	}
	views := make([]segmentSearchView, 0, len(s.segments))
	for _, segment := range s.segments {
		rows := segment.Rows()
		allowed := make([]bool, len(rows))
		count := 0
		for ordinal, row := range rows {
			if _, ok := liveIDs[row.VectorID]; ok {
				allowed[ordinal] = true
				count++
			}
		}
		views = append(views, segmentSearchView{segment: segment, rows: rows, filter: rowFilter{allowed: allowed, count: count}})
	}
	return views
}

func searchSegmentsChunks(ctx context.Context, space vector.Space, views []segmentSearchView, query []float32, k, maxK int, searchOptions vector.SearchOptions) (ChunkSearchResult, error) {
	if k <= 0 || k > maxK {
		return ChunkSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxK)
	}
	if ctx == nil {
		return ChunkSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return ChunkSearchResult{}, err
	}
	if _, err := space.Prepare(query); err != nil {
		return ChunkSearchResult{}, err
	}
	if len(views) == 0 {
		return ChunkSearchResult{Hits: []ChunkHit{}}, nil
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
			return ChunkSearchResult{}, err
		}
		stats.VisitedNodes += result.Stats.VisitedNodes
		stats.ExpandedNodes += result.Stats.ExpandedNodes
		stats.DistanceComputations += result.Stats.DistanceComputations
		stats.RejectedNodes += result.Stats.RejectedNodes
		if result.Incomplete {
			incomplete = true
		}
		for _, hit := range result.Hits {
			if int(hit.Ordinal) >= len(view.rows) {
				return ChunkSearchResult{}, ErrInternalState
			}
			all = append(all, rankedHit{
				hit:       ChunkHit{Ref: view.rows[hit.Ordinal].Chunk, Distance: hit.Distance},
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
	return ChunkSearchResult{Hits: hits, Stats: stats, Incomplete: incomplete}, nil
}

func searchDocuments(ctx context.Context, view searchView, query []float32, k int) (DocumentSearchResult, error) {
	if k <= 0 || k > view.maxK {
		return DocumentSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, view.maxK)
	}
	liveCount := len(view.rows)
	if view.filter != nil {
		liveCount = view.filter.AllowedOrdinalCount()
	}
	if liveCount == 0 {
		if ctx == nil {
			return DocumentSearchResult{}, vector.ErrNilContext
		}
		if err := ctx.Err(); err != nil {
			return DocumentSearchResult{}, err
		}
		if _, err := view.space.Prepare(query); err != nil {
			return DocumentSearchResult{}, err
		}
		return DocumentSearchResult{Hits: []DocumentHit{}}, nil
	}
	budget := min(liveCount, view.maxChunkCandidates)
	chunks, err := searchChunksUpTo(ctx, view, query, budget, view.maxChunkCandidates)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	documents := groupDocuments(chunks.Hits, view.maxChunksPerDocumentHit)
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
