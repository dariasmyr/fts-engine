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
	live                    vector.BitSet
	vectorIDs               []VectorID
	refs                    map[VectorID]chunk.Ref
	maxK                    int
	maxChunkCandidates      int
	maxChunksPerDocumentHit int
}

func (s *Service) SearchChunks(ctx context.Context, query []float32, k int) (ChunkSearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return searchChunks(ctx, s.searchViewLocked(), query, k)
}

func searchChunks(ctx context.Context, view searchView, query []float32, k int) (ChunkSearchResult, error) {
	return searchChunksUpTo(ctx, view, query, k, view.maxK)
}

func searchChunksUpTo(ctx context.Context, view searchView, query []float32, k, maxK int) (ChunkSearchResult, error) {
	if k <= 0 || k > maxK {
		return ChunkSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, maxK)
	}
	result, err := view.searcher.Search(ctx, query, k, vector.SearchOptions{ResultFilter: view.live})
	if err != nil {
		return ChunkSearchResult{}, err
	}
	hits := make([]ChunkHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		index := int(hit.Ordinal)
		if index >= len(view.vectorIDs) {
			return ChunkSearchResult{}, ErrInternalState
		}
		ref, ok := view.refs[view.vectorIDs[index]]
		if !ok {
			return ChunkSearchResult{}, ErrInternalState
		}
		hits = append(hits, ChunkHit{Ref: ref, Distance: hit.Distance})
	}
	return ChunkSearchResult{Hits: hits, Stats: result.Stats, Incomplete: result.Incomplete}, nil
}

func (s *Service) SearchDocuments(ctx context.Context, query []float32, k int) (DocumentSearchResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return searchDocuments(ctx, s.searchViewLocked(), query, k)
}

func searchDocuments(ctx context.Context, view searchView, query []float32, k int) (DocumentSearchResult, error) {
	if k <= 0 || k > view.maxK {
		return DocumentSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, view.maxK)
	}
	liveCount := view.live.AllowedOrdinalCount()
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

func (s *Service) searchViewLocked() searchView {
	return searchView{
		space: s.space, searcher: s.head, live: s.live, vectorIDs: s.vectorIDs, refs: s.refByVector,
		maxK: s.config.MaxK, maxChunkCandidates: s.config.MaxChunkCandidates,
		maxChunksPerDocumentHit: s.config.MaxChunksPerDocumentHit,
	}
}
