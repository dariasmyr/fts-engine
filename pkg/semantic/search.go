package semantic

import (
	"context"
	"fmt"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func (s *Service) SearchChunks(ctx context.Context, query []float32, k int) (ChunkSearchResult, error) {
	if k <= 0 || k > s.config.MaxK {
		return ChunkSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, s.config.MaxK)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.searchChunksLocked(ctx, query, k)
}

func (s *Service) searchChunksLocked(ctx context.Context, query []float32, k int) (ChunkSearchResult, error) {
	result, err := s.head.Search(ctx, query, k, vector.SearchOptions{Accept: s.live})
	if err != nil {
		return ChunkSearchResult{}, err
	}
	hits := make([]ChunkHit, 0, len(result.Hits))
	for _, hit := range result.Hits {
		index := int(hit.Ordinal)
		if index >= len(s.vectorIDs) {
			return ChunkSearchResult{}, ErrInternalState
		}
		ref, ok := s.refByVector[s.vectorIDs[index]]
		if !ok {
			return ChunkSearchResult{}, ErrInternalState
		}
		hits = append(hits, ChunkHit{Ref: ref, Distance: hit.Distance})
	}
	return ChunkSearchResult{Hits: hits, Stats: result.Stats, Incomplete: result.Incomplete}, nil
}

func (s *Service) SearchDocuments(ctx context.Context, query []float32, k int) (DocumentSearchResult, error) {
	if k <= 0 || k > s.config.MaxK {
		return DocumentSearchResult{}, fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, k, s.config.MaxK)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	live := s.live.Cardinality()
	if live == 0 {
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
	budget := min(live, s.config.MaxChunkCandidates)
	chunks, err := s.searchChunksLocked(ctx, query, budget)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	documents := s.groupDocuments(chunks.Hits)
	distinctDocuments := len(documents)
	if len(documents) > k {
		documents = documents[:k]
	}
	return DocumentSearchResult{
		Hits:               documents,
		CandidateChunks:    len(chunks.Hits),
		DistinctDocuments:  distinctDocuments,
		GroupingIncomplete: budget < live || chunks.Incomplete,
		Stats:              chunks.Stats,
	}, nil
}

func (s *Service) groupDocuments(hits []ChunkHit) []DocumentHit {
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
		if len(group.chunks) < s.config.MaxChunksPerDocumentHit {
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
