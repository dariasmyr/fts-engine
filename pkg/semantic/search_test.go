package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// searchChunks exposes the internal ANN candidate stage to semantic package
// tests without restoring a public chunk-search API.
func (s *Service) searchChunks(ctx context.Context, query []float32, k int) (chunkSearchResult, error) {
	s.mu.RLock()
	published := s.published
	maxK := s.config.MaxK
	s.mu.RUnlock()
	return searchSegmentsChunks(ctx, s.space, published.segments, query, k, maxK, vector.SearchOptions{})
}

func (s Snapshot) searchChunks(ctx context.Context, query []float32, k int) (chunkSearchResult, error) {
	return searchSegmentChunks(ctx, fullSegmentView(s.Segment), query, k, s.MaxK)
}
