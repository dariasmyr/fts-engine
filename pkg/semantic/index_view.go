package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// IndexView is one immutable committed semantic index view. It owns neither
// mutable ingest state nor persistence resources, so old views remain usable
// while the index installs newer views.
type IndexView struct {
	segments     []segmentView
	revision     Revision
	liveCount    int
	searchLimits searchBudgetLimits
	resultLimits resultShapeLimits
	searchConfig hnsw.SearchConfig
	space        vector.Space
}

func newIndexView(
	ctx context.Context,
	revision Revision,
	segments []segmentView,
	space vector.Space,
	searchLimits searchBudgetLimits,
	resultLimits resultShapeLimits,
	search hnsw.SearchConfig,
) (*IndexView, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if space.Dimensions() <= 0 || searchLimits.maxChunkCandidates < resultLimits.maxDocumentsPerSearch ||
		searchLimits.maxQueryChunks <= 0 || searchLimits.maxEfSearch <= 0 || searchLimits.maxVisitLimit <= 0 ||
		resultLimits.maxDocumentsPerSearch <= 0 || resultLimits.maxChunksPerDocumentHit <= 0 {
		return nil, ErrInvalidConfig
	}
	view := &IndexView{
		segments:     segments,
		revision:     revision,
		searchLimits: searchLimits,
		resultLimits: resultLimits,
		searchConfig: search,
		space:        space,
	}
	for i, segment := range view.segments {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return nil, err
		}
		if segment.segment == nil || segment.liveness.TotalOrdinalCount() != uint32(segment.segment.len()) {
			return nil, ErrInternalState
		}
		view.liveCount += segment.liveness.AllowedOrdinalCount()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return view, nil
}
