package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func (v *IndexView) validateSearchLimits(ctx context.Context, maxResultCount int, config QueryConfig) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if maxResultCount <= 0 || maxResultCount > v.resultLimits.maxDocumentsPerSearch {
		return fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, maxResultCount, v.resultLimits.maxDocumentsPerSearch)
	}
	if _, err := resolveCandidateBudget(config.CandidateChunks, v.searchLimits.maxChunkCandidates); err != nil {
		return err
	}
	if config.EfSearch < 0 || config.EfSearch > v.searchLimits.maxEfSearch ||
		config.VisitLimit < 0 || config.VisitLimit > v.searchLimits.maxVisitLimit {
		return ErrInvalidQueryConfig
	}
	return nil
}

// validateLoadedSegments checks cross-segment invariants at the persistence
// boundary. Segments built by Commit and Compact do not require this scan.
func validateLoadedSegments(
	ctx context.Context,
	segments []segmentView,
) error {
	segmentIDs := make(map[SegmentID]struct{}, len(segments))
	type chunkKey struct {
		documentID fts.DocID
		chunkID    ChunkID
	}
	liveChunks := make(map[chunkKey]struct{})
	liveDocumentSegments := make(map[fts.DocID]SegmentID)
	var previousVectorID VectorID
	for segmentIndex, item := range segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if item.segment == nil || item.liveness.TotalOrdinalCount() != uint32(item.segment.len()) {
			return ErrInvalidSegment
		}
		segmentID := item.segment.segmentID()
		if _, exists := segmentIDs[segmentID]; exists {
			return ErrInvalidSegment
		}
		segmentIDs[segmentID] = struct{}{}
		for ordinal, row := range item.segment.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return err
			}
			if previousVectorID >= row.VectorID {
				return ErrInvalidSegment
			}
			previousVectorID = row.VectorID
			if !item.liveness.Allows(vector.Ordinal(ordinal)) {
				continue
			}
			if owner, exists := liveDocumentSegments[row.Chunk.DocID]; exists && owner != segmentID {
				return ErrInvalidSegment
			}
			liveDocumentSegments[row.Chunk.DocID] = segmentID
			key := chunkKey{documentID: row.Chunk.DocID, chunkID: row.Chunk.ID}
			if _, exists := liveChunks[key]; exists {
				return ErrInvalidSegment
			}
			liveChunks[key] = struct{}{}
		}
	}
	return nil
}
