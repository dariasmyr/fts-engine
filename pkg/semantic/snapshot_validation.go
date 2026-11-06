package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

func (v *Snapshot) validateSearchLimits(ctx context.Context, maxResultCount int, options SearchOptions) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if maxResultCount <= 0 || maxResultCount > v.maxDocumentsPerSearch {
		return fmt.Errorf("%w: got %d, max %d", vector.ErrInvalidK, maxResultCount, v.maxDocumentsPerSearch)
	}
	if _, err := resolveCandidateBudget(options.CandidateChunks, v.maxCandidates); err != nil {
		return err
	}
	if options.EfSearch < 0 || options.EfSearch > v.maxEfSearch ||
		options.VisitLimit < 0 || options.VisitLimit > v.maxVisitLimit {
		return ErrInvalidSearchOptions
	}
	return nil
}

func validateSegmentViews(
	ctx context.Context,
	segments []segmentView,
	schema Schema,
	search hnsw.SearchConfig,
) error {
	components := make(map[SegmentID]struct{}, len(segments))
	type chunkKey struct {
		documentID fts.DocID
		chunkID    ChunkID
	}
	liveChunks := make(map[chunkKey]struct{})
	liveDocumentComponents := make(map[fts.DocID]SegmentID)
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
		got := item.segment.schema
		if err := validateSchemaCompatibility(got, schema); err != nil {
			return err
		}
		component := item.segment.componentID()
		if _, exists := components[component]; exists {
			return ErrInvalidSegment
		}
		components[component] = struct{}{}
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
			if owner, exists := liveDocumentComponents[row.Chunk.DocID]; exists && owner != component {
				return ErrInvalidSegment
			}
			liveDocumentComponents[row.Chunk.DocID] = component
			key := chunkKey{documentID: row.Chunk.DocID, chunkID: row.Chunk.ID}
			if _, exists := liveChunks[key]; exists {
				return ErrInvalidSegment
			}
			liveChunks[key] = struct{}{}
		}
	}
	return nil
}
