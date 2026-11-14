package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Compact rewrites all live rows from the in-memory committed view into at most
// one immutable segment. It performs no persistence I/O. Pending mutations must
// be committed first.
func (i *Index) Compact(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := i.lockMutations(ctx); err != nil {
		return err
	}
	defer i.unlockMutations()

	i.stateMu.RLock()
	preconditions := i.candidatePreconditionsLocked()
	view := preconditions.base
	config := i.config
	segmentID := preconditions.segmentID
	hasPending := preconditions.revision != view.revision
	i.stateMu.RUnlock()
	if hasPending {
		return ErrPendingMutations
	}

	if len(view.segments) <= 1 {
		hasStaleOrdinals := false
		for _, item := range view.segments {
			hasStaleOrdinals = hasStaleOrdinals || item.liveness.AllowedOrdinalCount() != item.segment.len()
		}
		if !hasStaleOrdinals {
			return nil
		}
	}

	if view.liveCount > 0 && segmentID == SegmentID(math.MaxUint64) {
		return ErrSegmentIDExhausted
	}
	compactedView, locations, allocatedSegment, err := compactView(
		ctx, view, segmentID, config,
	)
	if err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	candidate := stateCandidate{
		preconditions:    preconditions,
		view:             compactedView,
		locations:        locations,
		allocatedSegment: allocatedSegment,
	}
	if err := i.validateCandidate(candidate); err != nil {
		return err
	}
	i.publishToMemory(candidate)
	return nil
}

func compactView(ctx context.Context, view *IndexView, segmentID SegmentID, config IndexConfig) (*IndexView, *locationTable, bool, error) {
	var compacted *segment
	if view.liveCount > 0 {
		var err error
		compacted, err = buildCompactedSegment(ctx, view, segmentID, config)
		if err != nil {
			return nil, nil, false, err
		}
	}

	locations := newLocationTable()
	if compacted != nil {
		for ordinal, row := range compacted.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return nil, nil, false, err
			}
			if err := locations.set(row.VectorID, vectorLocation{segment: segmentID, ordinal: vector.Ordinal(ordinal)}); err != nil {
				return nil, nil, false, err
			}
		}
	}

	var segments []segmentView
	if compacted != nil {
		segments = []segmentView{{
			segment:  compacted,
			liveness: vector.NewFullBitSet(uint32(compacted.len())),
		}}
	}
	hnswConfig := config.hnswConfig()
	compactedView, err := newIndexView(
		ctx,
		view.revision,
		segments,
		view.space,
		config.runtimeLimits().search,
		config.runtimeLimits().results,
		hnswConfig.Search,
	)
	if err != nil {
		return nil, nil, false, err
	}
	return compactedView, locations, compacted != nil, nil
}

func buildCompactedSegment(ctx context.Context, view *IndexView, segmentID SegmentID, config IndexConfig) (*segment, error) {
	flatVectors, liveRows, err := materializeLiveRows(ctx, view)
	if err != nil {
		return nil, err
	}
	return buildMaterializedSegment(ctx, segmentID, flatVectors, liveRows, config)
}

func materializeLiveRows(ctx context.Context, view *IndexView) ([]float32, []VectorRow, error) {
	dimensions := view.space.Dimensions()
	flatVectors := make([]float32, view.liveCount*dimensions)
	liveRows := make([]VectorRow, view.liveCount)
	liveOrdinal := 0
	for _, item := range view.segments {
		for ordinal, row := range item.segment.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return nil, nil, err
			}
			if !item.liveness.Allows(vector.Ordinal(ordinal)) {
				continue
			}
			start := liveOrdinal * dimensions
			dst := flatVectors[start : start+dimensions]
			if err := item.segment.vectorStore().ReadVectorInto(ctx, vector.Ordinal(ordinal), dst); err != nil {
				return nil, nil, err
			}
			liveRows[liveOrdinal] = row
			liveOrdinal++
		}
	}
	if liveOrdinal != view.liveCount {
		return nil, nil, ErrInternalState
	}
	return flatVectors, liveRows, nil
}
