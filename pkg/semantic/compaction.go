package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
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
	revision := i.state.revision
	view := i.state.view
	config := i.config
	segmentID := i.state.nextSegmentID
	hasPending := i.state.revision != i.state.view.revision
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
	i.stateMu.Lock()
	defer i.stateMu.Unlock()
	if revision != i.state.revision {
		return ErrInternalState
	}
	i.state.view = compactedView
	i.state.locations = locations
	if allocatedSegment {
		i.state.nextSegmentID++
	}
	return nil
}

func compactView(ctx context.Context, view *IndexView, segmentID SegmentID, config IndexConfig) (*IndexView, map[VectorID]vectorLocation, bool, error) {
	var compacted *segment
	if view.liveCount > 0 {
		var err error
		compacted, err = buildCompactedSegment(ctx, view, segmentID, config)
		if err != nil {
			return nil, nil, false, err
		}
	}

	locations := make(map[VectorID]vectorLocation, view.liveCount)
	if compacted != nil {
		for ordinal, row := range compacted.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return nil, nil, false, err
			}
			locations[row.VectorID] = vectorLocation{segment: segmentID, ordinal: vector.Ordinal(ordinal)}
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
	space, err := vector.NewSpace(config.Dimensions, config.Metric)
	if err != nil {
		return nil, err
	}
	source, err := memorystore.NewPrepared(space, flatVectors)
	if err != nil {
		return nil, err
	}
	hnswConfig := config.hnswConfig()
	return buildSegment(
		ctx,
		segmentID,
		space,
		source,
		liveRows,
		hnswConfig,
	)
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
