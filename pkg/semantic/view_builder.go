package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// buildCommittedView derives the next immutable view and location table from a
// committed base without mutating either published object.
func buildCommittedView(ctx context.Context, base *IndexView, locations *locationTable, removedIDs []VectorID, pending *segment, pendingSegmentID SegmentID, revision Revision) (*IndexView, *locationTable, error) {
	segments := append([]segmentView(nil), base.segments...)
	segmentIndexes := make(map[SegmentID]int, len(segments))
	for n, view := range segments {
		if view.segment == nil {
			return nil, nil, ErrInternalState
		}
		segmentIndexes[view.segment.segmentID()] = n
	}

	changesBySegment := make(map[SegmentID][]vector.Ordinal)
	for n, id := range removedIDs {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, nil, err
		}
		location, ok := locations.get(id)
		if !ok {
			return nil, nil, ErrInternalState
		}
		if _, ok := segmentIndexes[location.segment]; !ok {
			return nil, nil, ErrInternalState
		}
		changesBySegment[location.segment] = append(changesBySegment[location.segment], location.ordinal)
	}

	for segmentID, removed := range changesBySegment {
		index := segmentIndexes[segmentID]
		liveness, err := segments[index].liveness.WithChanges(
			segments[index].liveness.TotalOrdinalCount(), nil, removed,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: update segment liveness: %v", ErrInternalState, err)
		}
		segments[index].liveness = liveness
	}

	var addedLocations map[VectorID]vectorLocation
	if pending != nil {
		addedLocations = make(map[VectorID]vectorLocation, len(pending.rows))
		segments = append(segments, segmentView{
			segment:  pending,
			liveness: vector.NewFullBitSet(uint32(pending.len())),
		})
		for ordinal, row := range pending.rows {
			addedLocations[row.VectorID] = vectorLocation{segment: pendingSegmentID, ordinal: vector.Ordinal(ordinal)}
		}
	}
	locations, err := locations.withChanges(ctx, removedIDs, addedLocations)
	if err != nil {
		return nil, nil, err
	}

	view, err := newIndexView(
		ctx,
		revision,
		segments,
		base.space,
		base.searchLimits,
		base.resultLimits,
		base.searchConfig,
	)
	if err != nil {
		return nil, nil, err
	}
	return view, locations, nil
}
