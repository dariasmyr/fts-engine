package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// buildCommittedView derives the next immutable view from a committed base.
// The input locations map is owned by the caller and updated transactionally.
func buildCommittedView(ctx context.Context, base *IndexView, locations map[VectorID]vectorLocation, removedIDs []VectorID, pending *segment, pendingSegmentID SegmentID, revision Revision) (*IndexView, map[VectorID]vectorLocation, error) {
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
		location, ok := locations[id]
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

	// Validate every removal before mutating the owned locations map.
	for _, id := range removedIDs {
		delete(locations, id)
	}

	if pending != nil {
		segments = append(segments, segmentView{
			segment:  pending,
			liveness: vector.NewFullBitSet(uint32(pending.len())),
		})
		for ordinal, row := range pending.rows {
			locations[row.VectorID] = vectorLocation{segment: pendingSegmentID, ordinal: vector.Ordinal(ordinal)}
		}
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

func cloneLocations(ctx context.Context, source map[VectorID]vectorLocation) (map[VectorID]vectorLocation, error) {
	result := make(map[VectorID]vectorLocation, len(source))
	n := 0
	for id, location := range source {
		if n%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		result[id] = location
		n++
	}
	return result, nil
}
