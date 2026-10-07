package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// publishSnapshot derives the next immutable snapshot from a committed base.
// The input locations map is owned by the caller and updated transactionally.
func publishSnapshot(ctx context.Context, base *Snapshot, locations map[VectorID]vectorLocation, removedIDs []VectorID, pending *segment, pendingComponent SegmentID, revision Revision) (*Snapshot, map[VectorID]vectorLocation, error) {
	segments := append([]segmentView(nil), base.segments...)
	componentIndexes := make(map[SegmentID]int, len(segments))
	for n, view := range segments {
		if view.segment == nil {
			return nil, nil, ErrInternalState
		}
		componentIndexes[view.segment.componentID()] = n
	}

	changesByComponent := make(map[SegmentID][]vector.Ordinal)
	for n, id := range removedIDs {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, nil, err
		}
		location, ok := locations[id]
		if !ok {
			return nil, nil, ErrInternalState
		}
		if _, ok := componentIndexes[location.component]; !ok {
			return nil, nil, ErrInternalState
		}
		changesByComponent[location.component] = append(changesByComponent[location.component], location.ordinal)
	}

	for componentID, removed := range changesByComponent {
		index := componentIndexes[componentID]
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
			locations[row.VectorID] = vectorLocation{component: pendingComponent, ordinal: vector.Ordinal(ordinal)}
		}
	}

	snapshot, err := newTrustedSnapshot(
		ctx,
		revision,
		segments,
		base.schema,
		searchPolicy{
			MaxDocumentsPerSearch:   base.maxDocumentsPerSearch,
			MaxChunkCandidates:      base.maxCandidates,
			MaxChunksPerDocumentHit: base.maxChunksPerDocumentHit,
			MaxQueryChunks:          base.maxQueryChunks,
		},
		base.searchConfig,
		base.calculator,
	)
	if err != nil {
		return nil, nil, err
	}
	return snapshot, locations, nil
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
