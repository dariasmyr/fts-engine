package semantic

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/internal/vector/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func publishIndex(ctx context.Context, base *ReadView, locations map[uint64]vectorLocation, disabledIDs []uint64, pending *segment, pendingComponent, generation uint64) (*ReadView, map[uint64]vectorLocation, error) {
	segments := append([]visibleSegment(nil), base.segments...)
	componentIndexes := make(map[uint64]int, len(segments))
	for i, view := range segments {
		if view.segment == nil {
			return nil, nil, ErrInternalState
		}
		componentIndexes[view.segment.componentID()] = i
	}
	type componentChanges struct {
		disallowed []vector.Ordinal
	}
	changesByComponent := make(map[uint64]*componentChanges)
	for idIndex, id := range disabledIDs {
		if err := contextcheck.PeriodicError(ctx, idIndex); err != nil {
			return nil, nil, err
		}

		location, ok := locations[id]
		if !ok {
			return nil, nil, ErrInternalState
		}
		if _, ok := componentIndexes[location.component]; !ok {
			return nil, nil, ErrInternalState
		}
		componentChange := changesByComponent[location.component]
		if componentChange == nil {
			componentChange = &componentChanges{}
			changesByComponent[location.component] = componentChange
		}
		componentChange.disallowed = append(componentChange.disallowed, location.ordinal)
	}
	for componentID, change := range changesByComponent {
		index := componentIndexes[componentID]
		filter, err := segments[index].filter.WithChanges(segments[index].filter.TotalOrdinalCount(), nil, change.disallowed)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: update segment filter: %v", ErrInternalState, err)
		}
		segments[index].filter = filter
	}
	resultLocations, err := cloneLocations(ctx, locations)
	if err != nil {
		return nil, nil, err
	}
	if pending != nil {
		segments = append(segments, visibleSegment{segment: pending, filter: vector.NewFullBitSet(uint32(pending.len()))})
		for ordinal, row := range pending.rows {
			resultLocations[row.VectorID] = vectorLocation{component: pendingComponent, ordinal: vector.Ordinal(ordinal)}
		}
	}
	view, err := newReadView(ctx, generation, segments, base.descriptor, searchPolicy{
		MaxDocumentsPerSearch:   base.maxDocumentsPerSearch,
		MaxChunkCandidates:      base.maxCandidates,
		MaxChunksPerDocumentHit: base.maxChunksPerDocumentHit,
		MaxQueryChunks:          base.maxQueryChunks,
	}, base.search)
	if err != nil {
		return nil, nil, err
	}
	return view, resultLocations, nil
}
func cloneLocations(ctx context.Context, source map[uint64]vectorLocation) (map[uint64]vectorLocation, error) {
	result := make(map[uint64]vectorLocation, len(source))
	i := 0
	for id, location := range source {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		result[id] = location
		i++
	}
	return result, nil
}
