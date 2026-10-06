package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/internal/vector/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Compact merges all visible live rows into one immutable HNSW segment.
func (s *Service) Compact(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.lockFlush(ctx); err != nil {
		return err
	}
	defer s.unlockFlush()
	if err := s.lockState(ctx); err != nil {
		return err
	}
	revision := s.revision
	published := s.published
	config := s.config
	componentID := s.nextComponentID
	hasPending := s.revision != s.published.revision
	s.unlockState()
	if hasPending {
		return ErrPendingMutations
	}
	if len(published.segments) <= 1 {
		stale := false
		for _, item := range published.segments {
			stale = stale || item.filter.AllowedOrdinalCount() != item.segment.len()
		}
		if !stale {
			return nil
		}
	}
	var compactedSegment *segment
	if published.liveCount > 0 {
		if componentID == uint64(math.MaxUint64) {
			return ErrComponentIDExhausted
		}
		var err error
		compactedSegment, err = buildCompactedSegment(ctx, published, componentID, config)
		if err != nil {
			return err
		}
	}
	locations := make(map[uint64]vectorLocation, published.liveCount)
	for ordinal, row := range compactedSegment.rows {
		if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
			return err
		}

		locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}
	}
	segments := []visibleSegment(nil)
	if compactedSegment != nil {
		segments = []visibleSegment{{segment: compactedSegment, filter: vector.NewFullBitSet(uint32(len(compactedSegment.rows)))}}
	}
	buildOptions, ok := config.hnswOptions()
	if !ok {
		return ErrInvalidConfig
	}
	view, err := newTrustedReadView(ctx, revision, segments, published.descriptor, config.searchPolicy(), buildOptions.Search, published.calculator)
	if err != nil {
		return err
	}
	if err := s.lockState(ctx); err != nil {
		return err
	}
	defer s.unlockState()
	if err := ctx.Err(); err != nil {
		return err
	}
	if revision != s.revision {
		return ErrPublicationConflict
	}
	s.published = view
	s.locations = locations
	if compactedSegment != nil {
		s.nextComponentID++
	}
	return nil
}

// buildCompactedSegment materializes live rows from an immutable read view and
// builds the replacement HNSW segment without touching mutable service state.
func buildCompactedSegment(ctx context.Context, view *ReadView, componentID uint64, config Config) (*segment, error) {
	flatVectors, liveRows, err := materializeLiveRows(ctx, view)
	if err != nil {
		return nil, err
	}
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		return nil, err
	}
	source, err := memorystore.NewPrepared(calculator, flatVectors)
	if err != nil {
		return nil, err
	}
	buildOptions, ok := config.hnswOptions()
	if !ok {
		return nil, ErrInvalidConfig
	}
	segment, err := buildSegment(
		ctx,
		componentID,
		config.pipelineDescriptor(),
		source,
		liveRows,
		buildOptions,
	)
	if err != nil {
		return nil, err
	}
	return segment, nil
}

func materializeLiveRows(ctx context.Context, published *ReadView) ([]float32, []VectorRow, error) {
	// flatVectors is a flat slice of all live vectors in the published segments, concatenated in order.
	// liveRows is a slice of all live vector ids in the published segments, concatenated in order.
	// The two slices are aligned by ordinal: flatVectors[dim*i:dim*(i+1)] is the vector for liveRows[i].
	dimensions := published.calculator.Dimensions()
	flatVectors := make([]float32, published.liveCount*dimensions)
	liveRows := make([]VectorRow, published.liveCount)
	liveOrdinal := 0
	for _, item := range published.segments {
		for ordinal, row := range item.segment.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return nil, nil, err
			}

			if !item.filter.Allows(vector.Ordinal(ordinal)) {
				continue
			}

			start := liveOrdinal * dimensions
			dst := flatVectors[start : start+dimensions]
			store := item.segment.vectorStore()
			if err := store.ReadVectorInto(ctx, vector.Ordinal(ordinal), dst); err != nil {
				return nil, nil, err
			}
			liveRows[liveOrdinal] = row
			liveOrdinal++
		}
	}
	if liveOrdinal != published.liveCount {
		return nil, nil, ErrInternalState
	}
	return flatVectors, liveRows, nil
}
