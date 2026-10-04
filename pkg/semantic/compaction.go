package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/internal/vector/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
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
	version := s.mutationVersion
	published := s.published
	config := s.config
	componentID := s.nextComponentID
	hasPending := s.mutationVersion != s.published.generation
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
	var merged *segment
	var rows []VectorRow
	if published.liveCount > 0 {
		if componentID == uint64(math.MaxUint64) {
			return ErrComponentIDExhausted
		}
		var err error
		merged, rows, err = buildCompactedSegment(ctx, published, componentID, config)
		if err != nil {
			return err
		}
	}
	locations := make(map[uint64]vectorLocation, len(rows))
	for ordinal, row := range rows {
		if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
			return err
		}

		locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}
	}
	segments := []visibleSegment(nil)
	if merged != nil {
		segments = []visibleSegment{{segment: merged, filter: vector.NewFullBitSet(uint32(len(rows)))}}
	}
	buildOptions, ok := config.hnswOptions()
	if !ok {
		return ErrInvalidConfig
	}
	view, err := newReadView(ctx, version, segments, published.descriptor, config.searchPolicy(), buildOptions.Search)
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
	if version != s.mutationVersion {
		return ErrPublicationConflict
	}
	s.published = view
	s.locations = locations
	if merged != nil {
		s.nextComponentID++
	}
	return nil
}

// buildCompactedSegment materializes live rows from an immutable read view and
// builds the replacement HNSW segment without touching mutable service state.
func buildCompactedSegment(ctx context.Context, view *ReadView, componentID uint64, config Config) (*segment, []VectorRow, error) {
	flatVectors, liveRows, err := materializeLiveRows(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		return nil, nil, err
	}
	source, err := vectorstore.NewPreparedMemoryVectorStore(calculator, flatVectors)
	if err != nil {
		return nil, nil, err
	}
	buildOptions, ok := config.hnswOptions()
	if !ok {
		return nil, nil, ErrInvalidConfig
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
		return nil, nil, err
	}
	return segment, liveRows, nil
}

func materializeLiveRows(ctx context.Context, published *ReadView) ([]float32, []VectorRow, error) {
	// flatVectors is a flat slice of all live vectors in the published segments, concatenated in order.
	// liveRows is a slice of all live vector ids in the published segments, concatenated in order.
	// The two slices are aligned by ordinal: flatVectors[dim*i:dim*(i+1)] is the vector for liveRows[i].
	var flatVectors []float32
	var liveRows []VectorRow
	for _, item := range published.segments {
		dim := item.segment.dimensions()

		for ordinal, row := range item.segment.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return nil, nil, err
			}

			if !item.filter.Allows(vector.Ordinal(ordinal)) {
				continue
			}

			start := len(flatVectors)
			flatVectors = append(flatVectors, make([]float32, dim)...)
			dst := flatVectors[start:]
			if err := item.segment.vectorStore().ReadVectorInto(ctx, vector.Ordinal(ordinal), dst); err != nil {
				return nil, nil, err
			}
			liveRows = append(liveRows, row)
		}
	}
	return flatVectors, liveRows, nil
}
