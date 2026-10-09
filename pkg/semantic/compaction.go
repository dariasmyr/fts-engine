package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Compact rewrites all live rows from the committed snapshot into at most one
// immutable segment. Pending mutations must be flushed first.
func (i *Index) Compact(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := i.lockPublication(ctx); err != nil {
		return err
	}
	defer i.unlockPublication()

	if err := i.lockState(ctx); err != nil {
		return err
	}
	revision := i.state.revision
	snapshot := i.snapshot
	config := i.config
	componentID := i.state.nextComponentID
	hasPending := i.state.revision != i.snapshot.revision
	i.unlockState()
	if hasPending {
		return ErrPendingMutations
	}

	if len(snapshot.segments) <= 1 {
		stale := false
		for _, item := range snapshot.segments {
			stale = stale || item.liveness.AllowedOrdinalCount() != item.segment.len()
		}
		if !stale {
			return nil
		}
	}

	if snapshot.liveCount > 0 && componentID == SegmentID(math.MaxUint64) {
		return ErrComponentIDExhausted
	}
	compactedSnapshot, locations, allocatedComponent, err := compactSnapshot(
		ctx, snapshot, componentID, config,
	)
	if err != nil {
		return err
	}

	if err := i.lockState(ctx); err != nil {
		return err
	}
	defer i.unlockState()
	if err := ctx.Err(); err != nil {
		return err
	}
	if revision != i.state.revision {
		return ErrPublicationConflict
	}
	i.snapshot = compactedSnapshot
	i.state.locations = locations
	if allocatedComponent {
		i.state.nextComponentID++
	}
	return nil
}

func compactSnapshot(ctx context.Context, snapshot *Snapshot, componentID SegmentID, config Config) (*Snapshot, map[VectorID]vectorLocation, bool, error) {
	var compacted *segment
	if snapshot.liveCount > 0 {
		var err error
		compacted, err = buildCompactedSegment(ctx, snapshot, componentID, config)
		if err != nil {
			return nil, nil, false, err
		}
	}

	locations := make(map[VectorID]vectorLocation, snapshot.liveCount)
	if compacted != nil {
		for ordinal, row := range compacted.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return nil, nil, false, err
			}
			locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}
		}
	}

	var segments []segmentView
	if compacted != nil {
		segments = []segmentView{{
			segment:  compacted,
			liveness: vector.NewFullBitSet(uint32(compacted.len())),
		}}
	}
	searchConfig, ok := config.hnswSearchConfig()
	if !ok {
		return nil, nil, false, ErrInvalidConfig
	}
	compactedSnapshot, err := newTrustedSnapshot(
		ctx,
		snapshot.revision,
		segments,
		snapshot.schema,
		config.searchPolicy(),
		searchConfig,
		snapshot.calculator,
	)
	if err != nil {
		return nil, nil, false, err
	}
	return compactedSnapshot, locations, compacted != nil, nil
}

func buildCompactedSegment(ctx context.Context, snapshot *Snapshot, componentID SegmentID, config Config) (*segment, error) {
	flatVectors, liveRows, err := materializeLiveRows(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	calculator, err := config.Schema.Embedding.Calculator()
	if err != nil {
		return nil, err
	}
	source, err := memorystore.NewPrepared(calculator, flatVectors)
	if err != nil {
		return nil, err
	}
	buildConfig, ok := config.hnswBuildConfig()
	if !ok {
		return nil, ErrInvalidConfig
	}
	searchConfig, ok := config.hnswSearchConfig()
	if !ok {
		return nil, ErrInvalidConfig
	}
	return buildSegment(
		ctx,
		componentID,
		config.Schema,
		source,
		liveRows,
		buildConfig,
		searchConfig,
	)
}

func materializeLiveRows(ctx context.Context, snapshot *Snapshot) ([]float32, []VectorRow, error) {
	dimensions := snapshot.calculator.Dimensions()
	flatVectors := make([]float32, snapshot.liveCount*dimensions)
	liveRows := make([]VectorRow, snapshot.liveCount)
	liveOrdinal := 0
	for _, item := range snapshot.segments {
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
	if liveOrdinal != snapshot.liveCount {
		return nil, nil, ErrInternalState
	}
	return flatVectors, liveRows, nil
}
