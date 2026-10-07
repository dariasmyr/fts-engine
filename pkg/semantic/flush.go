package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Flush publishes the complete pending batch as one new immutable segment plus
// liveness changes to older segments. Expensive HNSW construction happens
// outside the state lock; commit succeeds only if the captured revision is current.
func (i *Index) Flush(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := i.lockPublication(ctx); err != nil {
		return err
	}
	defer i.unlockPublication()
	return i.flushPending(ctx)
}

type flushState struct {
	revision    Revision
	componentID SegmentID
	pending     pendingBatch
	locations   map[VectorID]vectorLocation
	base        *Snapshot
	config      Config
}

func (i *Index) flushPending(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := i.captureFlushState(ctx)
	if err != nil {
		return err
	}
	if state.revision == state.base.revision {
		return nil
	}
	if len(state.pending.additions) > 0 && state.componentID == SegmentID(math.MaxUint64) {
		return ErrComponentIDExhausted
	}
	segment, err := buildPendingSegment(ctx, state.componentID, state.pending.additions, state.config)
	if err != nil {
		return err
	}
	snapshot, locations, err := publishSnapshot(
		ctx,
		state.base,
		state.locations,
		state.pending.removals,
		segment,
		state.componentID,
		state.revision,
	)
	if err != nil {
		return err
	}
	return i.commitFlush(ctx, state, snapshot, locations, segment != nil)
}

func (i *Index) captureFlushState(ctx context.Context) (flushState, error) {
	if err := i.lockState(ctx); err != nil {
		return flushState{}, err
	}
	defer i.unlockState()

	locations, err := cloneLocations(ctx, i.state.locations)
	if err != nil {
		return flushState{}, err
	}
	return flushState{
		revision:    i.state.revision,
		componentID: i.state.nextComponentID,
		pending: pendingBatch{
			additions: append([]pendingVector(nil), i.state.pending.additions...),
			removals:  append([]VectorID(nil), i.state.pending.removals...),
		},
		locations: locations,
		base:      i.snapshot,
		config:    i.config,
	}, nil
}

func (i *Index) commitFlush(ctx context.Context, state flushState, snapshot *Snapshot, locations map[VectorID]vectorLocation, allocatedComponent bool) error {
	if err := i.lockState(ctx); err != nil {
		return err
	}
	defer i.unlockState()
	if err := ctx.Err(); err != nil {
		return err
	}
	if state.revision != i.state.revision {
		return ErrPublicationConflict
	}
	i.snapshot = snapshot
	i.state.locations = locations
	i.state.pending.reset()
	if allocatedComponent {
		i.state.nextComponentID++
	}
	return nil
}

func buildPendingSegment(ctx context.Context, componentID SegmentID, pending []pendingVector, config Config) (*segment, error) {
	if len(pending) == 0 {
		return nil, nil
	}
	dimensions := config.Schema.Embedding.Dimensions
	flatVectors := make([]float32, len(pending)*dimensions)
	rows := make([]VectorRow, len(pending))
	for n, item := range pending {
		if err := contextcheck.PeriodicError(ctx, n); err != nil {
			return nil, err
		}
		copy(flatVectors[n*dimensions:(n+1)*dimensions], item.vector)
		rows[n] = item.row
	}
	calculator, err := config.Schema.Embedding.Calculator()
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
	return buildSegment(ctx, componentID, config.Schema, source, rows, buildOptions)
}
