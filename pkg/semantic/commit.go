package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Commit publishes the complete pending batch as one new immutable HNSW segment
// plus liveness changes to older segments. Expensive construction happens while
// holding the mutation gate but outside the short-lived state lock, so searches
// continue against the previous immutable view.
func (i *Index) Commit(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := i.lockMutations(ctx); err != nil {
		return err
	}
	defer i.unlockMutations()
	prepared, err := i.preparePendingCommit(ctx)
	if err != nil {
		return err
	}
	if err := i.validatePreparedCommit(prepared); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// State transition: the prepared view becomes searchable only after all
	// fallible construction and validation have completed.
	i.publishPreparedCommit(prepared)
	return nil
}

// CommitAndExportState publishes the pending batch and returns a detached copy
// of that exact committed revision. Commit and export share one mutation gate,
// so no staging operation, Commit, or Compact can interleave between them.
func (i *Index) CommitAndExportState(ctx context.Context) (*CommittedIndexState, error) {
	var committed *CommittedIndexState
	err := i.CommitWithState(ctx, func(state *CommittedIndexState) error {
		committed = state
		return nil
	})
	return committed, err
}

// CommitWithState prepares the complete pending batch and passes the candidate
// committed state to accept before making it searchable. The mutation gate is
// held through accept. If accept returns an error, the previous view remains
// searchable and the pending batch remains available for retry.
//
// Once accept returns nil, publication is an in-memory, non-failing state
// transition. Callers must therefore return nil only after any external commit
// point associated with the candidate state has completed. The callback may
// search the index but must not call mutation, Commit, or Compact methods on it.
func (i *Index) CommitWithState(
	ctx context.Context,
	accept func(*CommittedIndexState) error,
) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if accept == nil {
		return ErrInvalidConfig
	}
	if err := i.lockMutations(ctx); err != nil {
		return err
	}
	defer i.unlockMutations()

	prepared, err := i.preparePendingCommit(ctx)
	if err != nil {
		return err
	}
	// Validation boundary: reject any mismatch between the prepared candidate
	// and the mutable state before invoking a callback with external side effects.
	if err := i.validatePreparedCommit(prepared); err != nil {
		return err
	}
	committed, err := i.exportPreparedCommit(ctx, prepared)
	if err != nil {
		return err
	}

	// Validation boundary: accept must complete the external commit point before
	// this method changes the searchable view.
	if err := accept(committed); err != nil {
		return err
	}

	// State transition: after accept succeeds, publication cannot be canceled or
	// fail without diverging from the externally committed state.
	i.publishPreparedCommit(prepared)
	return nil
}

type commitState struct {
	revision             Revision
	segmentID            SegmentID
	maxAllocatedVectorID VectorID
	pending              pendingBatch
	locations            map[VectorID]vectorLocation
	base                 *IndexView
	config               IndexConfig
	limits               indexLimits
}

type preparedCommit struct {
	state            commitState
	view             *IndexView
	locations        map[VectorID]vectorLocation
	allocatedSegment bool
}

func (i *Index) preparePendingCommit(ctx context.Context) (preparedCommit, error) {
	if err := ctx.Err(); err != nil {
		return preparedCommit{}, err
	}
	state, err := i.captureCommitState(ctx)
	if err != nil {
		return preparedCommit{}, err
	}
	if state.revision == state.base.revision {
		return preparedCommit{
			state:     state,
			view:      state.base,
			locations: state.locations,
		}, nil
	}
	if len(state.pending.additions) > 0 && state.segmentID == SegmentID(math.MaxUint64) {
		return preparedCommit{}, ErrSegmentIDExhausted
	}
	pendingSegment, err := buildPendingSegment(ctx, state.segmentID, state.pending.additions, state.config)
	if err != nil {
		return preparedCommit{}, err
	}
	view, locations, err := buildCommittedView(
		ctx,
		state.base,
		state.locations,
		state.pending.removals,
		pendingSegment,
		state.segmentID,
		state.revision,
	)
	if err != nil {
		return preparedCommit{}, err
	}
	allocatedSegment := pendingSegment != nil
	if exceedsLifecycleBounds(view, state.limits.capacity, state.limits.lifecycle) {
		if view.liveCount > 0 && state.segmentID == SegmentID(math.MaxUint64) {
			return preparedCommit{}, ErrSegmentIDExhausted
		}
		view, locations, allocatedSegment, err = compactView(
			ctx, view, state.segmentID, state.config,
		)
		if err != nil {
			return preparedCommit{}, err
		}
	}
	return preparedCommit{
		state:            state,
		view:             view,
		locations:        locations,
		allocatedSegment: allocatedSegment,
	}, nil
}

func exceedsLifecycleBounds(
	view *IndexView,
	capacity capacityLimits,
	lifecycle lifecycleLimits,
) bool {
	stale := 0
	for _, view := range view.segments {
		stale += view.segment.len() - view.liveness.AllowedOrdinalCount()
	}
	return stale > capacity.maxStaleVectors || len(view.segments) > lifecycle.maxSegments
}

func (i *Index) captureCommitState(ctx context.Context) (commitState, error) {
	if err := ctx.Err(); err != nil {
		return commitState{}, err
	}
	i.stateMu.RLock()
	defer i.stateMu.RUnlock()

	locations, err := cloneLocations(ctx, i.state.locations)
	if err != nil {
		return commitState{}, err
	}
	return commitState{
		revision:             i.state.revision,
		segmentID:            i.state.nextSegmentID,
		maxAllocatedVectorID: i.state.maxAllocatedVectorID,
		pending: pendingBatch{
			additions: append([]pendingVector(nil), i.state.pending.additions...),
			removals:  append([]VectorID(nil), i.state.pending.removals...),
		},
		locations: locations,
		base:      i.state.view,
		config:    i.config,
		limits:    i.limits,
	}, nil
}

func (i *Index) exportPreparedCommit(
	ctx context.Context,
	prepared preparedCommit,
) (*CommittedIndexState, error) {
	nextSegmentID := prepared.state.segmentID
	if prepared.allocatedSegment {
		nextSegmentID++
	}
	return exportCommittedState(
		ctx,
		i.id,
		prepared.state.config,
		i.modelService,
		prepared.state.revision,
		prepared.state.maxAllocatedVectorID,
		nextSegmentID,
		prepared.view,
	)
}

func (i *Index) validatePreparedCommit(prepared preparedCommit) error {
	i.stateMu.RLock()
	defer i.stateMu.RUnlock()
	if prepared.state.revision != i.state.revision ||
		prepared.state.segmentID != i.state.nextSegmentID ||
		prepared.state.maxAllocatedVectorID != i.state.maxAllocatedVectorID ||
		prepared.state.base != i.state.view {
		return ErrInternalState
	}
	return nil
}

func (i *Index) publishPreparedCommit(prepared preparedCommit) {
	i.stateMu.Lock()
	defer i.stateMu.Unlock()
	i.state.view = prepared.view
	i.state.locations = prepared.locations
	i.state.pending.reset()
	if prepared.allocatedSegment {
		i.state.nextSegmentID++
	}
}

func buildPendingSegment(ctx context.Context, segmentID SegmentID, pending []pendingVector, config IndexConfig) (*segment, error) {
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
		if err := item.vector.CopyTo(flatVectors[n*dimensions : (n+1)*dimensions]); err != nil {
			return nil, err
		}
		rows[n] = item.row
	}
	space, err := config.Schema.Embedding.VectorSpace()
	if err != nil {
		return nil, err
	}
	source, err := memorystore.NewPrepared(space, flatVectors)
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
		segmentID,
		config.Schema,
		source,
		rows,
		buildConfig,
		searchConfig,
	)
}
