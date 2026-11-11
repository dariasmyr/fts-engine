package semantic

import (
	"context"
	"math"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Commit publishes the complete pending batch in memory as one new immutable
// HNSW segment plus liveness changes to older segments. It performs no
// persistence I/O. Expensive construction happens while holding the mutation
// gate but outside the short-lived state lock, so searches continue against the
// previous immutable view. Use semanticpersist.CommitAndSave when the same
// transition must also be durable.
func (i *Index) Commit(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := i.lockMutations(ctx); err != nil {
		return err
	}
	defer i.unlockMutations()
	candidate, err := i.prepareCommitCandidate(ctx)
	if err != nil {
		return err
	}
	if err := i.validateCandidate(candidate.target); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// State transition: the prepared view becomes searchable only after all
	// fallible construction and validation have completed.
	i.publishToMemory(candidate.target)
	return nil
}

// CommitAndExportState publishes the pending batch in memory and returns a
// persistence-neutral detached copy of that exact committed revision. It
// performs no persistence I/O. Commit and export share one mutation gate, so no
// staging operation, Commit, or Compact can interleave between them.
func (i *Index) CommitAndExportState(ctx context.Context) (*CommittedIndexState, error) {
	var committed *CommittedIndexState
	err := i.CommitWithState(ctx, func(state *CommittedIndexState) error {
		committed = state
		return nil
	})
	return committed, err
}

// CommitWithState prepares the complete pending batch and passes the candidate
// committed state to accept before making it searchable in memory. The Index
// performs no persistence I/O itself; accept may establish an external commit
// point. The mutation gate is held through accept. If accept returns an error,
// the previous view remains searchable and the pending batch remains available
// for retry.
//
// Once accept returns nil, the in-memory publish is a non-failing state
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

	candidate, err := i.prepareCommitCandidate(ctx)
	if err != nil {
		return err
	}
	// Validation boundary: reject any mismatch between the prepared candidate
	// and the mutable state before invoking a callback with external side effects.
	if err := i.validateCandidate(candidate.target); err != nil {
		return err
	}
	committed, err := i.exportCommitCandidate(ctx, candidate)
	if err != nil {
		return err
	}

	// Validation boundary: accept must complete the external commit point before
	// this method changes the searchable view.
	if err := accept(committed); err != nil {
		return err
	}

	// State transition: after accept succeeds, the in-memory publish cannot be
	// canceled or fail without diverging from the externally committed state.
	i.publishToMemory(candidate.target)
	return nil
}

type commitState struct {
	candidatePreconditions
	pending   pendingBatch
	locations map[VectorID]vectorLocation
	config    IndexConfig
	limits    indexLimits
}

type commitCandidate struct {
	source commitState
	target stateCandidate
}

func (i *Index) prepareCommitCandidate(ctx context.Context) (commitCandidate, error) {
	if err := ctx.Err(); err != nil {
		return commitCandidate{}, err
	}
	state, err := i.captureCommitState(ctx)
	if err != nil {
		return commitCandidate{}, err
	}
	if state.revision == state.base.revision {
		return commitCandidate{
			source: state,
			target: stateCandidate{
				preconditions: state.candidatePreconditions,
				view:          state.base,
				locations:     state.locations,
				resetPending:  true,
			},
		}, nil
	}
	if len(state.pending.additions) > 0 && state.segmentID == SegmentID(math.MaxUint64) {
		return commitCandidate{}, ErrSegmentIDExhausted
	}
	pendingSegment, err := buildPendingSegment(ctx, state.segmentID, state.pending.additions, state.config)
	if err != nil {
		return commitCandidate{}, err
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
		return commitCandidate{}, err
	}
	allocatedSegment := pendingSegment != nil
	if exceedsLifecycleBounds(view, state.limits.capacity, state.limits.lifecycle) {
		if view.liveCount > 0 && state.segmentID == SegmentID(math.MaxUint64) {
			return commitCandidate{}, ErrSegmentIDExhausted
		}
		view, locations, allocatedSegment, err = compactView(
			ctx, view, state.segmentID, state.config,
		)
		if err != nil {
			return commitCandidate{}, err
		}
	}
	return commitCandidate{
		source: state,
		target: stateCandidate{
			preconditions:    state.candidatePreconditions,
			view:             view,
			locations:        locations,
			allocatedSegment: allocatedSegment,
			resetPending:     true,
		},
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
		candidatePreconditions: i.candidatePreconditionsLocked(),
		pending: pendingBatch{
			additions: append([]pendingVector(nil), i.state.pending.additions...),
			removals:  append([]VectorID(nil), i.state.pending.removals...),
		},
		locations: locations,
		config:    i.config,
		limits:    i.limits,
	}, nil
}

func (i *Index) exportCommitCandidate(
	ctx context.Context,
	candidate commitCandidate,
) (*CommittedIndexState, error) {
	nextSegmentID := candidate.source.segmentID
	if candidate.target.allocatedSegment {
		nextSegmentID++
	}
	return exportCommittedState(
		ctx,
		i.id,
		candidate.source.config,
		candidate.source.revision,
		candidate.source.maxAllocatedVectorID,
		nextSegmentID,
		candidate.target.view,
	)
}

func buildPendingSegment(ctx context.Context, segmentID SegmentID, pending []pendingVector, config IndexConfig) (*segment, error) {
	if len(pending) == 0 {
		return nil, nil
	}
	dimensions := config.Dimensions
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
	return buildMaterializedSegment(ctx, segmentID, flatVectors, rows, config)
}
