package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// SegmentState contains one immutable physical segment and the liveness
// mask belonging to the committed view. Vectors and Index are immutable
// resources and must not be mutated by state adapters.
type SegmentState struct {
	ID            SegmentID
	Rows          []VectorRow
	LivenessWords []uint64
	Vectors       vector.PreparedVectorStore
	Index         *hnsw.Index
}

func (s *segment) exportState(words []uint64) SegmentState {
	if s == nil {
		return SegmentState{}
	}
	return SegmentState{
		ID:            s.id,
		Rows:          append([]VectorRow(nil), s.rows...),
		LivenessWords: append([]uint64(nil), words...),
		Vectors:       s.vectors,
		Index:         s.index,
	}
}

// CommittedIndexState contains everything required to reconstruct one
// committed Index revision and continue writing after a process restart.
// Pending mutations are intentionally excluded.
type CommittedIndexState struct {
	ID                   IndexID
	Config               SemanticConfig
	Revision             Revision
	MaxAllocatedVectorID VectorID
	NextSegmentID        SegmentID
	Segments             []SegmentState
}

// ExportState returns a persistence-neutral copy of the committed index.
// Pending mutations must be committed first so the returned value has one clear
// revision and one matching IndexView.
func (i *Index) ExportState(ctx context.Context) (*CommittedIndexState, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i.stateMu.RLock()
	// Validation boundary: a standalone export never changes search visibility.
	// It therefore rejects state that still requires Commit.
	if !i.state.pending.empty() || i.state.revision != i.state.view.revision {
		i.stateMu.RUnlock()
		return nil, ErrPendingMutations
	}

	view := i.state.view
	config := i.config
	revision := i.state.revision
	maxAllocatedVectorID := i.state.maxAllocatedVectorID
	nextSegmentID := i.state.nextSegmentID
	i.stateMu.RUnlock()

	return exportCommittedState(
		ctx,
		i.id,
		config,
		revision,
		maxAllocatedVectorID,
		nextSegmentID,
		view,
	)
}

func exportCommittedState(
	ctx context.Context,
	id IndexID,
	config SemanticConfig,
	revision Revision,
	maxAllocatedVectorID VectorID,
	nextSegmentID SegmentID,
	view *IndexView,
) (*CommittedIndexState, error) {
	segments := make([]SegmentState, len(view.segments))
	for n, item := range view.segments {
		if n%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		segments[n] = item.segment.exportState(item.liveness.SnapshotWords())
	}

	return &CommittedIndexState{
		ID:                   id,
		Config:               config,
		Revision:             revision,
		MaxAllocatedVectorID: maxAllocatedVectorID,
		NextSegmentID:        nextSegmentID,
		Segments:             segments,
	}, nil
}

// Restore validates a state and reconstructs a writable Index with an
// empty pending batch.
func Restore(ctx context.Context, state CommittedIndexState) (*Index, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	config, err := state.Config.normalized()
	if err != nil {
		return nil, err
	}
	if config != state.Config {
		return nil, ErrInvalidConfig
	}
	if state.ID == (IndexID{}) || state.NextSegmentID == 0 {
		return nil, ErrInternalState
	}
	if len(state.Segments) > config.Index.runtimeLimits().lifecycle.maxSegments {
		return nil, ErrCapacityExceeded
	}

	space, err := vector.NewSpace(config.Model.Dimensions, config.Index.Metric)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	views := make([]segmentView, len(state.Segments))
	locations := make(map[VectorID]vectorLocation)
	documents := make(map[fts.DocID]documentVectors)
	type chunkKey struct {
		documentID fts.DocID
		chunkID    ChunkID
	}
	liveChunks := make(map[chunkKey]struct{})
	var maxVectorID VectorID
	var maxSegmentID SegmentID

	for segmentIndex, persisted := range state.Segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		if persisted.Index == nil || persisted.Vectors == nil {
			return nil, ErrInvalidSegment
		}
		physical, err := newSegment(
			ctx,
			persisted.ID,
			space,
			persisted.Vectors,
			persisted.Index,
			persisted.Rows,
		)
		if err != nil {
			return nil, err
		}
		liveness, err := vector.NewBitSetFromWords(uint32(physical.len()), persisted.LivenessWords)
		if err != nil {
			return nil, ErrInvalidSegment
		}
		views[segmentIndex] = segmentView{segment: physical, liveness: liveness}

		segmentID := physical.segmentID()
		maxSegmentID = max(maxSegmentID, segmentID)
		for ordinal, row := range physical.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return nil, err
			}
			maxVectorID = max(maxVectorID, row.VectorID)
			if !liveness.Allows(vector.Ordinal(ordinal)) {
				continue
			}

			key := chunkKey{documentID: row.Chunk.DocID, chunkID: row.Chunk.ID}
			if _, exists := liveChunks[key]; exists {
				return nil, ErrInvalidSegment
			}
			liveChunks[key] = struct{}{}
			locations[row.VectorID] = vectorLocation{segment: segmentID, ordinal: vector.Ordinal(ordinal)}

			version := documents[row.Chunk.DocID]
			if version.count == 0 {
				version.first = row.VectorID
			} else if row.VectorID != version.vectorID(version.count) {
				return nil, ErrInvalidSegment
			}
			version.count++
			documents[row.Chunk.DocID] = version
		}
	}

	if state.MaxAllocatedVectorID < maxVectorID || state.NextSegmentID <= maxSegmentID {
		return nil, ErrInternalState
	}

	searchConfig := config.Index.hnswSearchConfig()

	limits := config.Index.runtimeLimits()
	if err := validateLoadedSegments(ctx, views); err != nil {
		return nil, err
	}
	view, err := newIndexView(
		ctx,
		state.Revision,
		views,
		space,
		limits.search,
		limits.results,
		searchConfig,
	)
	if err != nil {
		return nil, err
	}
	if view.liveCount > limits.capacity.maxLiveVectors {
		return nil, ErrCapacityExceeded
	}
	staleCount := 0
	for _, view := range views {
		staleCount += view.segment.len() - view.liveness.AllowedOrdinalCount()
	}
	if staleCount > limits.capacity.maxStaleVectors {
		return nil, ErrCapacityExceeded
	}
	for _, version := range documents {
		if version.count > limits.ingestion.maxChunksPerDocument {
			return nil, ErrInvalidSegment
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return &Index{
		mutationGate: make(chan struct{}, 1),
		id:           state.ID,
		config:       config,
		limits:       limits,
		space:        space,
		state: indexState{
			view:                 view,
			revision:             state.Revision,
			documents:            documents,
			locations:            locations,
			liveVectorCount:      view.liveCount,
			maxAllocatedVectorID: state.MaxAllocatedVectorID,
			nextSegmentID:        state.NextSegmentID,
		},
	}, nil
}
