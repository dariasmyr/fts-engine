package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// CheckpointIndex is the read-only index metadata exposed to checkpoint
// adapters. The concrete navigation index remains an implementation detail of
// the semantic index and its persistence codec.
//
// Restore accepts only index values originating from this package or from a
// compatible semantic persistence adapter; callers should not implement this
// interface themselves.
type CheckpointIndex interface {
	Len() int
	Dimensions() int
	Metric() vector.Metric
	Normalization() vector.Normalization
}

// CheckpointSegment contains one immutable physical segment and the liveness
// mask belonging to the committed snapshot. Vectors and Index are immutable
// resources and must not be mutated by checkpoint adapters.
type CheckpointSegment struct {
	ID            SegmentID
	Schema        Schema
	Rows          []VectorRow
	LivenessWords []uint64
	Vectors       vector.PreparedVectorStore
	Index         CheckpointIndex
}

func (s *segment) checkpoint(words []uint64) CheckpointSegment {
	if s == nil {
		return CheckpointSegment{}
	}
	return CheckpointSegment{
		ID:            s.component,
		Schema:        s.schema,
		Rows:          append([]VectorRow(nil), s.rows...),
		LivenessWords: append([]uint64(nil), words...),
		Vectors:       s.vectors,
		Index:         s.index,
	}
}

// IndexCheckpoint contains everything required to reconstruct a committed
// Index and continue writing after a process restart. Pending mutations are
// intentionally excluded.
type IndexCheckpoint struct {
	Config               Config
	Revision             Revision
	MaxAllocatedVectorID VectorID
	NextSegmentID        SegmentID
	Segments             []CheckpointSegment
}

// CreateCheckpoint returns a persistence-neutral view of the committed index.
// Pending mutations must be flushed first so the returned value has one clear
// revision and one matching Snapshot.
func (i *Index) CreateCheckpoint(ctx context.Context) (*IndexCheckpoint, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := i.lockState(ctx); err != nil {
		return nil, err
	}
	if !i.state.pending.empty() || i.state.revision != i.snapshot.revision {
		i.unlockState()
		return nil, ErrPendingMutations
	}

	snapshot := i.snapshot
	config := i.config
	revision := i.state.revision
	maxAllocatedVectorID := i.state.maxAllocatedVectorID
	nextComponentID := i.state.nextComponentID
	i.unlockState()

	segments := make([]CheckpointSegment, len(snapshot.segments))
	for n, item := range snapshot.segments {
		if n%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		segments[n] = item.segment.checkpoint(item.liveness.SnapshotWords())
	}

	return &IndexCheckpoint{
		Config:               config,
		Revision:             revision,
		MaxAllocatedVectorID: maxAllocatedVectorID,
		NextSegmentID:        nextComponentID,
		Segments:             segments,
	}, nil
}

// Restore validates a checkpoint and reconstructs a writable Index with an
// empty pending batch.
func Restore(ctx context.Context, checkpoint IndexCheckpoint) (*Index, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	config, err := checkpoint.Config.normalized()
	if err != nil {
		return nil, err
	}
	if config != checkpoint.Config {
		return nil, ErrInvalidConfig
	}
	if checkpoint.NextSegmentID == 0 {
		return nil, ErrInternalState
	}
	if len(checkpoint.Segments) > config.Limits.MaxSegments {
		return nil, ErrCapacityExceeded
	}

	views := make([]segmentView, len(checkpoint.Segments))
	locations := make(map[VectorID]vectorLocation)
	documents := make(map[fts.DocID]documentVectors)
	type chunkKey struct {
		documentID fts.DocID
		chunkID    ChunkID
	}
	liveChunks := make(map[chunkKey]struct{})
	var maxVectorID VectorID
	var maxComponentID SegmentID

	for segmentIndex, persisted := range checkpoint.Segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		index, ok := persisted.Index.(*hnsw.Index)
		if !ok || index == nil || persisted.Vectors == nil {
			return nil, ErrInvalidSegment
		}
		if err := validateSchemaCompatibility(persisted.Schema, config.Schema); err != nil {
			return nil, err
		}

		physical, err := newSegment(ctx, persisted.ID, persisted.Schema, persisted.Vectors, index, persisted.Rows)
		if err != nil {
			return nil, err
		}
		liveness, err := vector.NewBitSetFromWords(uint32(physical.len()), persisted.LivenessWords)
		if err != nil {
			return nil, ErrInvalidSegment
		}
		views[segmentIndex] = segmentView{segment: physical, liveness: liveness}

		componentID := physical.componentID()
		maxComponentID = max(maxComponentID, componentID)
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
			locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}

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

	if checkpoint.MaxAllocatedVectorID < maxVectorID || checkpoint.NextSegmentID <= maxComponentID {
		return nil, ErrInternalState
	}

	searchConfig, ok := config.hnswSearchConfig()
	if !ok {
		return nil, ErrInvalidConfig
	}

	snapshot, err := newSnapshot(ctx, checkpoint.Revision, views, config.Schema, config.searchPolicy(), searchConfig)
	if err != nil {
		return nil, err
	}
	if snapshot.liveCount > config.Limits.MaxLiveVectors {
		return nil, ErrCapacityExceeded
	}
	staleCount := 0
	for _, view := range views {
		staleCount += view.segment.len() - view.liveness.AllowedOrdinalCount()
	}
	if staleCount > config.Limits.MaxStaleVectors {
		return nil, ErrCapacityExceeded
	}
	for _, version := range documents {
		if version.count > config.Limits.MaxChunksPerDocument {
			return nil, ErrInvalidSegment
		}
	}

	calculator, err := config.Schema.Embedding.Calculator()
	if err != nil {
		return nil, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return &Index{
		stateGate:   make(chan struct{}, 1),
		publishGate: make(chan struct{}, 1),
		config:      config,
		calculator:  calculator,
		snapshot:    snapshot,
		state: workingState{
			revision:             checkpoint.Revision,
			documents:            documents,
			locations:            locations,
			liveVectorCount:      snapshot.liveCount,
			maxAllocatedVectorID: checkpoint.MaxAllocatedVectorID,
			nextComponentID:      checkpoint.NextSegmentID,
		},
	}, nil
}
