package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// SegmentData is the persistence transfer value for one immutable physical
// segment. Vectors and Index are immutable resources and must not be mutated by
// the persistence layer.
type SegmentData struct {
	ID      SegmentID
	Schema  Schema
	Rows    []VectorRow
	Vectors vector.PreparedVectorStore
	Index   *hnsw.Index
}

func (s *segment) data() SegmentData {
	if s == nil {
		return SegmentData{}
	}
	return SegmentData{
		ID:      s.component,
		Schema:  s.schema,
		Rows:    append([]VectorRow(nil), s.rows...),
		Vectors: s.vectors,
		Index:   s.index,
	}
}

// StateSegment is one persisted immutable segment plus the liveness mask that
// belongs to the committed Snapshot from which State was created.
type StateSegment struct {
	Data          SegmentData
	LivenessWords []uint64
}

// State is the persistence boundary of semantic.Index. It contains everything
// required to reconstruct the committed index and continue writing after a
// process restart. Pending mutations are intentionally excluded.
type State struct {
	Config               Config
	Revision             Revision
	MaxAllocatedVectorID VectorID
	NextSegmentID        SegmentID
	Segments             []StateSegment
}

// State returns a durable representation of the current committed index.
// Pending mutations must be flushed first so the returned value has one clear
// revision and one matching Snapshot.
func (i *Index) State(ctx context.Context) (*State, error) {
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

	segments := make([]StateSegment, len(snapshot.segments))
	for n, item := range snapshot.segments {
		if n%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		segments[n] = StateSegment{
			Data:          item.segment.data(),
			LivenessWords: item.liveness.SnapshotWords(),
		}
	}

	return &State{
		Config:               config,
		Revision:             revision,
		MaxAllocatedVectorID: maxAllocatedVectorID,
		NextSegmentID:        nextComponentID,
		Segments:             segments,
	}, nil
}

func (s *Service) State(ctx context.Context) (*State, error) {
	return s.index.State(ctx)
}

// Open validates persisted committed state and reconstructs a writable Index
// with an empty pending batch.
func Open(ctx context.Context, state State) (*Index, error) {
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
	if state.NextSegmentID == 0 {
		return nil, ErrInternalState
	}

	views := make([]segmentView, len(state.Segments))
	locations := make(map[VectorID]vectorLocation)
	documents := make(map[fts.DocID]documentVectors)
	type chunkKey struct {
		documentID fts.DocID
		chunkID    chunk.ID
	}
	liveChunks := make(map[chunkKey]struct{})
	var maxVectorID VectorID
	var maxComponentID SegmentID

	for segmentIndex, persisted := range state.Segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		data := persisted.Data
		if data.Index == nil || data.Vectors == nil {
			return nil, ErrInvalidSegment
		}
		if err := validateSchemaCompatibility(data.Schema, config.Schema); err != nil {
			return nil, err
		}

		physical, err := newSegment(ctx, data.ID, data.Schema, data.Vectors, data.Index, data.Rows)
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

	if state.MaxAllocatedVectorID < maxVectorID || state.NextSegmentID <= maxComponentID {
		return nil, ErrInternalState
	}

	searchConfig, ok := config.hnswSearchConfig()
	if !ok {
		return nil, ErrInvalidConfig
	}

	snapshot, err := newSnapshot(ctx, state.Revision, views, config.Schema, config.searchPolicy(), searchConfig)
	if err != nil {
		return nil, err
	}
	if snapshot.liveCount > config.Limits.MaxLiveVectors {
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
			revision:             state.Revision,
			documents:            documents,
			locations:            locations,
			liveVectorCount:      snapshot.liveCount,
			maxAllocatedVectorID: state.MaxAllocatedVectorID,
			nextComponentID:      state.NextSegmentID,
		},
	}, nil
}
