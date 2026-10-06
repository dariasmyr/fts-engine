package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/internal/vector/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// SegmentData is the persistence transfer value for one immutable segment.
// Callers must treat Vectors and Index as immutable.
type SegmentData struct {
	ComponentID uint64
	Pipeline    PipelineDescriptor
	Rows        []VectorRow
	Vectors     vector.PreparedVectorStore
	Index       *hnsw.Index
}

func (s *segment) data() SegmentData {
	if s == nil {
		return SegmentData{}
	}
	return SegmentData{
		ComponentID: s.component,
		Pipeline:    s.descriptor,
		Rows:        append([]VectorRow(nil), s.rows...),
		Vectors:     s.vectors,
		Index:       s.index,
	}
}

// CommittedSegment is one immutable component of committed state.
type CommittedSegment struct {
	segment       *segment
	livenessWords []uint64
}

// Data returns a defensive persistence transfer value.
func (s CommittedSegment) Data() SegmentData { return s.segment.data() }

// LivenessWords returns a canonical copy of the component-local liveness mask.
func (s CommittedSegment) LivenessWords() []uint64 {
	return append([]uint64(nil), s.livenessWords...)
}

// CommittedState is a clean, immutable persistence boundary. It contains no
// pending mutations and does not own the segment resources it references.
type CommittedState struct {
	config               Config
	revision             uint64
	maxAllocatedVectorID uint64
	nextComponentID      uint64
	segments             []CommittedSegment
}

func (s *CommittedState) Config() Config {
	if s == nil {
		return Config{}
	}
	return s.config
}

func (s *CommittedState) Revision() uint64 {
	if s == nil {
		return 0
	}
	return s.revision
}

func (s *CommittedState) MaxAllocatedVectorID() uint64 {
	if s == nil {
		return 0
	}
	return s.maxAllocatedVectorID
}

func (s *CommittedState) NextComponentID() uint64 {
	if s == nil {
		return 0
	}
	return s.nextComponentID
}

func (s *CommittedState) Segments() []CommittedSegment {
	if s == nil {
		return nil
	}
	return append([]CommittedSegment(nil), s.segments...)
}

// CommittedState captures the current committed state without publishing
// pending mutations.
func (s *Service) CommittedState(ctx context.Context) (*CommittedState, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := s.lockState(ctx); err != nil {
		return nil, err
	}
	if s.revision != s.published.revision {
		s.unlockState()
		return nil, ErrPendingMutations
	}
	published := s.published
	config := s.config
	revision := s.revision
	maxAllocatedVectorID := s.maxAllocatedID
	nextComponentID := s.nextComponentID
	s.unlockState()

	segments := make([]CommittedSegment, len(published.segments))
	for i, item := range published.segments {
		if i%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		segments[i] = CommittedSegment{
			segment:       item.segment,
			livenessWords: item.filter.SnapshotWords(),
		}
	}
	return &CommittedState{
		config:               config,
		revision:             revision,
		maxAllocatedVectorID: maxAllocatedVectorID,
		nextComponentID:      nextComponentID,
		segments:             segments,
	}, nil
}

// StoredSegment describes one persisted immutable component and its local
// liveness mask. LivenessWords uses the same canonical layout as vector.BitSet.
type StoredSegment struct {
	Data          SegmentData
	LivenessWords []uint64
}

// RestoreState contains the persisted state required to resume writes.
type RestoreState struct {
	Config               Config
	Revision             uint64
	MaxAllocatedVectorID uint64
	NextComponentID      uint64
	Segments             []StoredSegment
}

// Restore validates persisted committed state and restores a writable service
// with empty pending mutation queues.
func Restore(ctx context.Context, state RestoreState) (*Service, error) {
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
	if state.NextComponentID == 0 {
		return nil, ErrInternalState
	}

	visible := make([]visibleSegment, len(state.Segments))
	locations := make(map[uint64]vectorLocation)
	documents := make(map[fts.DocID]documentVersion)
	type chunkKey struct {
		documentID fts.DocID
		chunkID    chunk.ID
	}
	seenChunks := make(map[chunkKey]struct{})
	var maxVectorID uint64
	var maxComponentID uint64
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
		segment, err := newSegment(ctx, data.ComponentID, data.Pipeline, data.Vectors, data.Index, data.Rows)
		if err != nil {
			return nil, err
		}
		filter, err := vector.NewBitSetFromWords(uint32(segment.len()), persisted.LivenessWords)
		if err != nil {
			return nil, ErrInvalidSegment
		}
		visible[segmentIndex] = visibleSegment{segment: segment, filter: filter}
		componentID := segment.componentID()
		maxComponentID = max(maxComponentID, componentID)
		for ordinal, row := range segment.rows {
			if err := contextcheck.PeriodicError(ctx, ordinal); err != nil {
				return nil, err
			}

			maxVectorID = max(maxVectorID, row.VectorID)
			key := chunkKey{documentID: row.Chunk.DocID, chunkID: row.Chunk.ID}
			if _, exists := seenChunks[key]; exists {
				return nil, ErrInvalidSegment
			}
			seenChunks[key] = struct{}{}
			if filter.Allows(vector.Ordinal(ordinal)) {
				locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}
				version := documents[row.Chunk.DocID]
				if version.vectorCount == 0 {
					version.firstVectorID = row.VectorID
				} else if row.VectorID != version.vectorID(version.vectorCount) {
					return nil, ErrInvalidSegment
				}
				version.vectorCount++
				documents[row.Chunk.DocID] = version
			}
		}
	}
	if state.MaxAllocatedVectorID < maxVectorID || state.NextComponentID <= maxComponentID {
		return nil, ErrInternalState
	}

	buildOptions, ok := config.hnswOptions()
	if !ok {
		return nil, ErrInvalidConfig
	}

	published, err := newReadView(ctx, state.Revision, visible, config.pipelineDescriptor(), config.searchPolicy(), buildOptions.Search)
	if err != nil {
		return nil, err
	}
	if published.liveCount > config.Limits.MaxLiveVectors {
		return nil, ErrCapacityExceeded
	}
	for _, version := range documents {
		if version.vectorCount > config.Limits.MaxChunksPerDocument {
			return nil, ErrInvalidSegment
		}
	}
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		return nil, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Service{
		stateGate:       make(chan struct{}, 1),
		flushGate:       make(chan struct{}, 1),
		config:          config,
		calculator:      calculator,
		published:       published,
		currentByDoc:    documents,
		locations:       locations,
		liveVectorCount: published.liveCount,
		maxAllocatedID:  state.MaxAllocatedVectorID,
		nextComponentID: state.NextComponentID,
		revision:        state.Revision,
	}, nil
}
