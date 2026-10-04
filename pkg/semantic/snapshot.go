package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/internal/vector/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

// SegmentSnapshot is the persistence transfer representation of one immutable
// semantic segment. Rows returns a defensive copy; vectors and index are
// immutable readers shared with the runtime segment.
type SegmentSnapshot struct {
	componentID uint64
	descriptor  PipelineDescriptor
	rows        []VectorRow
	vectors     vectorstore.PreparedVectorStore
	index       *hnsw.Index
}

// SegmentData is the persistence transfer value for one immutable segment.
// Callers must treat Vectors and Index as immutable.
type SegmentData struct {
	ComponentID uint64
	Pipeline    PipelineDescriptor
	Rows        []VectorRow
	Vectors     vectorstore.PreparedVectorStore
	Index       *hnsw.Index
}

// NewSegmentSnapshot creates persistence transfer data for hydration. Hydrate
// validates the data and constructs the private runtime segment.
func NewSegmentSnapshot(data SegmentData) SegmentSnapshot {
	return SegmentSnapshot{
		componentID: data.ComponentID,
		descriptor:  data.Pipeline,
		rows:        append([]VectorRow(nil), data.Rows...),
		vectors:     data.Vectors,
		index:       data.Index,
	}
}

// Data returns a defensive persistence transfer value.
func (s SegmentSnapshot) Data() SegmentData {
	return SegmentData{
		ComponentID: s.componentID,
		Pipeline:    s.descriptor,
		Rows:        append([]VectorRow(nil), s.rows...),
		Vectors:     s.vectors,
		Index:       s.index,
	}
}

func (s *segment) snapshot() SegmentSnapshot {
	if s == nil {
		return SegmentSnapshot{}
	}
	return NewSegmentSnapshot(SegmentData{
		ComponentID: s.component,
		Pipeline:    s.descriptor,
		Rows:        s.rows,
		Vectors:     s.vectors,
		Index:       s.index,
	})
}

// CommittedSegment is one immutable component of a committed snapshot.
type CommittedSegment struct {
	segment       *segment
	livenessWords []uint64
}

func (s CommittedSegment) Snapshot() SegmentSnapshot { return s.segment.snapshot() }

// LivenessWords returns a canonical copy of the component-local liveness mask.
func (s CommittedSegment) LivenessWords() []uint64 {
	return append([]uint64(nil), s.livenessWords...)
}

// CommittedSnapshot is a clean, immutable persistence boundary. It contains no
// pending mutations and does not own the segment resources it references.
type CommittedSnapshot struct {
	config               Config
	revision             uint64
	maxAllocatedVectorID uint64
	nextComponentID      uint64
	segments             []CommittedSegment
}

func (s *CommittedSnapshot) Config() Config {
	if s == nil {
		return Config{}
	}
	return s.config
}

func (s *CommittedSnapshot) Revision() uint64 {
	if s == nil {
		return 0
	}
	return s.revision
}

func (s *CommittedSnapshot) MaxAllocatedVectorID() uint64 {
	if s == nil {
		return 0
	}
	return s.maxAllocatedVectorID
}

func (s *CommittedSnapshot) NextComponentID() uint64 {
	if s == nil {
		return 0
	}
	return s.nextComponentID
}

func (s *CommittedSnapshot) Segments() []CommittedSegment {
	if s == nil {
		return nil
	}
	result := make([]CommittedSegment, len(s.segments))
	for i, segment := range s.segments {
		result[i] = CommittedSegment{
			segment:       segment.segment,
			livenessWords: append([]uint64(nil), segment.livenessWords...),
		}
	}
	return result
}

// CommittedSnapshot captures the current committed state without publishing
// pending mutations.
func (s *Service) CommittedSnapshot(ctx context.Context) (*CommittedSnapshot, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := s.lockState(ctx); err != nil {
		return nil, err
	}
	if s.mutationVersion != s.published.generation {
		s.unlockState()
		return nil, ErrPendingMutations
	}
	published := s.published
	config := s.config
	revision := s.mutationVersion
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
	return &CommittedSnapshot{
		config:               config,
		revision:             revision,
		maxAllocatedVectorID: maxAllocatedVectorID,
		nextComponentID:      nextComponentID,
		segments:             segments,
	}, nil
}

// HydratedSegment describes one persisted immutable component and its local
// liveness mask. LivenessWords uses the same canonical layout as vector.BitSet.
type HydratedSegment struct {
	Snapshot      SegmentSnapshot
	LivenessWords []uint64
}

// HydrationState contains the persisted state required to resume writes.
type HydrationState struct {
	Config               Config
	Revision             uint64
	MaxAllocatedVectorID uint64
	NextComponentID      uint64
	Segments             []HydratedSegment
}

// Hydrate validates persisted committed state and restores a writable service
// with empty pending mutation queues.
func Hydrate(ctx context.Context, state HydrationState) (*Service, error) {
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
	var maxVectorID uint64
	var maxComponentID uint64
	for segmentIndex, persisted := range state.Segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		snapshot := persisted.Snapshot
		if snapshot.index == nil || snapshot.vectors == nil {
			return nil, ErrInvalidSegment
		}
		segment, err := newSegment(ctx, snapshot.componentID, snapshot.descriptor, snapshot.vectors, snapshot.index, snapshot.rows)
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
			locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}
			if filter.Allows(vector.Ordinal(ordinal)) {
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
		mutationVersion: state.Revision,
	}, nil
}
