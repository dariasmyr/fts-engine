package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// CommittedSegment is one immutable component of a committed snapshot.
type CommittedSegment struct {
	segment       *Segment
	livenessWords []uint64
}

func (s CommittedSegment) Segment() *Segment { return s.segment }

// LivenessWords returns a canonical copy of the component-local liveness mask.
func (s CommittedSegment) LivenessWords() []uint64 {
	return append([]uint64(nil), s.livenessWords...)
}

// CommittedSnapshot is a clean, immutable persistence boundary. It contains no
// pending mutations and does not own the segment resources it references.
type CommittedSnapshot struct {
	config               Config
	revision             uint64
	maxAllocatedVectorID VectorID
	nextComponentID      ComponentID
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

func (s *CommittedSnapshot) MaxAllocatedVectorID() VectorID {
	if s == nil {
		return 0
	}
	return s.maxAllocatedVectorID
}

func (s *CommittedSnapshot) NextComponentID() ComponentID {
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
	defer s.unlockState()
	if s.mutationVersion != s.published.generation {
		return nil, ErrPendingMutations
	}

	segments := make([]CommittedSegment, len(s.published.segments))
	for i, item := range s.published.segments {
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
		config:               s.config,
		revision:             s.mutationVersion,
		maxAllocatedVectorID: s.maxAllocatedID,
		nextComponentID:      s.nextComponentID,
		segments:             segments,
	}, nil
}

// HydratedSegment describes one persisted immutable component and its local
// liveness mask. LivenessWords uses the same canonical layout as vector.BitSet.
type HydratedSegment struct {
	Segment       *Segment
	LivenessWords []uint64
}

// HydrationState contains the persisted state required to resume writes.
type HydrationState struct {
	Config               Config
	Revision             uint64
	MaxAllocatedVectorID VectorID
	NextComponentID      ComponentID
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
	if state.NextComponentID <= MutableHeadID || state.MaxAllocatedVectorID < config.InitialMaxAllocatedVectorID {
		return nil, ErrInternalState
	}

	visible := make([]visibleSegment, len(state.Segments))
	locations := make(map[VectorID]vectorLocation)
	documents := make(map[fts.DocID][]VectorID)
	var maxVectorID VectorID
	var maxComponentID ComponentID = MutableHeadID
	for segmentIndex, persisted := range state.Segments {
		if segmentIndex%16 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if persisted.Segment == nil {
			return nil, ErrInvalidSegment
		}
		filter, err := vector.NewBitSetFromWords(uint32(persisted.Segment.Len()), persisted.LivenessWords)
		if err != nil {
			return nil, ErrInvalidSegment
		}
		visible[segmentIndex] = visibleSegment{segment: persisted.Segment, filter: filter}
		componentID := persisted.Segment.ComponentID()
		maxComponentID = max(maxComponentID, componentID)
		for ordinal, row := range persisted.Segment.rows {
			if ordinal%64 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			maxVectorID = max(maxVectorID, row.VectorID)
			locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}
			if filter.Allows(vector.Ordinal(ordinal)) {
				documents[row.Chunk.DocID] = append(documents[row.Chunk.DocID], row.VectorID)
			}
		}
	}
	if state.MaxAllocatedVectorID < maxVectorID || state.NextComponentID <= maxComponentID {
		return nil, ErrInternalState
	}

	descriptor := PipelineDescriptor{Embedding: config.Embedding, Chunking: config.Chunking}
	policy := SearchPolicy{MaxK: config.MaxK, MaxChunkCandidates: config.MaxChunkCandidates, MaxChunksPerDocumentHit: config.MaxChunksPerDocumentHit}
	published, err := newReadView(ctx, state.Revision, visible, descriptor, policy, config.HNSWSearch)
	if err != nil {
		return nil, err
	}
	if published.liveCount > config.MaxVectors {
		return nil, ErrCapacityExceeded
	}
	for _, ids := range documents {
		if len(ids) > config.MaxChunksPerDocument {
			return nil, ErrInvalidSegment
		}
	}
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		return nil, ErrInvalidConfig
	}
	return &Service{
		stateGate:       make(chan struct{}, 1),
		flushGate:       make(chan struct{}, 1),
		config:          config,
		calculator:      calculator,
		published:       published,
		currentByDoc:    documents,
		locations:       locations,
		maxAllocatedID:  state.MaxAllocatedVectorID,
		nextComponentID: state.NextComponentID,
		mutationVersion: state.Revision,
		pendingVectors:  make([]pendingVector, 0, config.InitialVectorCapacity),
	}, nil
}
