package semantic

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	vectorflat "github.com/dariasmyr/fts-engine/pkg/vector/flat"
)

type Service struct {
	mu sync.RWMutex

	config Config
	space  vector.Space
	head   *vectorflat.Index

	maxAllocatedVectorID VectorID
	currentByDoc         map[fts.DocID][]VectorID
	refByVector          map[VectorID]chunk.Ref
	locationByVector     map[VectorID]Location
	vectorIDs            []VectorID
	live                 vector.BitSet
}

func New(config Config) (*Service, error) {
	space, err := vector.NewSpace(config.Space.Dimensions, config.Space.Metric)
	if err != nil {
		return nil, err
	}
	if config.Space.ID == "" || config.Space.VectorFormatVersion == 0 ||
		config.Space.Normalization != space.Normalization() || config.Chunking.ID == "" ||
		config.MaxVectors <= 0 || config.MaxChunksPerDocument <= 0 || config.MaxK <= 0 ||
		config.MaxChunkCandidates < config.MaxK || config.MaxChunkCandidates > config.MaxVectors ||
		config.MaxChunksPerDocument > config.MaxVectors || config.MaxChunksPerDocumentHit <= 0 ||
		config.MaxChunksPerDocumentHit > config.MaxChunksPerDocument || config.InitialVectorCapacity < 0 ||
		config.InitialVectorCapacity > config.MaxVectors {
		return nil, ErrInvalidConfig
	}
	flatMaxK := max(config.MaxK, config.MaxChunkCandidates)
	head, err := vectorflat.New(vectorflat.Config{
		Dimensions:            config.Space.Dimensions,
		Metric:                config.Space.Metric,
		MaxVectors:            config.MaxVectors,
		MaxK:                  flatMaxK,
		InitialVectorCapacity: config.InitialVectorCapacity,
	})
	if err != nil {
		return nil, err
	}
	return &Service{
		config:               config,
		space:                space,
		head:                 head,
		maxAllocatedVectorID: config.InitialMaxAllocatedVectorID,
		currentByDoc:         make(map[fts.DocID][]VectorID),
		refByVector:          make(map[VectorID]chunk.Ref),
		locationByVector:     make(map[VectorID]Location),
	}, nil
}

func (s *Service) Space() SpaceDescriptor { return s.config.Space }

func (s *Service) Chunking() ChunkingDescriptor { return s.config.Chunking }

func (s *Service) AddDocument(ctx context.Context, batch []ChunkVector) error {
	docID, prepared, err := s.validateBatch(ctx, batch)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, exists := s.currentByDoc[docID]; exists {
		return fmt.Errorf("%w: %s", ErrDocumentExists, docID)
	}
	return s.appendVersionLocked(docID, prepared, nil)
}

func (s *Service) ReplaceDocument(ctx context.Context, batch []ChunkVector) error {
	docID, prepared, err := s.validateBatch(ctx, batch)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	old, exists := s.currentByDoc[docID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrDocumentNotFound, docID)
	}
	return s.appendVersionLocked(docID, prepared, old)
}

func (s *Service) DeleteDocument(docID fts.DocID) bool {
	if docID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids, exists := s.currentByDoc[docID]
	if !exists {
		return false
	}
	staleOrdinals := make([]vector.Ordinal, len(ids))
	for i, id := range ids {
		location, ok := s.locationByVector[id]
		if !ok || location.Component != MutableHeadID {
			panic(ErrInternalState)
		}
		staleOrdinals[i] = location.Ordinal
	}
	next, err := s.live.WithChanges(s.live.TotalOrdinalCount(), nil, staleOrdinals)
	if err != nil {
		panic(fmt.Errorf("%w: %v", ErrInternalState, err))
	}
	s.live = next
	delete(s.currentByDoc, docID)
	return true
}

// Compact mutates the service by rebuilding its flat head from live vectors.
// Stable VectorIDs and MaxAllocatedVectorID are preserved while local ordinals
// are reassigned.
func (s *Service) Compact(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.live.AllowedOrdinalCount() == s.head.Len() {
		return ctx.Err()
	}

	nextHead, err := s.head.Compact(ctx, s.live)
	if err != nil {
		return err
	}
	nextVectorIDs := make([]VectorID, 0, s.live.AllowedOrdinalCount())
	nextRefs := make(map[VectorID]chunk.Ref, s.live.AllowedOrdinalCount())
	nextLocations := make(map[VectorID]Location, s.live.AllowedOrdinalCount())
	for ordinal, id := range s.vectorIDs {
		if !s.live.Allows(vector.Ordinal(ordinal)) {
			continue
		}
		ref, ok := s.refByVector[id]
		if !ok {
			return ErrInternalState
		}
		nextOrdinal := vector.Ordinal(len(nextVectorIDs))
		nextVectorIDs = append(nextVectorIDs, id)
		nextRefs[id] = ref
		nextLocations[id] = Location{Component: MutableHeadID, Ordinal: nextOrdinal}
	}
	if len(nextVectorIDs) != nextHead.Len() {
		return ErrInternalState
	}
	for docID, ids := range s.currentByDoc {
		for _, id := range ids {
			ref, ok := nextRefs[id]
			if !ok || ref.DocID != docID {
				return ErrInternalState
			}
		}
	}
	s.head = nextHead
	s.vectorIDs = nextVectorIDs
	s.refByVector = nextRefs
	s.locationByVector = nextLocations
	s.live = vector.NewFullBitSet(uint32(len(nextVectorIDs)))
	return nil
}

func (s *Service) appendVersionLocked(docID fts.DocID, batch []ChunkVector, old []VectorID) error {
	// Check capacity and derive new stable IDs without changing service state.
	// Example: watermark 10 and two new chunks produce IDs 11 and 12.
	if len(batch) > s.config.MaxVectors-s.head.Len() {
		return vectorflat.ErrCapacityExceeded
	}
	ids, err := s.allocateIDsLocked(len(batch))
	if err != nil {
		return err
	}
	// The append-only flat index assigns the next contiguous ordinal range.
	// Prepare the corresponding liveness snapshot before mutating the index.
	// Example replace: with head length 5, two new chunks use ordinals 5 and 6;
	// old ordinals 2 and 3 become stale but remain in the physical matrix.
	start := s.head.Len()
	newLiveOrdinals := make([]vector.Ordinal, len(batch))
	for i := range newLiveOrdinals {
		newLiveOrdinals[i] = vector.Ordinal(start + i)
	}
	staleOrdinals := make([]vector.Ordinal, len(old))
	for i, id := range old {
		location, ok := s.locationByVector[id]
		if !ok || location.Component != MutableHeadID {
			return ErrInternalState
		}
		staleOrdinals[i] = location.Ordinal
	}
	nextLive, err := s.live.WithChanges(uint32(start+len(batch)), newLiveOrdinals, staleOrdinals)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInternalState, err)
	}

	// AppendBatch prepares and copies every vector, or leaves the index unchanged.
	vectors := make([][]float32, len(batch))
	for i := range batch {
		vectors[i] = batch[i].Vector
	}
	ordinals, err := s.head.AppendBatch(vectors)
	if err != nil {
		return err
	}

	// The assigned range must match the ordinals used to build nextLive.
	if ordinals.Count != uint32(len(batch)) {
		return ErrInternalState
	}
	for i := range batch {
		ord, ok := ordinals.At(i)
		if !ok {
			return ErrInternalState
		}
		if ord != newLiveOrdinals[i] {
			return ErrInternalState
		}

		// Install the mappings while the service lock prevents readers from
		// observing a partially published document version.
		id := ids[i]
		s.vectorIDs = append(s.vectorIDs, id)
		s.refByVector[id] = batch[i].Ref
		s.locationByVector[id] = Location{Component: MutableHeadID, Ordinal: ord}
	}
	// Switch the document mapping and liveness snapshot to the new version.
	// Example: doc-A -> [9, 10] becomes doc-A -> [11, 12], while vectors 9
	// and 10 remain stored but are no longer allowed in search results.
	s.currentByDoc[docID] = append([]VectorID(nil), ids...)
	s.live = nextLive
	// Advance the watermark only after the new version is fully installed.
	s.maxAllocatedVectorID = ids[len(ids)-1]
	return nil
}

func (s *Service) validateBatch(ctx context.Context, batch []ChunkVector) (fts.DocID, []ChunkVector, error) {
	if ctx == nil {
		return "", nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if len(batch) == 0 || len(batch) > s.config.MaxChunksPerDocument {
		return "", nil, ErrInvalidBatch
	}
	docID := batch[0].Ref.DocID
	if docID == "" {
		return "", nil, chunk.ErrInvalidDocID
	}
	seen := make(map[chunk.ID]struct{}, len(batch))
	validated := make([]ChunkVector, len(batch))
	for i, item := range batch {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return "", nil, err
			}
		}
		if item.Ref.DocID != docID || item.Ref.ID == "" || item.Ref.Field == "" || item.Ref.StartByte > item.Ref.EndByte {
			return "", nil, ErrInvalidBatch
		}
		if _, exists := seen[item.Ref.ID]; exists {
			return "", nil, fmt.Errorf("%w: %s", ErrDuplicateChunkID, item.Ref.ID)
		}
		seen[item.Ref.ID] = struct{}{}
		// Validate the complete document batch without copying vector data. The
		// flat head copies and prepares every vector before publishing the batch.
		if err := s.space.Validate(item.Vector); err != nil {
			return "", nil, err
		}
		validated[i] = ChunkVector{Ref: item.Ref, Vector: item.Vector}
	}
	return docID, validated, nil
}

func (s *Service) allocateIDsLocked(count int) ([]VectorID, error) {
	if count <= 0 || uint64(count) > math.MaxUint64-uint64(s.maxAllocatedVectorID) {
		return nil, ErrVectorIDExhausted
	}
	ids := make([]VectorID, count)
	for i := range ids {
		ids[i] = s.maxAllocatedVectorID + VectorID(i) + 1
		if ids[i] == 0 {
			return nil, ErrVectorIDExhausted
		}
	}
	return ids, nil
}

func (s *Service) Statistics() Statistics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	physical := s.head.Len()
	liveCount := s.live.AllowedOrdinalCount()
	return Statistics{
		Documents:            len(s.currentByDoc),
		PhysicalVectors:      physical,
		LiveVectors:          liveCount,
		StaleVectors:         physical - liveCount,
		MaxAllocatedVectorID: s.maxAllocatedVectorID,
	}
}
