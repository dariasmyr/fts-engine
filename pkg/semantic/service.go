package semantic

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

type Service struct {
	mu sync.RWMutex

	config          Config
	space           vector.Space
	head            *mutableSource
	segments        []*SealedSegment
	nextComponentID ComponentID

	maxAllocatedVectorID VectorID
	currentByDoc         map[fts.DocID][]VectorID
	headOrdinalByVector  map[VectorID]vector.Ordinal
	vectorRows           []VectorRow
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
	config.HNSWBuild = normalizeBuildConfig(config.HNSWBuild, config.Space, config.MaxVectors)
	config.HNSWSearch = normalizeSearchConfig(config.HNSWSearch, config.MaxK, config.MaxChunkCandidates, config.MaxVectors)
	if _, err := hnsw.NewBuilder(config.HNSWBuild, config.HNSWSearch, 0); err != nil {
		return nil, ErrInvalidConfig
	}
	head := newMutableSource(space, config.MaxVectors, config.InitialVectorCapacity)
	return &Service{
		config:               config,
		space:                space,
		head:                 head,
		maxAllocatedVectorID: config.InitialMaxAllocatedVectorID,
		nextComponentID:      MutableHeadID + 1,
		currentByDoc:         make(map[fts.DocID][]VectorID),
		headOrdinalByVector:  make(map[VectorID]vector.Ordinal),
	}, nil
}

func normalizeBuildConfig(config hnsw.BuildConfig, space SpaceDescriptor, maxVectors int) hnsw.BuildConfig {
	if config.Dimensions == 0 {
		config.Dimensions = space.Dimensions
	}
	if config.Metric == 0 {
		config.Metric = space.Metric
	}
	if config.MaxVectors == 0 {
		config.MaxVectors = maxVectors
	}
	if config.MaxVectorBytes == 0 {
		config.MaxVectorBytes = uint64(maxVectors) * uint64(space.Dimensions) * 4
	}
	if config.MaxNeighbors == 0 {
		config.MaxNeighbors = 16
	}
	if config.EfConstruction == 0 {
		config.EfConstruction = max(64, config.MaxNeighbors)
	}
	return config
}

func normalizeSearchConfig(config hnsw.SearchConfig, maxK, maxCandidates, maxVectors int) hnsw.SearchConfig {
	if config.DefaultEfSearch == 0 {
		config.DefaultEfSearch = max(maxK, min(maxCandidates, 64))
	}
	if config.MaxEfSearch == 0 {
		config.MaxEfSearch = max(maxCandidates, config.DefaultEfSearch)
	}
	if config.DefaultVisitLimit == 0 {
		config.DefaultVisitLimit = maxVectors
	}
	if config.MaxVisitLimit == 0 {
		config.MaxVisitLimit = maxVectors
	}
	if config.MaxK == 0 {
		config.MaxK = maxCandidates
	}
	return config
}

func (s *Service) Space() SpaceDescriptor { return s.config.Space }

func (s *Service) Chunking() ChunkingDescriptor { return s.config.Chunking }

// Flush is a visibility barrier for the HNSW segment set. Mutations currently
// seal a small immutable HNSW segment synchronously, so Flush only observes
// cancellation and provides an explicit lifecycle hook for future batching.
func (s *Service) Flush(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	return ctx.Err()
}

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
	return s.appendVersionLocked(ctx, docID, prepared, nil)
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
	return s.appendVersionLocked(ctx, docID, prepared, old)
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
	staleOrdinals := make([]vector.Ordinal, 0, len(ids))
	for _, id := range ids {
		if ordinal, ok := s.headOrdinalByVector[id]; ok {
			staleOrdinals = append(staleOrdinals, ordinal)
		}
	}
	next, err := s.live.WithChanges(s.live.TotalOrdinalCount(), nil, staleOrdinals)
	if err != nil {
		panic(fmt.Errorf("%w: %v", ErrInternalState, err))
	}
	s.live = next
	delete(s.currentByDoc, docID)
	return true
}

// Compact rebuilds the mutable source and one merged HNSW segment from live vectors.
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
	nextRows := make([]VectorRow, 0, s.live.AllowedOrdinalCount())
	for ordinal, row := range s.vectorRows {
		if !s.live.Allows(vector.Ordinal(ordinal)) {
			continue
		}
		nextRows = append(nextRows, row)
	}
	if len(nextRows) != nextHead.Len() {
		return ErrInternalState
	}
	componentID := s.nextComponentID
	nextSource, err := nextHead.FreezeCompact(ctx, nil)
	if err != nil {
		return err
	}
	graph, err := hnsw.Build(ctx, nextSource, hnsw.BuildOptions{
		BuildConfig:  withBuildCapacity(s.config.HNSWBuild, nextHead.Len()),
		SearchConfig: s.config.HNSWSearch,
	})
	if err != nil {
		return err
	}
	merged, err := NewHNSWSegment(componentID, nextSource, graph, nextRows)
	if err != nil {
		return err
	}
	rowsByID := make(map[VectorID]VectorRow, len(nextRows))
	nextHeadOrdinals := make(map[VectorID]vector.Ordinal, len(nextRows))
	for ordinal, row := range nextRows {
		rowsByID[row.VectorID] = row
		nextHeadOrdinals[row.VectorID] = vector.Ordinal(ordinal)
	}
	for docID, ids := range s.currentByDoc {
		for _, id := range ids {
			row, ok := rowsByID[id]
			if !ok || row.Chunk.DocID != docID {
				return ErrInternalState
			}
		}
	}
	s.head = nextHead
	s.vectorRows = nextRows
	s.headOrdinalByVector = nextHeadOrdinals
	s.segments = []*SealedSegment{merged}
	s.nextComponentID++
	s.live = vector.NewFullBitSet(uint32(len(nextRows)))
	return nil
}

func (s *Service) appendVersionLocked(ctx context.Context, docID fts.DocID, batch []ChunkVector, old []VectorID) error {
	// Check capacity and derive new stable IDs without changing service state.
	// Example: watermark 10 and two new chunks produce IDs 11 and 12.
	if len(batch) > s.config.MaxVectors-s.head.Len() {
		return ErrCapacityExceeded
	}
	ids, err := s.allocateIDsLocked(len(batch))
	if err != nil {
		return err
	}
	componentID := s.nextComponentID
	rows := make([]VectorRow, len(batch))
	values := make([][]float32, len(batch))
	for i, item := range batch {
		rows[i] = VectorRow{VectorID: ids[i], Chunk: item.Ref}
		values[i] = item.Vector
	}
	source, err := newMemorySource(s.space, values)
	if err != nil {
		return err
	}
	graph, err := hnsw.Build(ctx, source, hnsw.BuildOptions{
		BuildConfig:  withBuildCapacity(s.config.HNSWBuild, len(batch)),
		SearchConfig: s.config.HNSWSearch,
	})
	if err != nil {
		return err
	}
	segment, err := NewHNSWSegment(componentID, source, graph, rows)
	if err != nil {
		return err
	}
	// The append-only ingest source assigns the next contiguous ordinal range.
	// Prepare the corresponding liveness snapshot before mutating the index.
	// Example replace: with head length 5, two new chunks use ordinals 5 and 6;
	// old ordinals 2 and 3 become stale but remain in the physical matrix.
	start := s.head.Len()
	newLiveOrdinals := make([]vector.Ordinal, len(batch))
	for i := range newLiveOrdinals {
		newLiveOrdinals[i] = vector.Ordinal(start + i)
	}
	staleOrdinals := make([]vector.Ordinal, 0, len(old))
	for _, id := range old {
		ordinal, ok := s.headOrdinalByVector[id]
		if !ok {
			continue
		}
		staleOrdinals = append(staleOrdinals, ordinal)
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
	if ordinals.Count != len(batch) {
		return ErrInternalState
	}
	for i := range batch {
		ord := ordinals.Start + vector.Ordinal(i)
		if ord != newLiveOrdinals[i] {
			return ErrInternalState
		}

		// Install the mappings while the service lock prevents readers from
		// observing a partially published document version.
		id := ids[i]
		s.vectorRows = append(s.vectorRows, VectorRow{VectorID: id, Chunk: batch[i].Ref})
		s.headOrdinalByVector[id] = ord
	}
	// Switch the document mapping and liveness snapshot to the new version.
	// Example: doc-A -> [9, 10] becomes doc-A -> [11, 12], while vectors 9
	// and 10 remain stored but are no longer allowed in search results.
	s.currentByDoc[docID] = append([]VectorID(nil), ids...)
	s.live = nextLive
	s.segments = append(s.segments, segment)
	s.nextComponentID++
	// Advance the watermark only after the new version is fully installed.
	s.maxAllocatedVectorID = ids[len(ids)-1]
	return nil
}

func withBuildCapacity(config hnsw.BuildConfig, count int) hnsw.BuildConfig {
	if config.MaxVectors < count {
		config.MaxVectors = count
	}
	if config.MaxVectorBytes < uint64(count)*uint64(config.Dimensions)*4 {
		config.MaxVectorBytes = uint64(count) * uint64(config.Dimensions) * 4
	}
	return config
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
		// The ingest source copies and prepares every vector before publishing the batch.
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
