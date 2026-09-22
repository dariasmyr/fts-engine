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

type pendingVector struct {
	row    VectorRow
	vector []float32
}

type vectorLocation struct {
	component ComponentID
	ordinal   vector.Ordinal
}

type livenessChange struct {
	ids     []VectorID
	allowed bool
}

type Service struct {
	mu      sync.RWMutex
	flushMu sync.Mutex

	config    Config
	space     vector.Space
	published *ReadView

	pendingVectors           []pendingVector
	pendingVisibilityChanges []livenessChange
	currentByDoc             map[fts.DocID][]VectorID
	locations                map[VectorID]vectorLocation
	maxAllocatedID           VectorID
	nextComponentID          ComponentID
	mutationVersion          uint64
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
	return &Service{
		config:          config,
		space:           space,
		published:       &ReadView{},
		maxAllocatedID:  config.InitialMaxAllocatedVectorID,
		nextComponentID: MutableHeadID + 1,
		currentByDoc:    make(map[fts.DocID][]VectorID),
		locations:       make(map[VectorID]vectorLocation),
		pendingVectors:  make([]pendingVector, 0, config.InitialVectorCapacity),
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

// AddDocument encodes a document through encoder and queues its vectors.
func (s *Service) AddDocument(ctx context.Context, encoder Encoder, document Document) error {
	if encoder == nil {
		return ErrInvalidConfig
	}
	batch, err := encoder.Encode(ctx, document)
	if err != nil {
		return err
	}
	return s.addEncodedDocument(ctx, document.ID, batch)
}

func (s *Service) addEncodedDocument(ctx context.Context, docID fts.DocID, batch []ChunkVector) error {
	if err := s.validateBatch(ctx, docID, batch); err != nil {
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
	return s.queueVersionLocked(docID, batch, nil)
}

// ReplaceDocument encodes a document version through encoder and queues it.
func (s *Service) ReplaceDocument(ctx context.Context, encoder Encoder, document Document) error {
	if encoder == nil {
		return ErrInvalidConfig
	}
	batch, err := encoder.Encode(ctx, document)
	if err != nil {
		return err
	}
	return s.replaceEncodedDocument(ctx, document.ID, batch)
}

func (s *Service) replaceEncodedDocument(ctx context.Context, docID fts.DocID, batch []ChunkVector) error {
	if err := s.validateBatch(ctx, docID, batch); err != nil {
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
	return s.queueVersionLocked(docID, batch, old)
}

// DeleteDocument queues a deletion. The document remains searchable until
// Flush publishes the updated component-local liveness filters.
func (s *Service) DeleteDocument(docID fts.DocID) bool {
	if docID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.currentByDoc[docID]; !exists {
		return false
	}
	ids := s.currentByDoc[docID]
	delete(s.currentByDoc, docID)
	s.pendingVisibilityChanges = append(s.pendingVisibilityChanges, livenessChange{ids: append([]VectorID(nil), ids...), allowed: false})
	s.mutationVersion++
	return true
}

// Flush builds one HNSW segment for the pending batch outside Service.mu and
// atomically publishes a new immutable read view. If mutations race with the
// build, the stale build is discarded and retried from the newer state.
func (s *Service) Flush(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	return s.flushPending(ctx)
}

// ReadView flushes pending mutations and returns the immutable published view.
// The returned view is independent of subsequent service mutations.
func (s *Service) ReadView(ctx context.Context) (*ReadView, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := s.Flush(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	view := s.published
	s.mu.RUnlock()
	return view, nil
}

// flushPending publishes the current pending state. The caller must hold
// flushMu so that flush and compact cannot build competing publications.
func (s *Service) flushPending(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		version, componentID, pendingVectors, visibilityChanges, documents, base, config := s.captureFlushState()
		if version == base.generation {
			return nil
		}
		segment, err := buildPendingSegment(ctx, componentID, pendingVectors, config)
		if err != nil {
			return err
		}

		s.mu.Lock()
		if version != s.mutationVersion {
			s.mu.Unlock()
			continue
		}
		published, locations, err := publishIndex(base, s.locations, visibilityChanges, segment, componentID, documents, version)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		s.published = published
		s.locations = locations
		s.pendingVectors = nil
		s.pendingVisibilityChanges = nil
		if segment != nil {
			s.nextComponentID++
		}
		s.mu.Unlock()
		return nil
	}
}

func (s *Service) captureFlushState() (uint64, ComponentID, []pendingVector, []livenessChange, map[fts.DocID][]VectorID, *ReadView, Config) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pendingVectors := make([]pendingVector, len(s.pendingVectors))
	for i, item := range s.pendingVectors {
		pendingVectors[i] = pendingVector{row: item.row, vector: append([]float32(nil), item.vector...)}
	}
	documents := cloneDocumentMapping(s.currentByDoc)
	pendingVisibilityChanges := make([]livenessChange, len(s.pendingVisibilityChanges))
	for i, change := range s.pendingVisibilityChanges {
		pendingVisibilityChanges[i] = livenessChange{ids: append([]VectorID(nil), change.ids...), allowed: change.allowed}
	}
	return s.mutationVersion, s.nextComponentID, pendingVectors, pendingVisibilityChanges, documents, s.published, s.config
}

func buildPendingSegment(ctx context.Context, componentID ComponentID, pending []pendingVector, config Config) (*Segment, error) {
	if len(pending) == 0 {
		return nil, nil
	}
	values := make([][]float32, len(pending))
	rows := make([]VectorRow, len(pending))
	for i, item := range pending {
		values[i] = item.vector
		rows[i] = item.row
	}
	source, err := newInMemoryVectorSourceFromConfig(config, values)
	if err != nil {
		return nil, err
	}
	return BuildSegment(ctx, componentID, SegmentMetadata{Space: config.Space, Chunking: config.Chunking}, source, rows, hnsw.BuildOptions{
		BuildConfig:  withBuildCapacity(config.HNSWBuild, len(rows)),
		SearchConfig: config.HNSWSearch,
	})
}

func newInMemoryVectorSourceFromConfig(config Config, values [][]float32) (*vector.MemorySource, error) {
	space, err := vector.NewSpace(config.Space.Dimensions, config.Space.Metric)
	if err != nil {
		return nil, err
	}
	return vector.NewMemorySource(space, values)
}

func publishIndex(base *ReadView, locations map[VectorID]vectorLocation, changes []livenessChange, pending *Segment, pendingComponent ComponentID, documents map[fts.DocID][]VectorID, generation uint64) (*ReadView, map[VectorID]vectorLocation, error) {
	segments := append([]segmentView(nil), base.segments...)
	componentIndexes := make(map[ComponentID]int, len(segments))
	for i, view := range segments {
		if view.segment == nil {
			return nil, nil, ErrInternalState
		}
		componentIndexes[view.segment.ComponentID()] = i
	}
	type componentChanges struct {
		allowed    []vector.Ordinal
		disallowed []vector.Ordinal
	}
	changesByComponent := make(map[ComponentID]*componentChanges)
	for _, change := range changes {
		for _, id := range change.ids {
			location, ok := locations[id]
			if !ok {
				continue
			}
			if _, ok := componentIndexes[location.component]; !ok {
				return nil, nil, ErrInternalState
			}
			componentChange := changesByComponent[location.component]
			if componentChange == nil {
				componentChange = &componentChanges{}
				changesByComponent[location.component] = componentChange
			}
			if change.allowed {
				componentChange.allowed = append(componentChange.allowed, location.ordinal)
			} else {
				componentChange.disallowed = append(componentChange.disallowed, location.ordinal)
			}
		}
	}
	for componentID, change := range changesByComponent {
		index := componentIndexes[componentID]
		filter, err := segments[index].filter.WithChanges(segments[index].filter.TotalOrdinalCount(), change.allowed, change.disallowed)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: update segment filter: %v", ErrInternalState, err)
		}
		segments[index].filter = filter
	}
	resultLocations := cloneLocations(locations)
	if pending != nil {
		filter, err := filterForDocumentMapping(pending, documents)
		if err != nil {
			return nil, nil, err
		}
		segments = append(segments, segmentView{segment: pending, filter: filter})
		for ordinal, row := range pending.Rows() {
			resultLocations[row.VectorID] = vectorLocation{component: pendingComponent, ordinal: vector.Ordinal(ordinal)}
		}
	}
	return newReadView(generation, segments), resultLocations, nil
}

func filterForDocumentMapping(segment *Segment, documents map[fts.DocID][]VectorID) (vector.BitSet, error) {
	liveIDs := make(map[VectorID]struct{})
	for _, ids := range documents {
		for _, id := range ids {
			liveIDs[id] = struct{}{}
		}
	}
	allowed := make([]vector.Ordinal, 0, segment.Len())
	for ordinal, row := range segment.Rows() {
		if _, ok := liveIDs[row.VectorID]; ok {
			allowed = append(allowed, vector.Ordinal(ordinal))
		}
	}
	filter, err := vector.NewBitSet(uint32(segment.Len()), allowed...)
	if err != nil {
		return vector.BitSet{}, fmt.Errorf("%w: build pending segment filter: %v", ErrInternalState, err)
	}
	return filter, nil
}

func cloneDocumentMapping(source map[fts.DocID][]VectorID) map[fts.DocID][]VectorID {
	result := make(map[fts.DocID][]VectorID, len(source))
	for docID, ids := range source {
		result[docID] = append([]VectorID(nil), ids...)
	}
	return result
}

func cloneLocations(source map[VectorID]vectorLocation) map[VectorID]vectorLocation {
	result := make(map[VectorID]vectorLocation, len(source))
	for id, location := range source {
		result[id] = location
	}
	return result
}

func (s *Service) queueVersionLocked(docID fts.DocID, batch []ChunkVector, old []VectorID) error {
	liveCount := 0
	for _, ids := range s.currentByDoc {
		liveCount += len(ids)
	}
	if liveCount-len(old)+len(batch) > s.config.MaxVectors {
		return ErrCapacityExceeded
	}
	ids, err := s.allocateIDsLocked(len(batch))
	if err != nil {
		return err
	}
	for i, item := range batch {
		id := ids[i]
		s.pendingVectors = append(s.pendingVectors, pendingVector{
			row:    VectorRow{VectorID: id, Chunk: item.Ref},
			vector: append([]float32(nil), item.Vector...),
		})
	}
	s.currentByDoc[docID] = append([]VectorID(nil), ids...)
	if len(old) > 0 {
		s.pendingVisibilityChanges = append(s.pendingVisibilityChanges, livenessChange{ids: append([]VectorID(nil), old...), allowed: false})
	}
	s.pendingVisibilityChanges = append(s.pendingVisibilityChanges, livenessChange{ids: append([]VectorID(nil), ids...), allowed: true})
	s.maxAllocatedID = ids[len(ids)-1]
	s.mutationVersion++
	return nil
}

func (s *Service) viewSegmentsPhysical() []segmentView {
	if s.published == nil {
		return nil
	}
	return s.published.segments
}

func (s *Service) physicalVectorCountLocked() int {
	count := len(s.pendingVectors)
	for _, view := range s.viewSegmentsPhysical() {
		count += view.segment.Len()
	}
	return count
}

// Compact merges all visible live rows into one immutable HNSW segment.
func (s *Service) Compact(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if err := s.flushPending(ctx); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.RLock()
		version := s.mutationVersion
		published := s.published
		config := s.config
		componentID := s.nextComponentID
		hasPending := len(s.pendingVectors) != 0 || len(s.pendingVisibilityChanges) != 0
		s.mu.RUnlock()
		if hasPending {
			if err := s.flushPending(ctx); err != nil {
				return err
			}
			continue
		}
		if len(published.segments) <= 1 {
			stale := false
			for _, item := range published.segments {
				stale = stale || item.filter.AllowedOrdinalCount() != item.segment.Len()
			}
			if !stale {
				return nil
			}
		}
		merged, rows, err := buildCompactedSegment(ctx, published, componentID, config)
		if err != nil {
			return err
		}
		filter := vector.NewFullBitSet(uint32(len(rows)))
		s.mu.Lock()
		if version != s.mutationVersion {
			s.mu.Unlock()
			if err := s.flushPending(ctx); err != nil {
				return err
			}
			continue
		}
		locations := make(map[VectorID]vectorLocation, len(rows))
		for ordinal, row := range rows {
			locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}
		}
		s.published = newReadView(version, []segmentView{{segment: merged, filter: filter}})
		s.locations = locations
		s.nextComponentID++
		s.mu.Unlock()
		return nil
	}
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

func (s *Service) validateBatch(ctx context.Context, docID fts.DocID, batch []ChunkVector) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(batch) == 0 || len(batch) > s.config.MaxChunksPerDocument {
		return ErrInvalidBatch
	}
	if docID == "" {
		return chunk.ErrInvalidDocID
	}
	for i, item := range batch {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if item.Ref.DocID != docID || item.Ref.ID == "" || item.Ref.Field == "" || item.Ref.StartByte > item.Ref.EndByte {
			return ErrInvalidBatch
		}
		if err := s.space.Validate(item.Vector); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) allocateIDsLocked(count int) ([]VectorID, error) {
	if count <= 0 || uint64(count) > math.MaxUint64-uint64(s.maxAllocatedID) {
		return nil, ErrVectorIDExhausted
	}
	ids := make([]VectorID, count)
	for i := range ids {
		ids[i] = s.maxAllocatedID + VectorID(i) + 1
		if ids[i] == 0 {
			return nil, ErrVectorIDExhausted
		}
	}
	return ids, nil
}

func (s *Service) Statistics() Statistics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	physical := s.physicalVectorCountLocked()
	live := 0
	for _, ids := range s.currentByDoc {
		live += len(ids)
	}
	return Statistics{
		Documents:            len(s.currentByDoc),
		PhysicalVectors:      physical,
		LiveVectors:          live,
		StaleVectors:         physical - live,
		MaxAllocatedVectorID: s.maxAllocatedID,
	}
}
