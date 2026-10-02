package semantic

import (
	"context"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
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
	stateGate chan struct{}
	flushGate chan struct{}

	config     Config
	calculator vector.Calculator
	published  *ReadView

	pendingVectors           []pendingVector
	pendingVisibilityChanges []livenessChange
	currentByDoc             map[fts.DocID][]VectorID
	locations                map[VectorID]vectorLocation
	maxAllocatedID           VectorID
	nextComponentID          ComponentID
	mutationVersion          uint64
}

func New(config Config) (*Service, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		return nil, ErrInvalidConfig
	}
	descriptor := PipelineDescriptor{Embedding: config.Embedding, Chunking: config.Chunking}
	policy := SearchPolicy{MaxK: config.MaxK, MaxChunkCandidates: config.MaxChunkCandidates, MaxChunksPerDocumentHit: config.MaxChunksPerDocumentHit}
	published, err := newEmptyReadView(context.Background(), descriptor, policy, config.HNSWSearch)
	if err != nil {
		return nil, err
	}
	return &Service{
		config:          config,
		calculator:      calculator,
		published:       published,
		stateGate:       make(chan struct{}, 1),
		flushGate:       make(chan struct{}, 1),
		maxAllocatedID:  config.InitialMaxAllocatedVectorID,
		nextComponentID: MutableHeadID + 1,
		currentByDoc:    make(map[fts.DocID][]VectorID),
		locations:       make(map[VectorID]vectorLocation),
		pendingVectors:  make([]pendingVector, 0, config.InitialVectorCapacity),
	}, nil
}

func (s *Service) Embedding() EmbeddingDescriptor { return s.config.Embedding }

func (s *Service) Chunking() ChunkingDescriptor { return s.config.Chunking }

func (s *Service) validateEncoder(encoder Encoder) error {
	if encoder == nil {
		return ErrInvalidConfig
	}
	_, err := descriptorsEqual(encoder.Descriptor(), PipelineDescriptor{Embedding: s.config.Embedding, Chunking: s.config.Chunking})
	return err
}

// AddDocument encodes a document through encoder and queues its vectors.
func (s *Service) AddDocument(ctx context.Context, encoder Encoder, document Document) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateEncoder(encoder); err != nil {
		return err
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
	if err := s.lockState(ctx); err != nil {
		return err
	}
	defer s.unlockState()
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
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateEncoder(encoder); err != nil {
		return err
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
	if err := s.lockState(ctx); err != nil {
		return err
	}
	defer s.unlockState()
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
func (s *Service) DeleteDocument(ctx context.Context, docID fts.DocID) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if docID == "" {
		return chunk.ErrInvalidDocID
	}
	if err := s.lockState(ctx); err != nil {
		return err
	}
	defer s.unlockState()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, exists := s.currentByDoc[docID]; !exists {
		return fmt.Errorf("%w: %s", ErrDocumentNotFound, docID)
	}
	if s.mutationVersion == math.MaxUint64 {
		return ErrRevisionExhausted
	}
	ids := s.coalescePendingLocked(s.currentByDoc[docID])
	delete(s.currentByDoc, docID)
	if len(ids) > 0 {
		s.pendingVisibilityChanges = append(s.pendingVisibilityChanges, livenessChange{ids: ids, allowed: false})
	}
	s.mutationVersion++
	return nil
}

// Flush builds one HNSW segment outside the state lock and atomically publishes
// it. A concurrent mutation rejects the stale build with ErrPublicationConflict;
// the caller may retry without losing pending state.
func (s *Service) Flush(ctx context.Context) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := s.lockFlush(ctx); err != nil {
		return err
	}
	defer s.unlockFlush()
	return s.flushPending(ctx)
}

// ReadView returns the current committed immutable view without publishing
// pending mutations. Call Flush explicitly to publish queued changes.
func (s *Service) ReadView() *ReadView {
	s.lockStateUninterruptible()
	view := s.published
	s.unlockState()
	return view
}

func (s *Service) readView(ctx context.Context) (*ReadView, error) {
	if err := s.lockState(ctx); err != nil {
		return nil, err
	}
	view := s.published
	s.unlockState()
	return view, nil
}

func (s *Service) lockState(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.stateGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			s.unlockState()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) lockStateUninterruptible() { s.stateGate <- struct{}{} }

func (s *Service) unlockState() { <-s.stateGate }

func (s *Service) lockFlush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.flushGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			s.unlockFlush()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) unlockFlush() { <-s.flushGate }

// flushPending publishes the current pending state. The caller must hold
// flushGate so flush and compact cannot build competing publications.
func (s *Service) flushPending(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := s.captureFlushState(ctx)
	if err != nil {
		return err
	}
	if state.version == state.base.generation {
		return nil
	}
	if len(state.pendingVectors) > 0 && state.componentID == ComponentID(math.MaxUint64) {
		return ErrComponentIDExhausted
	}
	segment, err := buildPendingSegment(ctx, state.componentID, state.pendingVectors, state.config)
	if err != nil {
		return err
	}
	published, locations, err := publishIndex(ctx, state.base, state.locations, state.visibilityChanges, segment, state.componentID, state.documents, state.version)
	if err != nil {
		return err
	}
	return s.commitFlush(ctx, state, published, locations, segment != nil)
}

func (s *Service) commitFlush(ctx context.Context, state flushState, published *ReadView, locations map[VectorID]vectorLocation, allocatedComponent bool) error {
	if err := s.lockState(ctx); err != nil {
		return err
	}
	defer s.unlockState()
	if state.version != s.mutationVersion {
		return ErrPublicationConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.published = published
	s.locations = locations
	s.pendingVectors = nil
	s.pendingVisibilityChanges = nil
	if allocatedComponent {
		s.nextComponentID++
	}
	return nil
}

type flushState struct {
	version           uint64
	componentID       ComponentID
	pendingVectors    []pendingVector
	visibilityChanges []livenessChange
	documents         map[fts.DocID][]VectorID
	locations         map[VectorID]vectorLocation
	base              *ReadView
	config            Config
}

func (s *Service) captureFlushState(ctx context.Context) (flushState, error) {
	if err := s.lockState(ctx); err != nil {
		return flushState{}, err
	}
	defer s.unlockState()
	pendingVectors := make([]pendingVector, len(s.pendingVectors))
	for i, item := range s.pendingVectors {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return flushState{}, err
			}
		}
		pendingVectors[i] = pendingVector{row: item.row, vector: append([]float32(nil), item.vector...)}
	}
	documents, err := cloneDocumentMapping(ctx, s.currentByDoc)
	if err != nil {
		return flushState{}, err
	}
	pendingVisibilityChanges := make([]livenessChange, len(s.pendingVisibilityChanges))
	for i, change := range s.pendingVisibilityChanges {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return flushState{}, err
			}
		}
		pendingVisibilityChanges[i] = livenessChange{ids: append([]VectorID(nil), change.ids...), allowed: change.allowed}
	}
	locations, err := cloneLocations(ctx, s.locations)
	if err != nil {
		return flushState{}, err
	}
	return flushState{
		version: s.mutationVersion, componentID: s.nextComponentID,
		pendingVectors: pendingVectors, visibilityChanges: pendingVisibilityChanges,
		documents: documents, locations: locations, base: s.published, config: s.config,
	}, nil
}

func buildPendingSegment(ctx context.Context, componentID ComponentID, pending []pendingVector, config Config) (*Segment, error) {
	if len(pending) == 0 {
		return nil, nil
	}
	values := make([][]float32, len(pending))
	rows := make([]VectorRow, len(pending))
	for i, item := range pending {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		values[i] = item.vector
		rows[i] = item.row
	}
	source, err := newInMemoryVectorStoreFromConfig(config, values)
	if err != nil {
		return nil, err
	}
	return BuildSegment(ctx, componentID, SegmentMetadata{Embedding: config.Embedding, Chunking: config.Chunking}, source, rows, hnsw.BuildOptions{
		Build:  config.HNSWBuild,
		Search: config.HNSWSearch,
	})
}

func newInMemoryVectorStoreFromConfig(config Config, values [][]float32) (*vectorstore.MemoryVectorStore, error) {
	calculator, err := config.Embedding.Calculator()
	if err != nil {
		return nil, err
	}
	return vectorstore.NewMemoryVectorStore(calculator, values)
}

func publishIndex(ctx context.Context, base *ReadView, locations map[VectorID]vectorLocation, changes []livenessChange, pending *Segment, pendingComponent ComponentID, documents map[fts.DocID][]VectorID, generation uint64) (*ReadView, map[VectorID]vectorLocation, error) {
	segments := append([]visibleSegment(nil), base.segments...)
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
	for changeIndex, change := range changes {
		if changeIndex%64 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		for idIndex, id := range change.ids {
			if idIndex%256 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, nil, err
				}
			}
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
	resultLocations, err := cloneLocations(ctx, locations)
	if err != nil {
		return nil, nil, err
	}
	if pending != nil {
		filter, err := filterForDocumentMapping(ctx, pending, documents)
		if err != nil {
			return nil, nil, err
		}
		segments = append(segments, visibleSegment{segment: pending, filter: filter})
		for ordinal, row := range pending.Rows() {
			resultLocations[row.VectorID] = vectorLocation{component: pendingComponent, ordinal: vector.Ordinal(ordinal)}
		}
	}
	view, err := newReadView(ctx, generation, segments, base.descriptor, SearchPolicy{MaxK: base.maxK, MaxChunkCandidates: base.maxCandidates, MaxChunksPerDocumentHit: base.maxChunksPerDocumentHit}, base.search)
	if err != nil {
		return nil, nil, err
	}
	return view, resultLocations, nil
}

func filterForDocumentMapping(ctx context.Context, segment *Segment, documents map[fts.DocID][]VectorID) (vector.BitSet, error) {
	liveIDs := make(map[VectorID]struct{})
	for _, ids := range documents {
		for _, id := range ids {
			liveIDs[id] = struct{}{}
		}
	}
	allowed := make([]vector.Ordinal, 0, segment.Len())
	for ordinal, row := range segment.rows {
		if ordinal%64 == 0 {
			if err := ctx.Err(); err != nil {
				return vector.BitSet{}, err
			}
		}
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

func cloneDocumentMapping(ctx context.Context, source map[fts.DocID][]VectorID) (map[fts.DocID][]VectorID, error) {
	result := make(map[fts.DocID][]VectorID, len(source))
	i := 0
	for docID, ids := range source {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		result[docID] = append([]VectorID(nil), ids...)
		i++
	}
	return result, nil
}

func cloneLocations(ctx context.Context, source map[VectorID]vectorLocation) (map[VectorID]vectorLocation, error) {
	result := make(map[VectorID]vectorLocation, len(source))
	i := 0
	for id, location := range source {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		result[id] = location
		i++
	}
	return result, nil
}

func (s *Service) queueVersionLocked(docID fts.DocID, batch []ChunkVector, old []VectorID) error {
	liveCount := 0
	for _, ids := range s.currentByDoc {
		liveCount += len(ids)
	}
	if liveCount-len(old)+len(batch) > s.config.MaxVectors {
		return ErrCapacityExceeded
	}
	pendingOld := 0
	for _, id := range old {
		if _, published := s.locations[id]; !published {
			pendingOld++
		}
	}
	if len(s.pendingVectors)-pendingOld > s.config.HNSWBuild.MaxVectors-len(batch) || s.mutationVersion == math.MaxUint64 {
		if s.mutationVersion == math.MaxUint64 {
			return ErrRevisionExhausted
		}
		return ErrCapacityExceeded
	}
	ids, err := s.allocateIDsLocked(len(batch))
	if err != nil {
		return err
	}
	old = s.coalescePendingLocked(old)
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

// coalescePendingLocked removes superseded, unpublished rows and their queued
// visibility changes. It returns only IDs that already belong to published
// segments and still need a liveness update.
func (s *Service) coalescePendingLocked(ids []VectorID) []VectorID {
	pending := make(map[VectorID]struct{})
	published := make([]VectorID, 0, len(ids))
	for _, id := range ids {
		if _, exists := s.locations[id]; exists {
			published = append(published, id)
		} else {
			pending[id] = struct{}{}
		}
	}
	if len(pending) == 0 {
		return published
	}
	vectors := s.pendingVectors[:0]
	for _, item := range s.pendingVectors {
		if _, remove := pending[item.row.VectorID]; !remove {
			vectors = append(vectors, item)
		}
	}
	s.pendingVectors = vectors
	changes := s.pendingVisibilityChanges[:0]
	for _, change := range s.pendingVisibilityChanges {
		kept := change.ids[:0]
		for _, id := range change.ids {
			if _, remove := pending[id]; !remove {
				kept = append(kept, id)
			}
		}
		if len(kept) > 0 {
			change.ids = kept
			changes = append(changes, change)
		}
	}
	s.pendingVisibilityChanges = changes
	return published
}

func (s *Service) viewSegmentsPhysical() []visibleSegment {
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.lockFlush(ctx); err != nil {
		return err
	}
	defer s.unlockFlush()
	if err := s.lockState(ctx); err != nil {
		return err
	}
	version := s.mutationVersion
	published := s.published
	config := s.config
	componentID := s.nextComponentID
	hasPending := s.mutationVersion != s.published.generation
	s.unlockState()
	if hasPending {
		return ErrPendingMutations
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
	var merged *Segment
	var rows []VectorRow
	if published.liveCount > 0 {
		if componentID == ComponentID(math.MaxUint64) {
			return ErrComponentIDExhausted
		}
		var err error
		merged, rows, err = buildCompactedSegment(ctx, published, componentID, config)
		if err != nil {
			return err
		}
	}
	locations := make(map[VectorID]vectorLocation, len(rows))
	for ordinal, row := range rows {
		if ordinal%64 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		locations[row.VectorID] = vectorLocation{component: componentID, ordinal: vector.Ordinal(ordinal)}
	}
	segments := []visibleSegment(nil)
	if merged != nil {
		segments = []visibleSegment{{segment: merged, filter: vector.NewFullBitSet(uint32(len(rows)))}}
	}
	view, err := newReadView(ctx, version, segments, published.descriptor, SearchPolicy{MaxK: config.MaxK, MaxChunkCandidates: config.MaxChunkCandidates, MaxChunksPerDocumentHit: config.MaxChunksPerDocumentHit}, config.HNSWSearch)
	if err != nil {
		return err
	}
	if err := s.lockState(ctx); err != nil {
		return err
	}
	defer s.unlockState()
	if version != s.mutationVersion {
		return ErrPublicationConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.published = view
	s.locations = locations
	if merged != nil {
		s.nextComponentID++
	}
	return nil
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
	seenChunks := make(map[chunk.ID]struct{}, len(batch))
	for i, item := range batch {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if item.Ref.DocID != docID || item.Ref.ID == "" || item.Ref.Field == "" || item.Ref.StartByte > item.Ref.EndByte {
			return ErrInvalidBatch
		}
		if _, exists := seenChunks[item.Ref.ID]; exists {
			return ErrInvalidBatch
		}
		seenChunks[item.Ref.ID] = struct{}{}
		if err := s.calculator.Validate(item.Vector); err != nil {
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
	s.lockStateUninterruptible()
	defer s.unlockState()
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
