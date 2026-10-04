package semantic

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/dariasmyr/fts-engine/internal/vector/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type pendingVector struct {
	row    VectorRow
	vector []float32
}

type vectorLocation struct {
	component uint64
	ordinal   vector.Ordinal
}

type documentVersion struct {
	firstVectorID uint64
	vectorCount   int
}

func (v documentVersion) vectorID(index int) uint64 {
	return v.firstVectorID + uint64(index)
}

type Service struct {
	stateGate chan struct{}
	flushGate chan struct{}

	config     Config
	calculator vector.Calculator
	published  *ReadView

	pendingVectors     []pendingVector
	pendingDisabledIDs []uint64
	currentByDoc       map[fts.DocID]documentVersion
	locations          map[uint64]vectorLocation
	liveVectorCount    int
	maxAllocatedID     uint64
	nextComponentID    uint64
	mutationVersion    uint64
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
	buildOptions, ok := config.hnswOptions()
	if !ok {
		return nil, ErrInvalidConfig
	}

	published, err := newReadView(context.Background(), 0, nil, config.pipelineDescriptor(), config.searchPolicy(), buildOptions.Search)
	if err != nil {
		return nil, err
	}
	return &Service{
		config:          config,
		calculator:      calculator,
		published:       published,
		stateGate:       make(chan struct{}, 1),
		flushGate:       make(chan struct{}, 1),
		nextComponentID: 1,
		currentByDoc:    make(map[fts.DocID]documentVersion),
		locations:       make(map[uint64]vectorLocation),
	}, nil
}

func (s *Service) Embedding() EmbeddingDescriptor { return s.config.Embedding }

func (s *Service) Chunking() ChunkingDescriptor { return s.config.Chunking }

func (s *Service) validateEncoder(encoder Encoder) error {
	if encoder == nil {
		return ErrInvalidConfig
	}
	return validatePipelineCompatibility(encoder.Descriptor(), s.config.pipelineDescriptor())
}

// AddDocument encodes a document through encoder and queues its vectors.
func (s *Service) AddDocument(ctx context.Context, encoder Encoder, document fts.Document) error {
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

func (s *Service) addEncodedDocument(ctx context.Context, docID fts.DocID, batch []EncodedChunk) error {
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
	return s.queueVersionLocked(docID, batch, documentVersion{})
}

// ReplaceDocument encodes a document version through encoder and queues it.
func (s *Service) ReplaceDocument(ctx context.Context, encoder Encoder, document fts.Document) error {
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

func (s *Service) replaceEncodedDocument(ctx context.Context, docID fts.DocID, batch []EncodedChunk) error {
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
	current := s.currentByDoc[docID]
	publishedOldIDs, err := s.discardSupersededVersion(current)
	if err != nil {
		return err
	}
	delete(s.currentByDoc, docID)
	s.liveVectorCount -= current.vectorCount
	if len(publishedOldIDs) > 0 {
		s.pendingDisabledIDs = append(s.pendingDisabledIDs, publishedOldIDs...)
	}
	s.mutationVersion++
	return nil
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

func (s *Service) queueVersionLocked(docID fts.DocID, encodedChunks []EncodedChunk, old documentVersion) error {
	nextLiveVectorCount := s.liveVectorCount - old.vectorCount + len(encodedChunks)
	if nextLiveVectorCount > s.config.Limits.MaxLiveVectors {
		return ErrCapacityExceeded
	}
	if s.mutationVersion == math.MaxUint64 {
		return ErrRevisionExhausted
	}
	version, err := s.allocateVersionLocked(len(encodedChunks))
	if err != nil {
		return err
	}
	publishedOldIDs, err := s.discardSupersededVersion(old)
	if err != nil {
		return err
	}
	for i, item := range encodedChunks {
		id := version.vectorID(i)
		s.pendingVectors = append(s.pendingVectors, pendingVector{
			row:    VectorRow{VectorID: id, Chunk: item.Ref},
			vector: append([]float32(nil), item.Vector...),
		})
	}
	s.currentByDoc[docID] = version
	if len(publishedOldIDs) > 0 {
		s.pendingDisabledIDs = append(s.pendingDisabledIDs, publishedOldIDs...)
	}
	s.liveVectorCount = nextLiveVectorCount
	s.maxAllocatedID = version.vectorID(version.vectorCount - 1)
	s.mutationVersion++
	return nil
}

// discardSupersededVersion removes unpublished vectors of a superseded document
// version from pendingVectors. It returns published vector IDs that callers
// must add to pendingDisabledIDs for the next Flush.
func (s *Service) discardSupersededVersion(version documentVersion) ([]uint64, error) {
	if version.vectorCount == 0 {
		return nil, nil
	}

	// Classify the whole version as published or pending.
	_, published := s.locations[version.firstVectorID]
	for i := 1; i < version.vectorCount; i++ {
		_, currentPublished := s.locations[version.vectorID(i)]
		if currentPublished != published {
			return nil, ErrInternalState
		}
	}
	if published {
		ids := make([]uint64, version.vectorCount)
		for i := range ids {
			ids[i] = version.vectorID(i)
		}
		return ids, nil
	}

	// Physically discard the unpublished range from pendingVectors.
	start := sort.Search(len(s.pendingVectors), func(i int) bool {
		return s.pendingVectors[i].row.VectorID >= version.firstVectorID
	})
	end := start + version.vectorCount
	if end > len(s.pendingVectors) {
		return nil, ErrInternalState
	}
	for i := range version.vectorCount {
		if s.pendingVectors[start+i].row.VectorID != version.vectorID(i) {
			return nil, ErrInternalState
		}
	}
	copy(s.pendingVectors[start:], s.pendingVectors[end:])
	clear(s.pendingVectors[len(s.pendingVectors)-version.vectorCount:])
	s.pendingVectors = s.pendingVectors[:len(s.pendingVectors)-version.vectorCount]
	return nil, nil
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
		count += view.segment.len()
	}
	return count
}

func (s *Service) validateBatch(ctx context.Context, docID fts.DocID, batch []EncodedChunk) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(batch) == 0 || len(batch) > s.config.Limits.MaxChunksPerDocument {
		return ErrInvalidBatch
	}
	if docID == "" {
		return chunk.ErrInvalidDocID
	}
	seenChunks := make(map[chunk.ID]struct{}, len(batch))
	for i, item := range batch {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return err
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

func (s *Service) allocateVersionLocked(count int) (documentVersion, error) {
	if count <= 0 || uint64(count) > math.MaxUint64-uint64(s.maxAllocatedID) {
		return documentVersion{}, ErrVectorIDExhausted
	}
	firstVectorID := s.maxAllocatedID + 1
	if firstVectorID == 0 {
		return documentVersion{}, ErrVectorIDExhausted
	}
	return documentVersion{firstVectorID: firstVectorID, vectorCount: count}, nil
}

func (s *Service) Statistics() Statistics {
	s.lockStateUninterruptible()
	defer s.unlockState()
	physical := s.physicalVectorCountLocked()
	return Statistics{
		Documents:            len(s.currentByDoc),
		PhysicalVectors:      physical,
		LiveVectors:          s.liveVectorCount,
		StaleVectors:         physical - s.liveVectorCount,
		MaxAllocatedVectorID: s.maxAllocatedID,
	}
}
