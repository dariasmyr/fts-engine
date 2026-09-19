package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// Snapshot is one coherent, immutable semantic generation. Every segment row
// is live, and Rows[ordinal] describes that row's stable semantic identity.
type Snapshot struct {
	Space    SpaceDescriptor
	Chunking ChunkingDescriptor
	// MaxAllocatedVectorID preserves the monotonic allocator across generations.
	MaxAllocatedVectorID    VectorID
	Segment                 *Segment
	MaxK                    int
	MaxChunkCandidates      int
	MaxChunksPerDocumentHit int
}

// Snapshot materializes the current live rows into one immutable HNSW segment.
// Tombstones remain an implementation detail of the mutable service.
func (s *Service) Snapshot(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		return Snapshot{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	s.mu.RLock()
	source, err := s.head.FreezeCompact(ctx, s.live)
	if err != nil {
		s.mu.RUnlock()
		return Snapshot{}, err
	}
	rows := make([]VectorRow, 0, s.live.AllowedOrdinalCount())
	for ordinal, row := range s.vectorRows {
		if ordinal%64 == 0 {
			if err := ctx.Err(); err != nil {
				s.mu.RUnlock()
				return Snapshot{}, err
			}
		}
		if s.live.Allows(vector.Ordinal(ordinal)) {
			rows = append(rows, row)
		}
	}
	if len(rows) != source.Len() {
		s.mu.RUnlock()
		return Snapshot{}, ErrInternalState
	}
	maxAllocatedVectorID := s.maxAllocatedVectorID
	config := s.config
	s.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	segment, err := BuildSegment(ctx, MutableHeadID, SegmentMetadata{Space: config.Space, Chunking: config.Chunking}, source, rows, hnsw.BuildOptions{
		BuildConfig: hnsw.BuildConfig{
			Dimensions: config.Space.Dimensions, Metric: config.Space.Metric,
			MaxVectors:     max(config.MaxVectors, source.Len()),
			MaxVectorBytes: uint64(max(config.MaxVectors, source.Len())) * uint64(config.Space.Dimensions) * 4,
			MaxNeighbors:   config.HNSWBuild.MaxNeighbors, EfConstruction: config.HNSWBuild.EfConstruction,
			Seed: config.HNSWBuild.Seed,
		},
		SearchConfig: config.HNSWSearch,
	})
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{
		Space:                   config.Space,
		Chunking:                config.Chunking,
		MaxAllocatedVectorID:    maxAllocatedVectorID,
		Segment:                 segment,
		MaxK:                    config.MaxK,
		MaxChunkCandidates:      config.MaxChunkCandidates,
		MaxChunksPerDocumentHit: config.MaxChunksPerDocumentHit,
	}
	if err := snapshot.validate(ctx); err != nil {
		return Snapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (c Snapshot) Validate() error {
	return c.validate(context.Background())
}

func (c Snapshot) validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.Segment == nil || c.Space.ID == "" || c.Chunking.ID == "" || c.Space.VectorFormatVersion == 0 ||
		c.MaxK <= 0 || c.MaxChunkCandidates < c.MaxK || c.MaxChunksPerDocumentHit <= 0 ||
		c.Segment.MaxK() < max(c.MaxK, c.MaxChunkCandidates) {
		return ErrInvalidSnapshot
	}
	if err := c.Segment.validate(); err != nil {
		return err
	}
	rowCount := c.Segment.rowCount()
	metadata := c.Segment.Metadata()
	if metadata.Space != c.Space || metadata.Chunking != c.Chunking {
		return ErrInvalidSnapshot
	}
	space, err := vector.NewSpace(c.Space.Dimensions, c.Space.Metric)
	if err != nil || space.Normalization() != c.Space.Normalization || c.Segment.Dimensions() != c.Space.Dimensions ||
		c.Segment.Metric() != c.Space.Metric || c.Segment.Normalization() != c.Space.Normalization ||
		c.Segment.Len() != rowCount {
		return ErrInvalidSnapshot
	}
	type chunkKey struct {
		documentID fts.DocID
		chunkID    chunk.ID
	}
	seenIDs := make(map[VectorID]struct{}, rowCount)
	seenChunks := make(map[chunkKey]struct{}, rowCount)
	var maxID VectorID
	for ordinal := 0; ordinal < rowCount; ordinal++ {
		row, ok := c.Segment.rowAt(ordinal)
		if !ok {
			return ErrInvalidSnapshot
		}
		if ordinal%64 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if row.VectorID == 0 || row.Chunk.ID == "" || row.Chunk.DocID == "" || row.Chunk.Field == "" || row.Chunk.StartByte > row.Chunk.EndByte {
			return ErrInvalidSnapshot
		}
		previous, ok := c.Segment.rowAt(ordinal - 1)
		if ordinal > 0 && (!ok || previous.VectorID >= row.VectorID) {
			return ErrInvalidSnapshot
		}
		if _, duplicate := seenIDs[row.VectorID]; duplicate {
			return ErrInvalidSnapshot
		}
		seenIDs[row.VectorID] = struct{}{}
		key := chunkKey{documentID: row.Chunk.DocID, chunkID: row.Chunk.ID}
		if _, duplicate := seenChunks[key]; duplicate {
			return ErrInvalidSnapshot
		}
		seenChunks[key] = struct{}{}
		maxID = max(maxID, row.VectorID)
	}
	if c.MaxAllocatedVectorID < maxID {
		return ErrInvalidSnapshot
	}
	return ctx.Err()
}

// SearchChunks searches this immutable semantic snapshot and resolves vector
// ordinals to their chunk references.
func (s Snapshot) SearchChunks(ctx context.Context, query []float32, k int) (ChunkSearchResult, error) {
	return searchChunks(ctx, segmentView{segment: s.Segment}, query, k, s.MaxK)
}

// SearchDocuments searches this immutable semantic snapshot and groups chunk
// hits by document.
func (s Snapshot) SearchDocuments(ctx context.Context, query []float32, k int) (DocumentSearchResult, error) {
	space, _ := vector.NewSpace(s.Space.Dimensions, s.Space.Metric)
	return searchDocuments(ctx, segmentView{segment: s.Segment}, space, query, k, s.MaxK, s.MaxChunkCandidates, s.MaxChunksPerDocumentHit)
}

// Close releases resources owned by the snapshot's immutable segment.
func (s Snapshot) Close() error {
	if s.Segment == nil {
		return nil
	}
	return s.Segment.Close()
}
