package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

// SegmentMetadata describes the embedding and chunking contracts of a segment.
type SegmentMetadata struct {
	Embedding EmbeddingDescriptor
	Chunking  ChunkingDescriptor
}

// SegmentKind identifies the persisted semantic segment format.
type SegmentKind uint8

const SegmentKindChunkHNSW SegmentKind = 1

// Segment searches one immutable HNSW component. The vector source is
// authoritative row storage and the HNSW index contains only navigation topology.
type Segment struct {
	component ComponentID
	metadata  SegmentMetadata
	rows      []VectorRow
	vectors   vectorstore.PreparedVectorStore
	search    hnsw.SearchConfig
	index     *hnsw.Index
}

func (s *Segment) Kind() SegmentKind {
	if s == nil {
		return 0
	}
	return SegmentKindChunkHNSW
}

// BuildSegment builds the target immutable semantic segment. The HNSW index
// navigates source rows by local ordinal; rows resolve those ordinals to stable
// semantic identities.
func BuildSegment(ctx context.Context, component ComponentID, metadata SegmentMetadata, source vectorstore.PreparedVectorStore, rows []VectorRow, options hnsw.BuildOptions) (*Segment, error) {
	index, err := hnsw.Build(ctx, source, options)
	if err != nil {
		return nil, err
	}
	return NewSegment(ctx, component, metadata, source, index, rows)
}

// NewSegment creates an immutable semantic segment from an HNSW index and
// rows that resolve its local ordinals to stable semantic identities.
func NewSegment(ctx context.Context, component ComponentID, metadata SegmentMetadata, vectors vectorstore.PreparedVectorStore, index *hnsw.Index, rows []VectorRow) (*Segment, error) {
	if component == 0 || vectors == nil || index == nil || len(rows) != index.Len() {
		return nil, ErrInvalidSegment
	}
	if err := index.ValidateSource(ctx, vectors); err != nil {
		if ctx == nil || ctx.Err() != nil {
			return nil, err
		}
		return nil, ErrInvalidSegment
	}
	segment := &Segment{
		component: component,
		metadata:  metadata,
		rows:      append([]VectorRow(nil), rows...),
		vectors:   vectors,
		search:    index.Report().Search,
		index:     index,
	}
	if err := segment.validateContents(); err != nil {
		return nil, err
	}
	return segment, nil
}

func (s *Segment) ComponentID() ComponentID {
	if s == nil {
		return 0
	}
	return s.component
}

func (s *Segment) Rows() []VectorRow {
	if s == nil {
		return nil
	}
	return append([]VectorRow(nil), s.rows...)
}

func (s *Segment) rowCount() int {
	if s == nil {
		return 0
	}
	return len(s.rows)
}

func (s *Segment) rowAt(index int) (VectorRow, bool) {
	if s == nil || index < 0 || index >= len(s.rows) {
		return VectorRow{}, false
	}
	return s.rows[index], true
}

// Vectors returns the immutable authoritative source rows.
func (s *Segment) Vectors() vectorstore.PreparedVectorStore {
	if s == nil {
		return nil
	}
	return s.vectors
}

// Index returns the immutable HNSW index used by this segment.
func (s *Segment) Index() *hnsw.Index {
	if s == nil {
		return nil
	}
	return s.index
}

func (s *Segment) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if s == nil || s.index == nil {
		return vector.SearchResult{}, ErrInvalidSegment
	}
	return s.index.Search(ctx, query, k, options)
}

func (s *Segment) Len() int {
	if s == nil || s.index == nil {
		return 0
	}
	return s.index.Len()
}

func (s *Segment) Dimensions() int {
	if s == nil || s.index == nil {
		return 0
	}
	return s.index.Dimensions()
}

func (s *Segment) Metric() vector.Metric {
	if s == nil || s.index == nil {
		return 0
	}
	return s.index.Metric()
}

func (s *Segment) Normalization() vector.Normalization {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Normalization()
}

func (s *Segment) MaxK() int {
	if s == nil || s.index == nil {
		return 0
	}
	return s.search.MaxK
}

// SearchLimits returns the immutable HNSW work limits owned by this segment.
func (s *Segment) SearchLimits() hnsw.SearchConfig {
	if s == nil {
		return hnsw.SearchConfig{}
	}
	return s.search
}

func (s *Segment) Close() error {
	// Current sealed readers are fully loaded and Close is a no-op. Keeping the
	// segment non-owning makes shallow immutable views safe.
	return nil
}

func (s *Segment) Metadata() SegmentMetadata {
	if s == nil {
		return SegmentMetadata{}
	}
	return s.metadata
}

func (s *Segment) validate() error {
	return s.validateContents()
}

// Validate checks the immutable segment metadata, source binding and row
// identity mapping.
func (s *Segment) Validate() error {
	if err := s.validateContents(); err != nil {
		return err
	}
	seenIDs := make(map[VectorID]struct{}, len(s.rows))
	type chunkKey struct {
		documentID fts.DocID
		chunkID    chunk.ID
	}
	seenChunks := make(map[chunkKey]struct{}, len(s.rows))
	for i, row := range s.rows {
		if row.VectorID == 0 || row.Chunk.ID == "" || row.Chunk.DocID == "" || row.Chunk.Field == "" || row.Chunk.StartByte > row.Chunk.EndByte {
			return ErrInvalidSegment
		}
		if i > 0 && s.rows[i-1].VectorID >= row.VectorID {
			return ErrInvalidSegment
		}
		if _, exists := seenIDs[row.VectorID]; exists {
			return ErrInvalidSegment
		}
		seenIDs[row.VectorID] = struct{}{}
		key := chunkKey{documentID: row.Chunk.DocID, chunkID: row.Chunk.ID}
		if _, exists := seenChunks[key]; exists {
			return ErrInvalidSegment
		}
		seenChunks[key] = struct{}{}
	}
	return nil
}

func (s *Segment) validateContents() error {
	if s == nil || s.index == nil || s.vectors == nil {
		return ErrInvalidSegment
	}
	vectors := s.vectors
	if !s.metadata.Embedding.IsValid() || !s.metadata.Chunking.IsValid() {
		return ErrInvalidSegment
	}
	calculator, err := s.metadata.Embedding.Calculator()
	if err != nil || s.component == 0 || len(s.rows) != vectors.Len() || s.index.Len() != vectors.Len() ||
		vectors.Dimensions() != calculator.Dimensions() || vectors.Metric() != calculator.Metric() ||
		vectors.Normalization() != calculator.Normalization() || s.index.Dimensions() != vectors.Dimensions() ||
		s.index.Metric() != vectors.Metric() || s.index.Report().Search != s.search {
		return ErrInvalidSegment
	}
	return nil
}
