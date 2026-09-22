package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// SegmentMetadata describes the embedding and chunking contracts of a segment.
type SegmentMetadata struct {
	Space    SpaceDescriptor
	Chunking ChunkingDescriptor
}

// SegmentKind identifies the persisted semantic segment format.
type SegmentKind uint8

const SegmentKindChunkHNSW SegmentKind = 1

// Segment searches one immutable HNSW component. The vector source is
// authoritative row storage and the searcher contains only navigation topology.
type Segment struct {
	component ComponentID
	metadata  SegmentMetadata
	rows      []VectorRow
	searcher  *hnsw.Searcher
}

func (s *Segment) Kind() SegmentKind {
	if s == nil {
		return 0
	}
	return SegmentKindChunkHNSW
}

// BuildSegment builds the target immutable semantic segment. The searcher
// navigates source rows by local ordinal; rows resolve those ordinals to stable
// semantic identities.
func BuildSegment(ctx context.Context, component ComponentID, metadata SegmentMetadata, source vector.PreparedVectorSource, rows []VectorRow, options hnsw.BuildOptions) (*Segment, error) {
	searcher, err := hnsw.BuildSearcher(ctx, source, options)
	if err != nil {
		return nil, err
	}
	return NewSegment(component, metadata, searcher, rows)
}

// NewSegment creates an immutable semantic segment from an HNSW searcher and
// rows that resolve its local ordinals to stable semantic identities.
func NewSegment(component ComponentID, metadata SegmentMetadata, searcher *hnsw.Searcher, rows []VectorRow) (*Segment, error) {
	if component == 0 || searcher == nil || len(rows) != searcher.Len() {
		return nil, ErrInvalidSegment
	}
	segment := &Segment{
		component: component,
		metadata:  metadata,
		rows:      append([]VectorRow(nil), rows...),
		searcher:  searcher,
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
func (s *Segment) Vectors() vector.PreparedVectorSource {
	if s == nil {
		return nil
	}
	return s.searcher.VectorSource()
}

// Searcher returns the immutable ANN searcher used by this segment.
func (s *Segment) Searcher() *hnsw.Searcher {
	if s == nil {
		return nil
	}
	return s.searcher
}

func (s *Segment) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if s == nil || s.searcher == nil {
		return vector.SearchResult{}, ErrInvalidSegment
	}
	return s.searcher.Search(ctx, query, k, options)
}

func (s *Segment) Len() int {
	if s == nil || s.searcher == nil {
		return 0
	}
	return s.searcher.Len()
}

func (s *Segment) Dimensions() int {
	if s == nil || s.searcher == nil {
		return 0
	}
	return s.searcher.Dimensions()
}

func (s *Segment) Metric() vector.Metric {
	if s == nil || s.searcher == nil {
		return 0
	}
	return s.searcher.Metric()
}

func (s *Segment) Normalization() vector.Normalization {
	if s == nil || s.searcher == nil {
		return 0
	}
	return s.searcher.Normalization()
}

func (s *Segment) MaxK() int {
	if s == nil || s.searcher == nil {
		return 0
	}
	if s.searcher != nil {
		return s.searcher.MaxK()
	}
	return 0
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
	if s == nil || s.searcher == nil || s.searcher.VectorSource() == nil {
		return ErrInvalidSegment
	}
	vectors := s.searcher.VectorSource()
	space, err := vector.NewSpace(s.metadata.Space.Dimensions, s.metadata.Space.Metric)
	if err != nil || space.Normalization() != s.metadata.Space.Normalization || s.metadata.Space.ID == "" || s.metadata.Chunking.ID == "" {
		return ErrInvalidSegment
	}
	if s.component == 0 || len(s.rows) != vectors.Len() || s.searcher.Len() != vectors.Len() ||
		vectors.Dimensions() != s.metadata.Space.Dimensions || vectors.Metric() != s.metadata.Space.Metric ||
		vectors.Normalization() != s.metadata.Space.Normalization {
		return ErrInvalidSegment
	}
	return nil
}
