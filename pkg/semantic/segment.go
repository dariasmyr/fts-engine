package semantic

import (
	"context"

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
// authoritative row storage and the graph contains only navigation topology.
type Segment struct {
	component ComponentID
	metadata  SegmentMetadata
	rows      []VectorRow
	vectors   vector.PreparedVectorSource
	graph     *hnsw.Reader
}

func (s *Segment) Kind() SegmentKind {
	if s == nil {
		return 0
	}
	return SegmentKindChunkHNSW
}

// BuildSegment builds the target immutable semantic segment. The graph
// navigates source rows by local ordinal; rows resolve those ordinals to stable
// semantic identities.
func BuildSegment(ctx context.Context, component ComponentID, metadata SegmentMetadata, source vector.PreparedVectorSource, rows []VectorRow, options hnsw.BuildOptions) (*Segment, error) {
	graph, err := hnsw.BuildIndexReader(ctx, source, options)
	if err != nil {
		return nil, err
	}
	return NewSegment(component, metadata, source, graph, rows)
}

// NewSegment creates an immutable semantic segment from a prepared source
// and its HNSW reader.
func NewSegment(component ComponentID, metadata SegmentMetadata, source vector.PreparedVectorSource, graph *hnsw.Reader, rows []VectorRow) (*Segment, error) {
	if component == 0 || source == nil || graph == nil || len(rows) != source.Len() || graph.Len() != source.Len() {
		return nil, ErrInvalidSnapshot
	}
	segment := &Segment{
		component: component,
		metadata:  metadata,
		rows:      append([]VectorRow(nil), rows...),
		vectors:   source,
		graph:     graph,
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
	return s.vectors
}

// HNSW returns the immutable graph reader used by this ANN segment.
func (s *Segment) HNSW() *hnsw.Reader {
	if s == nil {
		return nil
	}
	return s.graph
}

func (s *Segment) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if s == nil || s.graph == nil {
		return vector.SearchResult{}, ErrInvalidSnapshot
	}
	return s.graph.Search(ctx, query, k, options)
}

func (s *Segment) Len() int {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Len()
}

func (s *Segment) Dimensions() int {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Dimensions()
}

func (s *Segment) Metric() vector.Metric {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Metric()
}

func (s *Segment) Normalization() vector.Normalization {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Normalization()
}

func (s *Segment) MaxK() int {
	if s == nil || s.vectors == nil {
		return 0
	}
	if s.graph != nil {
		return s.graph.MaxK()
	}
	return 0
}

func (s *Segment) Close() error {
	// Current sealed readers are fully loaded and Close is a no-op. Keeping the
	// segment non-owning makes shallow immutable snapshots safe.
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

func (s *Segment) validateContents() error {
	if s == nil || s.vectors == nil || s.graph == nil {
		return ErrInvalidSnapshot
	}
	space, err := vector.NewSpace(s.metadata.Space.Dimensions, s.metadata.Space.Metric)
	if err != nil || space.Normalization() != s.metadata.Space.Normalization || s.metadata.Space.ID == "" || s.metadata.Chunking.ID == "" {
		return ErrInvalidSnapshot
	}
	if s.component == 0 || len(s.rows) != s.vectors.Len() || s.graph.Len() != s.vectors.Len() ||
		s.vectors.Dimensions() != s.metadata.Space.Dimensions || s.vectors.Metric() != s.metadata.Space.Metric ||
		s.vectors.Normalization() != s.metadata.Space.Normalization {
		return ErrInvalidSnapshot
	}
	return nil
}
