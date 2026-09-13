package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// SegmentKind identifies the physical search implementation of one sealed
// semantic segment.
type SegmentKind uint8

const (
	SegmentKindChunkHNSW SegmentKind = iota + 1
)

// SealedSegment searches one immutable HNSW component. The vector source is
// authoritative row storage and the graph contains only navigation topology.
type SealedSegment struct {
	component ComponentID
	kind      SegmentKind
	rows      []VectorRow
	vectors   vector.PreparedVectorSource
	graph     *hnsw.Reader
}

// NewHNSWSegment creates the target immutable semantic segment. The graph
// navigates source rows by local ordinal; rows resolve those ordinals to stable
// semantic identities.
func NewHNSWSegment(component ComponentID, source vector.PreparedVectorSource, graph *hnsw.Reader, rows []VectorRow) (*SealedSegment, error) {
	if component == 0 || source == nil || graph == nil || len(rows) != source.Len() || graph.Len() != source.Len() {
		return nil, ErrInvalidSnapshot
	}
	segment := &SealedSegment{
		component: component,
		kind:      SegmentKindChunkHNSW,
		rows:      append([]VectorRow(nil), rows...),
		vectors:   source,
		graph:     graph,
	}
	if err := segment.validateContents(); err != nil {
		return nil, err
	}
	return segment, nil
}

func (s *SealedSegment) Kind() SegmentKind {
	if s == nil {
		return 0
	}
	return s.kind
}

func (s *SealedSegment) ComponentID() ComponentID {
	if s == nil {
		return 0
	}
	return s.component
}

func (s *SealedSegment) Rows() []VectorRow {
	if s == nil {
		return nil
	}
	return append([]VectorRow(nil), s.rows...)
}

// Vectors returns the immutable authoritative source rows.
func (s *SealedSegment) Vectors() vector.PreparedVectorSource {
	if s == nil {
		return nil
	}
	return s.vectors
}

// HNSW returns the optional immutable graph reader.
func (s *SealedSegment) HNSW() *hnsw.Reader {
	if s == nil {
		return nil
	}
	return s.graph
}

func (s *SealedSegment) Search(ctx context.Context, query []float32, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if s == nil || s.graph == nil {
		return vector.SearchResult{}, ErrInvalidSnapshot
	}
	return s.graph.Search(ctx, query, k, options)
}

func (s *SealedSegment) Len() int {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Len()
}

func (s *SealedSegment) Dimensions() int {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Dimensions()
}

func (s *SealedSegment) Metric() vector.Metric {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Metric()
}

func (s *SealedSegment) Normalization() vector.Normalization {
	if s == nil || s.vectors == nil {
		return 0
	}
	return s.vectors.Normalization()
}

func (s *SealedSegment) MaxK() int {
	if s == nil || s.vectors == nil {
		return 0
	}
	if s.graph != nil {
		return s.graph.MaxK()
	}
	return 0
}

func (s *SealedSegment) Close() error {
	// Current sealed readers are fully loaded and Close is a no-op. Keeping the
	// segment non-owning makes shallow immutable snapshots safe.
	return nil
}

func (s *SealedSegment) validate() error {
	return s.validateContents()
}

func (s *SealedSegment) validateContents() error {
	if s == nil || s.vectors == nil || s.graph == nil {
		return ErrInvalidSnapshot
	}
	if s.kind != SegmentKindChunkHNSW || s.component == 0 || len(s.rows) != s.vectors.Len() || s.graph.Len() != s.vectors.Len() {
		return ErrInvalidSnapshot
	}
	return nil
}
