package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// segment searches one immutable HNSW segment. The HNSW index retains the
// authoritative vector store; rows map its local ordinals to semantic identities.
type segment struct {
	id    SegmentID
	spec  vector.Spec
	rows  []VectorRow
	index *hnsw.Index
}

// buildSegment builds the target immutable semantic segment. The HNSW index
// navigates source rows by local ordinal; rows resolve those ordinals to stable
// semantic identities.
func buildSegment(
	ctx context.Context,
	id SegmentID,
	spec vector.Spec,
	source *vector.Matrix,
	rows []VectorRow,
	config hnsw.Config,
) (*segment, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if id == 0 || spec.Dimensions() <= 0 {
		return nil, ErrInvalidSegment
	}

	index, err := hnsw.Build(ctx, source, config)
	if err != nil {
		return nil, err
	}

	return newBuiltSegment(id, spec, index, rows)
}

// newBuiltSegment takes ownership of rows produced together with an index by
// the package's ingestion or compaction path.
func newBuiltSegment(id SegmentID, spec vector.Spec, index *hnsw.Index, rows []VectorRow) (*segment, error) {
	segment := &segment{
		id:    id,
		spec:  spec,
		rows:  rows,
		index: index,
	}
	if err := segment.validateContents(); err != nil {
		return nil, err
	}
	return segment, nil
}

func newSegment(ctx context.Context, id SegmentID, spec vector.Spec, index *hnsw.Index, rows []VectorRow) (*segment, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id == 0 || index == nil || index.Matrix() == nil || len(rows) != index.Len() {
		return nil, ErrInvalidSegment
	}
	if err := validateSegmentRowsContext(ctx, rows); err != nil {
		return nil, err
	}

	segment := &segment{
		id:    id,
		spec:  spec,
		rows:  append([]VectorRow(nil), rows...),
		index: index,
	}
	if err := segment.validateContents(); err != nil {
		return nil, err
	}
	return segment, nil
}

func (s *segment) segmentID() SegmentID {
	if s == nil {
		return 0
	}
	return s.id
}

func (s *segment) rowAt(index int) (VectorRow, bool) {
	if s == nil || index < 0 || index >= len(s.rows) {
		return VectorRow{}, false
	}
	return s.rows[index], true
}

func (s *segment) vectorMatrix() *vector.Matrix {
	if s == nil {
		return nil
	}
	return s.index.Matrix()
}

func (s *segment) searchPrepared(ctx context.Context, query vector.Prepared, k int, options hnsw.QueryConfig) (hnsw.SearchResult, error) {
	if s == nil || s.index == nil {
		return hnsw.SearchResult{}, ErrInvalidSegment
	}
	return s.index.SearchPrepared(ctx, query, k, options)
}

func (s *segment) len() int {
	if s == nil || s.index == nil {
		return 0
	}
	return s.index.Len()
}

func validateSegmentRowsContext(ctx context.Context, rows []VectorRow) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	type chunkKey struct {
		documentID fts.DocID
		chunkID    ChunkID
	}
	seenChunks := make(map[chunkKey]struct{}, len(rows))
	for i, row := range rows {
		if err := contextcheck.PeriodicError(ctx, i); err != nil {
			return err
		}

		if row.VectorID == 0 || row.Chunk.ID == "" || row.Chunk.DocID == "" || row.Chunk.Field == "" || row.Chunk.StartByte > row.Chunk.EndByte {
			return ErrInvalidSegment
		}
		if i > 0 && rows[i-1].VectorID >= row.VectorID {
			return ErrInvalidSegment
		}
		key := chunkKey{documentID: row.Chunk.DocID, chunkID: row.Chunk.ID}
		if _, exists := seenChunks[key]; exists {
			return ErrInvalidSegment
		}
		seenChunks[key] = struct{}{}
	}
	return nil
}

func (s *segment) validateContents() error {
	if s == nil || s.index == nil || s.index.Matrix() == nil {
		return ErrInvalidSegment
	}
	vectors := s.index.Matrix()
	spec := s.spec
	if s.id == 0 || spec.Dimensions() <= 0 || len(s.rows) != vectors.Len() ||
		s.index.Len() != vectors.Len() || vectors.Spec() != spec {
		return ErrInvalidSegment
	}
	return nil
}
