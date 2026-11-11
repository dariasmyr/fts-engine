package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/v2/internal/memorystore"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

// segment searches one immutable HNSW segment. The vector source is
// authoritative row storage and the HNSW index contains only navigation topology.
type segment struct {
	id      SegmentID
	space   vector.Space
	rows    []VectorRow
	vectors vector.PreparedVectorStore
	index   *hnsw.Index
}

// buildSegment builds the target immutable semantic segment. The HNSW index
// navigates source rows by local ordinal; rows resolve those ordinals to stable
// semantic identities.
func buildSegment(
	ctx context.Context,
	id SegmentID,
	space vector.Space,
	source vector.PreparedVectorStore,
	rows []VectorRow,
	config hnsw.Config,
) (*segment, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if id == 0 || space.Dimensions() <= 0 {
		return nil, ErrInvalidSegment
	}

	index, err := hnsw.Build(ctx, source, config)
	if err != nil {
		return nil, err
	}

	return newBuiltSegment(id, space, source, index, rows)
}

func buildMaterializedSegment(
	ctx context.Context,
	id SegmentID,
	values []float32,
	rows []VectorRow,
	config IndexConfig,
) (*segment, error) {
	space, err := vector.NewSpace(config.Dimensions, config.Metric)
	if err != nil {
		return nil, err
	}
	source, err := memorystore.NewPrepared(space, values)
	if err != nil {
		return nil, err
	}
	return buildSegment(ctx, id, space, source, rows, config.hnswConfig())
}

// newBuiltSegment takes ownership of rows produced together with an index by
// the package's ingestion or compaction path.
func newBuiltSegment(id SegmentID, space vector.Space, vectors vector.PreparedVectorStore, index *hnsw.Index, rows []VectorRow) (*segment, error) {
	segment := &segment{
		id:      id,
		space:   space,
		rows:    rows,
		vectors: vectors,
		index:   index,
	}
	if err := segment.validateContents(); err != nil {
		return nil, err
	}
	return segment, nil
}

func newSegment(ctx context.Context, id SegmentID, space vector.Space, vectors vector.PreparedVectorStore, index *hnsw.Index, rows []VectorRow) (*segment, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if id == 0 || vectors == nil || index == nil || len(rows) != index.Len() {
		return nil, ErrInvalidSegment
	}
	if err := validateSegmentRowsContext(ctx, rows); err != nil {
		return nil, err
	}

	segment := &segment{
		id:      id,
		space:   space,
		rows:    append([]VectorRow(nil), rows...),
		vectors: vectors,
		index:   index,
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

func (s *segment) vectorStore() vector.PreparedVectorStore {
	if s == nil {
		return nil
	}
	return s.vectors
}

func (s *segment) searchPrepared(ctx context.Context, query vector.PreparedVector, k int, options hnsw.QueryConfig) (hnsw.SearchResult, error) {
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
	if s == nil || s.index == nil || s.vectors == nil {
		return ErrInvalidSegment
	}
	vectors := s.vectors
	space := s.space
	if s.id == 0 || space.Dimensions() <= 0 || len(s.rows) != vectors.Len() || s.index.Len() != vectors.Len() ||
		vectors.Dimensions() != space.Dimensions() || vectors.Metric() != space.Metric() ||
		vectors.Normalization() != space.Normalization() || s.index.Dimensions() != vectors.Dimensions() ||
		s.index.Metric() != vectors.Metric() {
		return ErrInvalidSegment
	}
	return nil
}
