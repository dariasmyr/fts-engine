package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/internal/contextcheck"
	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

// segment searches one immutable HNSW component. The vector source is
// authoritative row storage and the HNSW index contains only navigation topology.
type segment struct {
	component  uint64
	descriptor PipelineDescriptor
	rows       []VectorRow
	vectors    vector.PreparedVectorStore
	search     hnsw.SearchConfig
	index      *hnsw.Index
}

// buildSegment builds the target immutable semantic segment. The HNSW index
// navigates source rows by local ordinal; rows resolve those ordinals to stable
// semantic identities.
func buildSegment(ctx context.Context, component uint64, descriptor PipelineDescriptor, source vector.PreparedVectorStore, rows []VectorRow, options hnsw.BuildOptions) (*segment, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if component == 0 || !descriptor.Embedding.IsValid() || !descriptor.Chunking.IsValid() {
		return nil, ErrInvalidSegment
	}
	index, err := hnsw.Build(ctx, source, options)
	if err != nil {
		return nil, err
	}
	return newBuiltSegment(component, descriptor, source, index, rows)
}

// newBuiltSegment takes ownership of rows produced together with an index by
// the package's ingestion or compaction path.
func newBuiltSegment(component uint64, descriptor PipelineDescriptor, vectors vector.PreparedVectorStore, index *hnsw.Index, rows []VectorRow) (*segment, error) {
	segment := &segment{
		component:  component,
		descriptor: descriptor,
		rows:       rows,
		vectors:    vectors,
		search:     index.Report().Search,
		index:      index,
	}
	if err := segment.validateContents(); err != nil {
		return nil, err
	}
	return segment, nil
}

func newSegment(ctx context.Context, component uint64, descriptor PipelineDescriptor, vectors vector.PreparedVectorStore, index *hnsw.Index, rows []VectorRow) (*segment, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if component == 0 || vectors == nil || index == nil || len(rows) != index.Len() {
		return nil, ErrInvalidSegment
	}
	if err := validateSegmentRowsContext(ctx, rows); err != nil {
		return nil, err
	}
	if err := index.ValidateSource(ctx, vectors); err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		return nil, ErrInvalidSegment
	}
	segment := &segment{
		component:  component,
		descriptor: descriptor,
		rows:       append([]VectorRow(nil), rows...),
		vectors:    vectors,
		search:     index.Report().Search,
		index:      index,
	}
	if err := segment.validateContents(); err != nil {
		return nil, err
	}
	return segment, nil
}

func (s *segment) componentID() uint64 {
	if s == nil {
		return 0
	}
	return s.component
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

func (s *segment) searchPrepared(ctx context.Context, query vector.PreparedQuery, k int, options vector.SearchOptions) (vector.SearchResult, error) {
	if s == nil || s.index == nil {
		return vector.SearchResult{}, ErrInvalidSegment
	}
	return s.index.SearchPrepared(ctx, query, k, options)
}

func (s *segment) len() int {
	if s == nil || s.index == nil {
		return 0
	}
	return s.index.Len()
}

func (s *segment) dimensions() int {
	if s == nil || s.index == nil {
		return 0
	}
	return s.index.Dimensions()
}

func (s *segment) searchConfig() hnsw.SearchConfig {
	if s == nil {
		return hnsw.SearchConfig{}
	}
	return s.search
}

func validateSegmentRowsContext(ctx context.Context, rows []VectorRow) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	type chunkKey struct {
		documentID fts.DocID
		chunkID    chunk.ID
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
	if !s.descriptor.Embedding.IsValid() || !s.descriptor.Chunking.IsValid() {
		return ErrInvalidSegment
	}
	calculator, err := s.descriptor.Embedding.Calculator()
	if err != nil || s.component == 0 || len(s.rows) != vectors.Len() || s.index.Len() != vectors.Len() ||
		vectors.Dimensions() != calculator.Dimensions() || vectors.Metric() != calculator.Metric() ||
		vectors.Normalization() != calculator.Normalization() || s.index.Dimensions() != vectors.Dimensions() ||
		s.index.Metric() != vectors.Metric() || s.index.Report().Search != s.search {
		return ErrInvalidSegment
	}
	return nil
}
