package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Service provides document-level semantic indexing and search.
// Encoder is registered once at construction.
//
// Service does not own embedding or chunking implementations.
// Index remains independent from document encoding.
type Service struct {
	index   *Index
	encoder Encoder
}

// New constructs a document service with a registered encoder.
// The encoder schema must match the index schema.
func New(config Config, encoder Encoder) (*Service, error) {
	if encoder == nil {
		return nil, ErrInvalidConfig
	}

	index, err := NewIndex(config)
	if err != nil {
		return nil, err
	}

	if err := validateSchemaCompatibility(
		encoder.Descriptor(),
		index.Schema(),
	); err != nil {
		return nil, err
	}

	return &Service{
		index:   index,
		encoder: encoder,
	}, nil
}

// Index returns the underlying semantic index for operations
// that work directly with prepared embeddings.
func (s *Service) Index() *Index {
	return s.index
}

// Schema returns the semantic compatibility schema.
func (s *Service) Schema() Schema {
	return s.index.Schema()
}

func (s *Service) Embedding() EmbeddingDescriptor {
	return s.index.Embedding()
}

func (s *Service) Chunking() ChunkingDescriptor {
	return s.index.Chunking()
}

// AddDocument encodes and stages a new document.
// Changes become searchable after Flush.
func (s *Service) AddDocument(
	ctx context.Context,
	document fts.Document,
) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	batch, err := s.encoder.Encode(ctx, document)
	if err != nil {
		return err
	}

	return s.index.Add(ctx, document.ID, batch)
}

// ReplaceDocument encodes and stages a replacement document.
// Changes become searchable after Flush.
func (s *Service) ReplaceDocument(
	ctx context.Context,
	document fts.Document,
) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	batch, err := s.encoder.Encode(ctx, document)
	if err != nil {
		return err
	}

	return s.index.Replace(ctx, document.ID, batch)
}

// DeleteDocument stages deletion of an indexed document.
func (s *Service) DeleteDocument(
	ctx context.Context,
	docID fts.DocID,
) error {
	return s.index.Delete(ctx, docID)
}

// SearchDocuments encodes the query document and searches
// the committed semantic snapshot.
func (s *Service) SearchDocuments(
	ctx context.Context,
	query fts.Document,
	maxResultCount int,
) (DocumentSearchResult, error) {
	return s.SearchDocumentsWithOptions(
		ctx,
		query,
		maxResultCount,
		SearchOptions{},
	)
}

// SearchDocumentsWithOptions encodes the query and searches
// with explicit ANN and grouping options.
func (s *Service) SearchDocumentsWithOptions(
	ctx context.Context,
	query fts.Document,
	maxResultCount int,
	options SearchOptions,
) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}

	queries, err := s.encoder.Encode(ctx, query)
	if err != nil {
		return DocumentSearchResult{}, err
	}

	return s.index.Search(
		ctx,
		queries,
		maxResultCount,
		options,
	)
}

// Flush publishes pending mutations as a committed snapshot.
func (s *Service) Flush(ctx context.Context) error {
	return s.index.Flush(ctx)
}

// Compact compacts committed immutable segments.
// Pending mutations must be flushed first.
func (s *Service) Compact(ctx context.Context) error {
	return s.index.Compact(ctx)
}

func (s *Service) Statistics() Statistics {
	return s.index.Statistics()
}

func (s *Service) Snapshot() *Snapshot {
	return s.index.Snapshot()
}

// State exports the committed index state for persistence.
func (s *Service) State(ctx context.Context) (*State, error) {
	return s.index.State(ctx)
}
