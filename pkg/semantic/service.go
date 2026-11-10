package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Service provides document-level semantic indexing and text search. New and
// NewCustom own model-bound document/query preparation; NewService accepts
// custom encoders. Index remains independent from document encoding.
type Service struct {
	index           *Index
	documentEncoder DocumentEncoder
	queryEncoder    QueryEncoder
}

// New constructs a model-backed service with the built-in chunk splitter.
func New(client embedding.Client, config SemanticConfig) (*Service, error) {
	components, err := prepareStandardModelService(client, config)
	if err != nil {
		return nil, err
	}
	index, err := NewIndex(components.config)
	if err != nil {
		return nil, err
	}
	return NewService(index, components.documents, components.queries)
}

// NewCustom constructs a model-backed service with a runtime chunk splitter.
// The static config must select ChunkingKindCustom.
func NewCustom(
	client embedding.Client,
	config SemanticConfig,
	splitter ChunkSplitter,
) (*Service, error) {
	components, err := prepareCustomModelService(client, config, splitter)
	if err != nil {
		return nil, err
	}
	index, err := NewIndex(components.config)
	if err != nil {
		return nil, err
	}
	return NewService(index, components.documents, components.queries)
}

// NewFromIndex reconstructs the persisted standard model pipeline and binds it
// to an existing index, including one reconstructed by Restore.
func NewFromIndex(
	client embedding.Client,
	index *Index,
) (*Service, error) {
	if index == nil {
		return nil, ErrInvalidConfig
	}
	components, err := prepareStandardModelService(client, index.config)
	if err != nil {
		return nil, ErrModelServiceUnavailable
	}
	return NewService(index, components.documents, components.queries)
}

// NewCustomFromIndex binds a runtime chunk splitter to an existing custom
// index, including one reconstructed by Restore.
func NewCustomFromIndex(
	client embedding.Client,
	index *Index,
	splitter ChunkSplitter,
) (*Service, error) {
	if index == nil {
		return nil, ErrInvalidConfig
	}
	components, err := prepareCustomModelService(client, index.config, splitter)
	if err != nil {
		return nil, ErrModelServiceUnavailable
	}
	return NewService(index, components.documents, components.queries)
}

// NewService binds custom document and query encoders to an existing index.
// Encoders are responsible for producing vectors with the dimensions expected
// by the index; each encoded batch is validated before it is staged or searched.
func NewService(
	index *Index,
	documentEncoder DocumentEncoder,
	queryEncoder QueryEncoder,
) (*Service, error) {
	if index == nil || documentEncoder == nil || queryEncoder == nil {
		return nil, ErrInvalidConfig
	}
	return &Service{
		index:           index,
		documentEncoder: documentEncoder,
		queryEncoder:    queryEncoder,
	}, nil
}

// Index returns the underlying semantic index for operations
// that work directly with prepared embeddings.
func (s *Service) Index() *Index {
	return s.index
}

// Config returns the static semantic configuration.
func (s *Service) Config() SemanticConfig { return s.index.Config() }

// AddDocument encodes and stages a new document.
// Changes become searchable after Commit.
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

	batch, err := s.documentEncoder.EncodeDocument(ctx, document)
	if err != nil {
		return err
	}

	return s.index.AddDocument(ctx, document.ID, batch)
}

// ReplaceDocument encodes and stages a replacement document.
// Changes become searchable after Commit.
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

	batch, err := s.documentEncoder.EncodeDocument(ctx, document)
	if err != nil {
		return err
	}

	return s.index.ReplaceDocument(ctx, document.ID, batch)
}

// DeleteDocument stages deletion of an indexed document.
func (s *Service) DeleteDocument(
	ctx context.Context,
	docID fts.DocID,
) error {
	return s.index.DeleteDocument(ctx, docID)
}

// SearchDocuments encodes the query document and searches
// the committed semantic view.
func (s *Service) SearchDocuments(
	ctx context.Context,
	query fts.Document,
	maxResultCount int,
) (DocumentSearchResult, error) {
	return s.SearchDocumentsWithConfig(
		ctx,
		query,
		maxResultCount,
		QueryConfig{},
	)
}

// SearchDocumentsWithConfig encodes the query and searches with explicit
// request-local query configuration.
func (s *Service) SearchDocumentsWithConfig(
	ctx context.Context,
	query fts.Document,
	maxResultCount int,
	config QueryConfig,
) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}

	queries, err := s.queryEncoder.EncodeQueryDocument(ctx, query, config)
	if err != nil {
		return DocumentSearchResult{}, err
	}

	return s.index.Search(
		ctx,
		queries,
		maxResultCount,
		config,
	)
}

// Search embeds one text query and searches the committed semantic view.
func (s *Service) Search(
	ctx context.Context,
	query string,
	maxResultCount int,
) (DocumentSearchResult, error) {
	return s.SearchWithConfig(ctx, query, maxResultCount, QueryConfig{})
}

// SearchWithConfig searches one text query with explicit request-local query
// configuration.
func (s *Service) SearchWithConfig(
	ctx context.Context,
	query string,
	maxResultCount int,
	config QueryConfig,
) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}
	queries, err := s.queryEncoder.EncodeQuery(ctx, query, config)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return s.index.Search(ctx, queries, maxResultCount, config)
}

// Commit publishes pending document mutations as a searchable view.
func (s *Service) Commit(ctx context.Context) error {
	return s.index.Commit(ctx)
}

// Compact compacts committed immutable segments.
// Pending mutations must be committed first.
func (s *Service) Compact(ctx context.Context) error {
	return s.index.Compact(ctx)
}

func (s *Service) Statistics() Statistics {
	return s.index.Statistics()
}

func (s *Service) View() *IndexView {
	return s.index.View()
}
