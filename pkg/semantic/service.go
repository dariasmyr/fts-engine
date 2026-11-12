package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Service provides document-level semantic indexing and text search. New owns
// the standard model-bound preparation pipeline; NewService accepts external
// encoders. Index remains independent from document encoding.
type Service struct {
	index           *Index
	documentEncoder DocumentEncoder
	queryEncoder    QueryEncoder
	config          *ServiceConfig
}

// New constructs a model-backed service with the built-in chunk splitter.
func New(client embedding.Client, config ServiceConfig) (*Service, error) {
	components, err := prepareStandardModelService(client, config)
	if err != nil {
		return nil, err
	}
	index, err := NewIndex(components.config.Index)
	if err != nil {
		return nil, err
	}
	service, err := NewService(index, components.encoder, components.encoder)
	if err != nil {
		return nil, err
	}
	service.config = &components.config
	return service, nil
}

// NewFromIndex builds the configured standard model pipeline and binds it to an
// existing compatible index, including one reconstructed by Restore.
func NewFromIndex(
	client embedding.Client,
	index *Index,
	config ServiceConfig,
) (*Service, error) {
	if index == nil {
		return nil, ErrInvalidConfig
	}
	components, err := prepareStandardModelService(client, config)
	if err != nil {
		return nil, ErrModelServiceUnavailable
	}
	if components.config.Index != index.config {
		return nil, ErrModelServiceUnavailable
	}
	service, err := NewService(index, components.encoder, components.encoder)
	if err != nil {
		return nil, err
	}
	service.config = &components.config
	return service, nil
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

// Config returns the built-in model pipeline configuration. The second result
// is false for services assembled from external encoders with NewService; use
// Index().Config() to inspect their vector index configuration.
func (s *Service) Config() (ServiceConfig, bool) {
	if s.config == nil {
		return ServiceConfig{}, false
	}
	return *s.config, true
}

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

// Commit publishes pending document mutations to the in-memory searchable view.
// It performs no persistence I/O; use semanticpersist.CommitAndSave for a
// durable commit.
func (s *Service) Commit(ctx context.Context) error {
	return s.index.Commit(ctx)
}

// Compact compacts committed immutable segments in memory. It performs no
// persistence I/O. Pending mutations must be committed first.
func (s *Service) Compact(ctx context.Context) error {
	return s.index.Compact(ctx)
}

func (s *Service) Statistics() Statistics {
	return s.index.Statistics()
}

func (s *Service) View() *IndexView {
	return s.index.View()
}
