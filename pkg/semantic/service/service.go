package service

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Service provides document-level semantic indexing and text search. New owns
// the standard model-bound preparation pipeline; NewService accepts external
// encoders. Index remains independent from document encoding.
type Service struct {
	index           *semantic.Index
	documentEncoder DocumentEncoder
	queryEncoder    QueryEncoder
	pipeline        *PipelineConfig
}

// New constructs a model-backed service with the built-in chunk splitter.
func New(client embedding.Client, config Config) (*Service, error) {
	components, err := prepareStandardModelService(client, config)
	if err != nil {
		return nil, err
	}
	index, err := semantic.NewIndex(components.config.Index)
	if err != nil {
		return nil, err
	}
	service, err := NewService(index, components.encoder, components.encoder)
	if err != nil {
		return nil, err
	}
	service.pipeline = &components.config.Pipeline
	return service, nil
}

// NewService binds custom document and query encoders to an existing index.
// Encoders are responsible for producing vectors with the dimensions expected
// by the index; each encoded batch is validated before it is staged or searched.
func NewService(
	index *semantic.Index,
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

// Config returns the built-in model pipeline configuration. The second result
// is false for services assembled from external encoders with NewService.
func (s *Service) Config() (Config, bool) {
	if s.pipeline == nil {
		return Config{}, false
	}
	return Config{Index: s.index.Config(), Pipeline: *s.pipeline}, true
}

// IndexConfig returns a copy of the immutable vector index configuration.
func (s *Service) IndexConfig() semantic.IndexConfig {
	return s.index.Config()
}

// ExportState returns a detached persistence-neutral copy of the committed
// index state. Pending mutations must be committed first.
func (s *Service) ExportState(ctx context.Context) (*semantic.CommittedIndexState, error) {
	return s.index.ExportState(ctx)
}

// CommitWithState prepares pending mutations and passes the exact committed
// candidate to accept before publishing it to searches. The callback must
// complete its external commit point before returning nil and must not
// reentrantly mutate, commit, or compact this Service.
func (s *Service) CommitWithState(
	ctx context.Context,
	accept func(*semantic.CommittedIndexState) error,
) error {
	return s.index.CommitWithState(ctx, accept)
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
) (semantic.DocumentSearchResult, error) {
	return s.SearchDocumentsWithConfig(
		ctx,
		query,
		maxResultCount,
		semantic.QueryConfig{},
	)
}

// SearchDocumentsWithConfig encodes the query and searches with explicit
// request-local query configuration.
func (s *Service) SearchDocumentsWithConfig(
	ctx context.Context,
	query fts.Document,
	maxResultCount int,
	config semantic.QueryConfig,
) (semantic.DocumentSearchResult, error) {
	if ctx == nil {
		return semantic.DocumentSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return semantic.DocumentSearchResult{}, err
	}
	if err := s.index.ValidateSearchRequest(ctx, maxResultCount, config); err != nil {
		return semantic.DocumentSearchResult{}, err
	}

	queries, err := s.queryEncoder.EncodeQueryDocument(ctx, query)
	if err != nil {
		return semantic.DocumentSearchResult{}, err
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
) (semantic.DocumentSearchResult, error) {
	return s.SearchWithConfig(ctx, query, maxResultCount, semantic.QueryConfig{})
}

// SearchWithConfig searches one text query with explicit request-local query
// configuration.
func (s *Service) SearchWithConfig(
	ctx context.Context,
	query string,
	maxResultCount int,
	config semantic.QueryConfig,
) (semantic.DocumentSearchResult, error) {
	if ctx == nil {
		return semantic.DocumentSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return semantic.DocumentSearchResult{}, err
	}
	if err := s.index.ValidateSearchRequest(ctx, maxResultCount, config); err != nil {
		return semantic.DocumentSearchResult{}, err
	}
	queries, err := s.queryEncoder.EncodeQuery(ctx, query)
	if err != nil {
		return semantic.DocumentSearchResult{}, err
	}
	return s.index.Search(ctx, queries, maxResultCount, config)
}

// Commit publishes pending document mutations to the in-memory searchable view.
// It performs no persistence I/O; use semanticpersist.CommitAndSaveService for a
// durable commit.
func (s *Service) Commit(ctx context.Context) error {
	return s.index.Commit(ctx)
}

// Compact compacts committed immutable segments in memory. It performs no
// persistence I/O. Pending mutations must be committed first.
func (s *Service) Compact(ctx context.Context) error {
	return s.index.Compact(ctx)
}

// Statistics returns a snapshot of index counters.
func (s *Service) Statistics() semantic.Statistics {
	return s.index.Statistics()
}

// View returns the current immutable searchable index view.
func (s *Service) View() *semantic.IndexView {
	return s.index.View()
}
