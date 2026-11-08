package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Service provides document-level semantic indexing and text search. New and
// NewFromIndex own model-bound document/query preparation; NewService accepts
// custom encoders. Index remains independent from document encoding.
type Service struct {
	index                *Index
	documentEncoder      DocumentEncoder
	queryEncoder         QueryEncoder
	retrievalFingerprint string
}

type retrievalDescriber interface {
	RetrievalFingerprint() string
}

// New constructs the high-level semantic service from one
// model-bound embedding client. Model metadata, role prefixes, schemas, and
// vector-space configuration are derived internally.
func New(client embedding.Client, options ...Option) (*Service, error) {
	components, err := prepareModelService(client, options)
	if err != nil {
		return nil, err
	}
	index, err := NewIndex(components.indexConfig)
	if err != nil {
		return nil, err
	}
	return NewService(index, components.documents, components.queries)
}

// NewFromIndex binds a model-derived document/query pipeline to an existing
// compatible index, including one reconstructed by Restore.
func NewFromIndex(
	client embedding.Client,
	index *Index,
	options ...Option,
) (*Service, error) {
	if index == nil {
		return nil, ErrInvalidConfig
	}
	components, err := prepareModelService(client, options)
	if err != nil {
		return nil, err
	}
	return NewService(index, components.documents, components.queries)
}

// NewService binds custom document and query encoders to an existing index.
// Both encoder schemas must match the index schema. When either encoder exposes
// a retrieval fingerprint, both must expose the same non-empty fingerprint.
func NewService(
	index *Index,
	documentEncoder DocumentEncoder,
	queryEncoder QueryEncoder,
) (*Service, error) {
	if index == nil || documentEncoder == nil || queryEncoder == nil {
		return nil, ErrInvalidConfig
	}
	if err := validateSchemaCompatibility(documentEncoder.Schema(), index.Schema()); err != nil {
		return nil, err
	}
	if err := validateSchemaCompatibility(queryEncoder.Schema(), index.Schema()); err != nil {
		return nil, err
	}
	retrievalFingerprint := ""
	documentRetrieval, documentOK := documentEncoder.(retrievalDescriber)
	queryRetrieval, queryOK := queryEncoder.(retrievalDescriber)
	if documentOK != queryOK {
		return nil, ErrEmbeddingMismatch
	}
	if documentOK {
		retrievalFingerprint = documentRetrieval.RetrievalFingerprint()
		if retrievalFingerprint == "" || retrievalFingerprint != queryRetrieval.RetrievalFingerprint() {
			return nil, ErrEmbeddingMismatch
		}
	}
	return &Service{
		index:                index,
		documentEncoder:      documentEncoder,
		queryEncoder:         queryEncoder,
		retrievalFingerprint: retrievalFingerprint,
	}, nil
}

// RetrievalFingerprint reports the versioned document/query compatibility
// contract when provided by the configured encoders.
func (s *Service) RetrievalFingerprint() string { return s.retrievalFingerprint }

// Index returns the underlying semantic index for operations
// that work directly with prepared embeddings.
func (s *Service) Index() *Index {
	return s.index
}

// Schema returns the semantic compatibility schema.
func (s *Service) Schema() Schema {
	return s.index.Schema()
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

	queries, err := s.queryEncoder.EncodeQueryDocument(ctx, query)
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

// Search embeds one text query and searches the committed semantic view.
func (s *Service) Search(
	ctx context.Context,
	query string,
	maxResultCount int,
) (DocumentSearchResult, error) {
	return s.SearchWithOptions(ctx, query, maxResultCount, SearchOptions{})
}

// SearchWithOptions searches one text query with explicit ANN and grouping
// options. Query input handling is configured when the service is constructed.
func (s *Service) SearchWithOptions(
	ctx context.Context,
	query string,
	maxResultCount int,
	options SearchOptions,
) (DocumentSearchResult, error) {
	if ctx == nil {
		return DocumentSearchResult{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return DocumentSearchResult{}, err
	}
	queries, err := s.queryEncoder.EncodeQuery(ctx, query)
	if err != nil {
		return DocumentSearchResult{}, err
	}
	return s.index.Search(ctx, queries, maxResultCount, options)
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
