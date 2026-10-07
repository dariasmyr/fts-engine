package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Service is the document-oriented facade over Index. It owns no indexing state;
// Encoder remains request-provided for backwards compatibility with the package's
// original API. New code that already has embeddings can use Index directly.
type Service struct {
	index *Index
}

func New(config Config) (*Service, error) {
	index, err := NewIndex(config)
	if err != nil {
		return nil, err
	}
	return &Service{index: index}, nil
}

func (s *Service) Index() *Index                  { return s.index }
func (s *Service) Embedding() EmbeddingDescriptor { return s.index.Embedding() }
func (s *Service) Chunking() ChunkingDescriptor   { return s.index.Chunking() }

func (s *Service) validateEncoder(encoder Encoder) error {
	if encoder == nil {
		return ErrInvalidConfig
	}
	return validateSchemaCompatibility(encoder.Descriptor(), s.index.Schema())
}

func (s *Service) AddDocument(ctx context.Context, encoder Encoder, document fts.Document) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := s.validateEncoder(encoder); err != nil {
		return err
	}
	batch, err := encoder.Encode(ctx, document)
	if err != nil {
		return err
	}
	return s.index.Add(ctx, document.ID, batch)
}

func (s *Service) ReplaceDocument(ctx context.Context, encoder Encoder, document fts.Document) error {
	if ctx == nil {
		return vector.ErrNilContext
	}
	if err := s.validateEncoder(encoder); err != nil {
		return err
	}
	batch, err := encoder.Encode(ctx, document)
	if err != nil {
		return err
	}
	return s.index.Replace(ctx, document.ID, batch)
}

func (s *Service) DeleteDocument(ctx context.Context, docID fts.DocID) error {
	return s.index.Delete(ctx, docID)
}

func (s *Service) Flush(ctx context.Context) error   { return s.index.Flush(ctx) }
func (s *Service) Compact(ctx context.Context) error { return s.index.Compact(ctx) }
func (s *Service) Statistics() Statistics            { return s.index.Statistics() }

func (s *Service) Snapshot() *Snapshot { return s.index.Snapshot() }
