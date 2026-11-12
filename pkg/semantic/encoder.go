package semantic

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

// ChunkEmbedding couples source-chunk metadata with one raw finite embedding.
// Index validates, copies, and performs metric-specific preparation.
type ChunkEmbedding struct {
	Ref       Ref
	Embedding []float32
}

// DocumentEncoder converts source documents into raw chunk embeddings.
// Implementations passed to concurrent Service operations must be safe for
// concurrent use.
type DocumentEncoder interface {
	EncodeDocument(context.Context, fts.Document) ([]ChunkEmbedding, error)
}

// QueryEncoder converts direct text and advanced document queries into raw
// embeddings. Query preparation may differ from document chunking.
type QueryEncoder interface {
	EncodeQuery(context.Context, string, QueryConfig) ([]ChunkEmbedding, error)
	EncodeQueryDocument(context.Context, fts.Document, QueryConfig) ([]ChunkEmbedding, error)
}

// InputLimits bounds text and vector material created by one encoding call.
type InputLimits struct {
	MaxFields         int
	MaxSourceBytes    uint64
	MaxChunks         int
	MaxEmbeddingBytes uint64
}

// ChunkSplitter splits one UTF-8 text into byte ranges. Implementations used
// by concurrent Service operations must be safe for concurrent use.
type ChunkSplitter interface {
	Split(text string) ([]chunk.Range, error)
}

// EncoderConfig configures document and query encoding.
type EncoderConfig struct {
	Limits   InputLimits
	Splitter ChunkSplitter
	Client   embedding.Client
}

// Validate checks the encoder configuration.
func (c EncoderConfig) Validate() error {
	_, err := newEncoder(c.Client, c.Splitter, c.Limits, c.Limits, nil)
	return err
}

// Encoder prepares source documents and queries as semantic vectors.
type Encoder struct {
	splitter       ChunkSplitter
	client         embedding.Client
	documentLimits InputLimits
	queryLimits    InputLimits
	dimensions     int
	documentPrefix string
	queryPrefix    string
}

// NewEncoder constructs one encoder for document and query preparation. A nil
// splitter encodes each field or query as one whole chunk.
func NewEncoder(config EncoderConfig) (*Encoder, error) {
	return newEncoder(config.Client, config.Splitter, config.Limits, config.Limits, nil)
}

func newEncoder(
	client embedding.Client,
	splitter ChunkSplitter,
	documentLimits InputLimits,
	queryLimits InputLimits,
	expectedModel *ModelConfig,
) (*Encoder, error) {
	if client == nil || validateInputLimits(documentLimits) != nil || validateInputLimits(queryLimits) != nil {
		return nil, ErrInvalidConfig
	}
	model := client.Model()
	if err := model.Validate(); err != nil {
		return nil, ErrInvalidConfig
	}
	if expectedModel != nil && (expectedModel.Validate() != nil || newModelConfig(model) != *expectedModel) {
		return nil, ErrInvalidConfig
	}
	return &Encoder{
		splitter:       splitter,
		client:         client,
		documentLimits: documentLimits,
		queryLimits:    queryLimits,
		dimensions:     model.Dimensions,
		documentPrefix: model.DocumentPrefix,
		queryPrefix:    model.QueryPrefix,
	}, nil
}

func validateInputLimits(limits InputLimits) error {
	if limits.MaxFields <= 0 || limits.MaxSourceBytes == 0 || limits.MaxChunks <= 0 ||
		limits.MaxEmbeddingBytes == 0 {
		return ErrInvalidConfig
	}
	return nil
}

var (
	_ DocumentEncoder = (*Encoder)(nil)
	_ QueryEncoder    = (*Encoder)(nil)
)
