package encoding

import (
	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
)

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

// EncoderConfig configures document and query encoding independently.
type EncoderConfig struct {
	Client         embedding.Client
	Splitter       ChunkSplitter
	DocumentLimits InputLimits
	QueryLimits    InputLimits
}

// Validate checks the encoder configuration.
func (c EncoderConfig) Validate() error {
	_, err := NewEncoder(c)
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
	if config.Client == nil || validateInputLimits(config.DocumentLimits) != nil ||
		validateInputLimits(config.QueryLimits) != nil {
		return nil, ErrInvalidConfig
	}
	model := config.Client.Model()
	if err := model.Validate(); err != nil {
		return nil, ErrInvalidConfig
	}
	return &Encoder{
		splitter:       config.Splitter,
		client:         config.Client,
		documentLimits: config.DocumentLimits,
		queryLimits:    config.QueryLimits,
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
