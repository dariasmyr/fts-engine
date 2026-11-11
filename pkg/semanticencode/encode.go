// Package semanticencode provides advanced custom document/query encoders.
// Normal model-bound services should use semantic.New. This package does not
// own or mutate a semantic index.
package semanticencode

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

var (
	ErrInvalidConfig          = errors.New("semanticencode: invalid configuration")
	ErrInvalidDocument        = errors.New("semanticencode: invalid document")
	ErrInvalidQuery           = errors.New("semanticencode: invalid query")
	ErrLimitExceeded          = errors.New("semanticencode: limit exceeded")
	ErrInvalidChunk           = errors.New("semanticencode: invalid chunk")
	ErrEmbeddingCountMismatch = errors.New("semanticencode: embedding count does not match chunks")
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

// EncoderConfig configures an advanced document and query encoder. Normal
// model-bound services should use semantic.New instead.
type EncoderConfig struct {
	Limits InputLimits
	// Splitter splits document fields into byte ranges.
	Splitter ChunkSplitter
	Client   embedding.Client
}

func (c EncoderConfig) Validate() error { _, _, err := c.normalized(); return err }

func (c EncoderConfig) normalized() (EncoderConfig, embedding.Model, error) {
	if c.Client == nil || c.Limits.MaxFields <= 0 ||
		c.Limits.MaxSourceBytes == 0 || c.Limits.MaxChunks <= 0 || c.Limits.MaxEmbeddingBytes == 0 {
		return EncoderConfig{}, embedding.Model{}, ErrInvalidConfig
	}
	model := c.Client.Model()
	if model.Validate() != nil {
		return EncoderConfig{}, embedding.Model{}, ErrInvalidConfig
	}
	return c, model, nil
}

// Encoder prepares source documents or queries as semantic vectors.
type Encoder struct {
	splitter       ChunkSplitter
	client         embedding.Client
	limits         InputLimits
	dimensions     int
	documentPrefix string
	queryPrefix    string
}

// NewEncoder constructs one encoder for both document and query preparation.
func NewEncoder(config EncoderConfig) (*Encoder, error) {
	config, model, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return &Encoder{
		splitter: config.Splitter, client: config.Client,
		limits: config.Limits, dimensions: model.Dimensions,
		documentPrefix: model.DocumentPrefix, queryPrefix: model.QueryPrefix,
	}, nil
}

// EncodeDocument prepares a source document for indexing.
func (e *Encoder) EncodeDocument(ctx context.Context, document fts.Document) ([]semantic.ChunkEmbedding, error) {
	return e.encodeDocument(ctx, document, e.documentPrefix)
}

// EncodeQuery prepares a direct text query with the configured splitter. With
// no splitter, the complete query is encoded as one input.
func (e *Encoder) EncodeQuery(
	ctx context.Context,
	query string,
	config semantic.QueryConfig,
) ([]semantic.ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if query == "" || !utf8.ValidString(query) {
		return nil, ErrInvalidQuery
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if uint64(len(query)) > e.limits.MaxSourceBytes {
		return nil, fmt.Errorf("%w: query bytes exceed %d", ErrLimitExceeded, e.limits.MaxSourceBytes)
	}

	ranges, err := e.splitText(query)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidQuery, err)
	}
	if len(ranges) == 0 {
		return nil, ErrInvalidQuery
	}
	if len(ranges) > e.limits.MaxChunks || uint64(len(ranges)-1) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: query chunks exceed %d", ErrLimitExceeded, e.limits.MaxChunks)
	}
	inputs := make([]embeddingInput, len(ranges))
	for i, r := range ranges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := r.Validate(query); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidQuery, err)
		}
		inputs[i] = embeddingInput{
			ref: semantic.Ref{
				ID: semantic.ChunkID(fmt.Sprintf("query-%06d", i)), DocID: "_query",
				Field: fts.DefaultField, Ordinal: uint32(i),
				StartByte: uint64(r.StartByte), EndByte: uint64(r.EndByte),
			},
			text: query[r.StartByte:r.EndByte],
		}
	}
	return e.encodeInputs(ctx, inputs, e.queryPrefix)
}

// EncodeQueryDocument prepares an advanced structured query document.
func (e *Encoder) EncodeQueryDocument(
	ctx context.Context,
	document fts.Document,
	config semantic.QueryConfig,
) ([]semantic.ChunkEmbedding, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return e.encodeDocument(ctx, document, e.queryPrefix)
}

func (e *Encoder) encodeDocument(
	ctx context.Context,
	document fts.Document,
	prefix string,
) ([]semantic.ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	inputs, err := e.chunkDocument(ctx, document)
	if err != nil {
		return nil, err
	}
	return e.encodeInputs(ctx, inputs, prefix)
}

func (e *Encoder) encodeInputs(
	ctx context.Context,
	inputs []embeddingInput,
	prefix string,
) ([]semantic.ChunkEmbedding, error) {
	if err := e.validateEmbeddingSize(len(inputs)); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	texts := make([]string, len(inputs))
	for i, input := range inputs {
		texts[i] = prefix + input.text
	}
	vectors, err := e.client.Embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.makeChunkEmbeddings(ctx, inputs, vectors)
}

var (
	_ semantic.DocumentEncoder = (*Encoder)(nil)
	_ semantic.QueryEncoder    = (*Encoder)(nil)
)

type embeddingInput struct {
	ref  semantic.Ref
	text string
}

func (e *Encoder) splitText(text string) ([]chunk.Range, error) {
	if e.splitter != nil {
		return e.splitter.Split(text)
	}
	whole, err := chunk.Whole(text)
	if err != nil {
		return nil, err
	}
	return []chunk.Range{whole}, nil
}

func (e *Encoder) chunkDocument(ctx context.Context, document fts.Document) ([]embeddingInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if document.ID == "" || len(document.Fields) == 0 {
		return nil, ErrInvalidDocument
	}
	if len(document.Fields) > e.limits.MaxFields {
		return nil, fmt.Errorf("%w: fields %d exceed %d", ErrLimitExceeded, len(document.Fields), e.limits.MaxFields)
	}

	fields := make([]string, 0, len(document.Fields))
	var totalBytes uint64
	for field, value := range document.Fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		text := value.Text
		if text == "" {
			text = value.Value
		}
		if field == "" || text == "" {
			return nil, ErrInvalidDocument
		}
		n := uint64(len(text))
		if n > e.limits.MaxSourceBytes-totalBytes {
			return nil, fmt.Errorf("%w: source bytes exceed %d", ErrLimitExceeded, e.limits.MaxSourceBytes)
		}
		totalBytes += n
		fields = append(fields, field)
	}
	slices.Sort(fields)

	inputs := make([]embeddingInput, 0, min(len(fields), e.limits.MaxChunks))
	for _, field := range fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining := e.limits.MaxChunks - len(inputs)
		if remaining <= 0 {
			return nil, fmt.Errorf("%w: chunks exceed %d", ErrLimitExceeded, e.limits.MaxChunks)
		}
		value := document.Fields[field]
		text := value.Text
		if text == "" {
			text = value.Value
		}
		if !utf8.ValidString(text) {
			return nil, fmt.Errorf("%w: field %q: invalid UTF-8", ErrInvalidChunk, field)
		}

		ranges, err := e.splitText(text)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q: %v", ErrInvalidChunk, field, err)
		}
		if len(ranges) == 0 {
			return nil, fmt.Errorf("%w: field %q produced no chunks", ErrInvalidChunk, field)
		}
		if len(ranges) > remaining || uint64(len(ranges)-1) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: chunks exceed %d or ordinal overflow", ErrLimitExceeded, e.limits.MaxChunks)
		}
		for i, r := range ranges {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := r.Validate(text); err != nil {
				return nil, fmt.Errorf("%w: field %q: %v", ErrInvalidChunk, field, err)
			}
			id := fmt.Sprintf("chunk-%06d", i)
			if e.splitter == nil {
				id = "_whole"
			}
			if field != fts.DefaultField {
				if e.splitter == nil {
					id = "_whole/" + field
				} else {
					id = field + "/" + id
				}
			}
			inputs = append(inputs, embeddingInput{
				ref: semantic.Ref{
					ID: semantic.ChunkID(id), DocID: document.ID, Field: field, Ordinal: uint32(i),
					StartByte: uint64(r.StartByte), EndByte: uint64(r.EndByte),
				},
				text: text[r.StartByte:r.EndByte],
			})
		}
	}
	return inputs, nil
}

func (e *Encoder) validateEmbeddingSize(count int) error {
	dimensions := uint64(e.dimensions)
	if dimensions == 0 || dimensions > math.MaxUint64/4 {
		return fmt.Errorf("%w: invalid embedding dimensions", ErrLimitExceeded)
	}
	bytesPerVector := dimensions * 4
	if uint64(count) > math.MaxUint64/bytesPerVector || uint64(count)*bytesPerVector > e.limits.MaxEmbeddingBytes {
		return fmt.Errorf("%w: embeddings exceed %d bytes", ErrLimitExceeded, e.limits.MaxEmbeddingBytes)
	}
	return nil
}

func (e *Encoder) makeChunkEmbeddings(ctx context.Context, inputs []embeddingInput, vectors [][]float32) ([]semantic.ChunkEmbedding, error) {
	if len(inputs) != len(vectors) {
		return nil, fmt.Errorf("%w: got %d vectors for %d chunks", ErrEmbeddingCountMismatch, len(vectors), len(inputs))
	}
	result := make([]semantic.ChunkEmbedding, len(inputs))
	for i, input := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result[i] = semantic.ChunkEmbedding{Ref: input.ref, Embedding: vectors[i]}
	}
	return result, nil
}
