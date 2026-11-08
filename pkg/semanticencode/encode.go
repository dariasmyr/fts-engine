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

// InputRole selects the model prefix used by a derived encoder.
type InputRole uint8

const (
	RoleUnspecified InputRole = iota
	RoleDocument
	RoleQuery
)

// InputLimits bounds text and vector material created by one encoding call.
type InputLimits struct {
	MaxFields         int
	MaxSourceBytes    uint64
	MaxChunks         int
	MaxEmbeddingBytes uint64
}

// EncoderConfig configures one advanced document or query encoder. Normal
// model-bound services should use semantic.New instead.
type EncoderConfig struct {
	Schema semantic.Schema
	Metric vector.Metric
	Role   InputRole
	Limits InputLimits
	// Chunker splits document fields into byte ranges. Custom implementations
	// require an explicit Schema.
	Chunker semantic.Chunker
	Client  embedding.Client
}

func (c EncoderConfig) Validate() error { _, _, _, err := c.normalized(); return err }

func (c EncoderConfig) normalized() (EncoderConfig, int, string, error) {
	if c.Client == nil || c.Limits.MaxFields <= 0 ||
		c.Limits.MaxSourceBytes == 0 || c.Limits.MaxChunks <= 0 || c.Limits.MaxEmbeddingBytes == 0 {
		return EncoderConfig{}, 0, "", ErrInvalidConfig
	}
	model := c.Client.Model()
	if model.Validate() != nil {
		return EncoderConfig{}, 0, "", ErrInvalidConfig
	}
	derived := c.Schema == (semantic.Schema{})
	if derived {
		chunking, err := deriveChunkingSchema(c.Chunker)
		if err != nil {
			return EncoderConfig{}, 0, "", err
		}
		schema, err := semantic.DeriveSchema(model, chunking, c.Metric)
		if err != nil {
			return EncoderConfig{}, 0, "", fmt.Errorf("%w: derive schema: %v", ErrInvalidConfig, err)
		}
		c.Schema = schema
	}
	if !c.Schema.IsValid() || model.ID != c.Schema.Embedding.ModelID ||
		model.Revision != c.Schema.Embedding.ModelRevision ||
		model.Dimensions != c.Schema.Embedding.Dimensions {
		return EncoderConfig{}, 0, "", ErrInvalidConfig
	}
	space, err := c.Schema.Embedding.VectorSpace()
	if err != nil {
		return EncoderConfig{}, 0, "", fmt.Errorf("%w: embedding schema: %v", ErrInvalidConfig, err)
	}
	prefix := ""
	if c.Role != RoleUnspecified || derived {
		if c.Role == RoleUnspecified {
			c.Role = RoleDocument
		}
		switch c.Role {
		case RoleDocument:
			prefix = model.DocumentPrefix
		case RoleQuery:
			prefix = model.QueryPrefix
		default:
			return EncoderConfig{}, 0, "", ErrInvalidConfig
		}
	}
	return c, space.Dimensions(), prefix, nil
}

func deriveChunkingSchema(chunker semantic.Chunker) (semantic.ChunkingSchema, error) {
	if chunker == nil {
		return semantic.ChunkingSchema{
			ID: chunk.WholeID, Version: chunk.WholeVersion, Fingerprint: chunk.WholeFingerprint(),
		}, nil
	}
	splitter, ok := chunker.(*chunk.Splitter)
	if !ok {
		return semantic.ChunkingSchema{}, ErrInvalidConfig
	}
	config := splitter.Config()
	fingerprint, err := config.Fingerprint()
	if err != nil {
		return semantic.ChunkingSchema{}, fmt.Errorf("%w: chunking config: %v", ErrInvalidConfig, err)
	}
	return semantic.ChunkingSchema{
		ID: chunk.SplitterID, Version: chunk.SplitterVersion, Fingerprint: fingerprint,
	}, nil
}

// Encoder prepares source documents or queries as semantic vectors.
type Encoder struct {
	chunker              semantic.Chunker
	client               embedding.Client
	schema               semantic.Schema
	limits               InputLimits
	dimensions           int
	prefix               string
	retrievalFingerprint string
}

// NewEncoder constructs one role-specific encoder.
func NewEncoder(config EncoderConfig) (*Encoder, error) {
	config, dimensions, prefix, err := config.normalized()
	if err != nil {
		return nil, err
	}
	retrievalFingerprint, err := semantic.DeriveRetrievalFingerprint(config.Schema, config.Client.Model())
	if err != nil {
		return nil, fmt.Errorf("%w: derive retrieval compatibility: %v", ErrInvalidConfig, err)
	}
	return &Encoder{
		chunker: config.Chunker, client: config.Client,
		schema: config.Schema, limits: config.Limits, dimensions: dimensions,
		prefix: prefix, retrievalFingerprint: retrievalFingerprint,
	}, nil
}

func (e *Encoder) Schema() semantic.Schema { return e.schema }

// RetrievalFingerprint reports the versioned document/query contract used for
// evaluation. It is not persisted as document compatibility.
func (e *Encoder) RetrievalFingerprint() string { return e.retrievalFingerprint }

// EncodeDocument prepares a source document for indexing.
func (e *Encoder) EncodeDocument(ctx context.Context, document fts.Document) ([]semantic.ChunkEmbedding, error) {
	return e.encodeDocument(ctx, document)
}

// EncodeQuery prepares a direct text query as one embedding input. Normal
// model-bound services should use semantic.New for explicit long-query policy.
func (e *Encoder) EncodeQuery(ctx context.Context, query string) ([]semantic.ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if query == "" || !utf8.ValidString(query) {
		return nil, ErrInvalidQuery
	}
	if uint64(len(query)) > e.limits.MaxSourceBytes {
		return nil, fmt.Errorf("%w: query bytes exceed %d", ErrLimitExceeded, e.limits.MaxSourceBytes)
	}
	return e.encodeInputs(ctx, []embeddingInput{{
		ref: semantic.Ref{
			ID:      "query-000000",
			DocID:   "_query",
			Field:   fts.DefaultField,
			EndByte: uint64(len(query)),
		},
		text: query,
	}})
}

// EncodeQueryDocument prepares an advanced structured query document.
func (e *Encoder) EncodeQueryDocument(
	ctx context.Context,
	document fts.Document,
) ([]semantic.ChunkEmbedding, error) {
	return e.encodeDocument(ctx, document)
}

func (e *Encoder) encodeDocument(ctx context.Context, document fts.Document) ([]semantic.ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	inputs, err := e.chunkDocument(ctx, document)
	if err != nil {
		return nil, err
	}
	return e.encodeInputs(ctx, inputs)
}

func (e *Encoder) encodeInputs(ctx context.Context, inputs []embeddingInput) ([]semantic.ChunkEmbedding, error) {
	if err := e.validateEmbeddingSize(len(inputs)); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	texts := make([]string, len(inputs))
	for i, input := range inputs {
		texts[i] = e.prefix + input.text
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

		var ranges []chunk.Range
		if e.chunker == nil {
			whole, err := chunk.Whole(text)
			if err != nil {
				return nil, fmt.Errorf("%w: field %q: %v", ErrInvalidChunk, field, err)
			}
			ranges = []chunk.Range{whole}
		} else {
			var err error
			ranges, err = e.chunker.Split(text)
			if err != nil {
				return nil, fmt.Errorf("%w: field %q: %v", ErrInvalidChunk, field, err)
			}
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
			if e.chunker == nil {
				id = "_whole"
			}
			if field != fts.DefaultField {
				if e.chunker == nil {
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
