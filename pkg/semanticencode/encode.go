// Package semanticencode converts source documents into prepared semantic
// vectors. It does not own or mutate a semantic index.
package semanticencode

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

var (
	ErrInvalidConfig          = errors.New("semanticencode: invalid configuration")
	ErrInvalidDocument        = errors.New("semanticencode: invalid document")
	ErrLimitExceeded          = errors.New("semanticencode: limit exceeded")
	ErrInvalidChunk           = errors.New("semanticencode: invalid chunk")
	ErrDuplicateChunkID       = errors.New("semanticencode: duplicate chunk ID")
	ErrEmbeddingCountMismatch = errors.New("semanticencode: embedding count does not match chunks")
)

// Chunker splits one document field into deterministic source chunks. A nil
// Chunker makes the encoder create one whole-field chunk.
type Chunker interface {
	Split(ctx context.Context, text string) ([]chunk.Range, error)
}

// EmbeddingProvider converts chunks into embeddings in the same order as the
// input slice. The provider owns model-specific preprocessing and batching.
// It must not mutate or retain the input slice. It transfers ownership of the
// returned slices to the caller and must not mutate or reuse them after
// returning. Implementations used by concurrent Encode calls must be safe for
// concurrent use. DocumentEncoder validates and copies every returned vector.
type EmbeddingProvider interface {
	Embed(context.Context, []chunk.Chunk) ([][]float32, error)
}

// Limits bounds work and allocations performed by one Encode call. Every field
// is mandatory and must be positive.
type Limits struct {
	MaxFields         int
	MaxSourceBytes    uint64
	MaxChunks         int
	MaxEmbeddingBytes uint64
}

// Config defines an encoder's immutable schema, dependencies, and per-call
// resource limits.
type Config struct {
	Schema   semantic.Schema
	Limits   Limits
	Chunker  Chunker
	Embedder EmbeddingProvider
}

// Validate checks that Config can construct an encoder.
func (c Config) Validate() error {
	_, err := c.calculator()
	return err
}

func (c Config) calculator() (vector.Calculator, error) {
	if c.Embedder == nil || !c.Schema.IsValid() ||
		c.Limits.MaxFields <= 0 || c.Limits.MaxSourceBytes == 0 ||
		c.Limits.MaxChunks <= 0 || c.Limits.MaxEmbeddingBytes == 0 {
		return vector.Calculator{}, ErrInvalidConfig
	}
	calculator, err := c.Schema.Embedding.Calculator()
	if err != nil {
		return vector.Calculator{}, fmt.Errorf("%w: embedding schema: %v", ErrInvalidConfig, err)
	}
	return calculator, nil
}

// DocumentEncoder owns document-level chunking and embedding orchestration.
// It does not mutate internal state and is safe for concurrent use when its
// configured Chunker and EmbeddingProvider are safe for concurrent use.
type DocumentEncoder struct {
	chunker    Chunker
	embedder   EmbeddingProvider
	schema     semantic.Schema
	limits     Limits
	calculator vector.Calculator
}

func New(config Config) (*DocumentEncoder, error) {
	calculator, err := config.calculator()
	if err != nil {
		return nil, err
	}
	return &DocumentEncoder{
		chunker: config.Chunker, embedder: config.Embedder, schema: config.Schema,
		limits: config.Limits, calculator: calculator,
	}, nil
}

// Descriptor returns the immutable model and chunking identity used by Encode.
func (e *DocumentEncoder) Descriptor() semantic.Schema { return e.schema }

// Encode validates, chunks, and embeds a complete document. All configured
// limits and all chunk source references are checked before Embed is called.
// Returned vector buffers are owned by the caller and do not alias provider
// buffers.
func (e *DocumentEncoder) Encode(ctx context.Context, document fts.Document) ([]semantic.EncodedChunk, error) {
	chunks, err := e.chunkDocument(ctx, document)
	if err != nil {
		return nil, err
	}
	if err := e.validateEmbeddingSize(len(chunks)); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vectors, err := e.embedder.Embed(ctx, chunks)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.makeChunkVectors(ctx, chunks, vectors)
}

func (e *DocumentEncoder) chunkDocument(ctx context.Context, document fts.Document) ([]chunk.Chunk, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if document.ID == "" || len(document.Fields) == 0 {
		return nil, ErrInvalidDocument
	}
	if len(document.Fields) > e.limits.MaxFields {
		return nil, fmt.Errorf(
			"%w: fields %d exceed %d",
			ErrLimitExceeded,
			len(document.Fields),
			e.limits.MaxFields,
		)
	}

	fields := make([]string, 0, len(document.Fields))
	var documentBytes uint64
	for field, value := range document.Fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if field == "" || value.Text == "" {
			return nil, ErrInvalidDocument
		}
		fieldBytes := uint64(len(value.Text))
		if fieldBytes > e.limits.MaxSourceBytes-documentBytes {
			return nil, fmt.Errorf("%w: source bytes exceed %d", ErrLimitExceeded, e.limits.MaxSourceBytes)
		}
		documentBytes += fieldBytes
		fields = append(fields, field)
	}
	slices.Sort(fields)

	chunks := make([]chunk.Chunk, 0, min(len(fields), e.limits.MaxChunks))

	for _, field := range fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remainingChunks := e.limits.MaxChunks - len(chunks)
		if remainingChunks <= 0 {
			return nil, fmt.Errorf("%w: chunks exceed %d", ErrLimitExceeded, e.limits.MaxChunks)
		}

		text := document.Fields[field].Text

		var ranges []chunk.Range
		var err error
		if e.chunker == nil {
			whole, err := chunk.Whole(document.ID, field, text)
			if err != nil {
				return nil, err
			}
			ranges = []chunk.Range{
				{
					StartByte: int(whole.Ref.StartByte),
					EndByte:   int(whole.Ref.EndByte),
				},
			}
		}

		ranges, err = e.chunker.Split(ctx, text)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q: %v", ErrInvalidChunk, field, err)
		}

		if len(ranges) == 0 {
			return nil, fmt.Errorf(
				"%w: field %q produced no chunks",
				ErrInvalidChunk,
				field,
			)
		}

		if len(ranges) > remainingChunks {
			return nil, fmt.Errorf(
				"%w: chunks exceed %d",
				ErrLimitExceeded,
				e.limits.MaxChunks,
			)
		}

		fieldChunks, err := buildChunks(
			ctx,
			document.ID,
			field,
			text,
			ranges,
		)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field, err)
		}

		chunks = append(chunks, fieldChunks...)
	}

	return chunks, nil
}

func buildChunks(
	ctx context.Context,
	docID fts.DocID,
	field, text string,
	ranges []chunk.Range,
) ([]chunk.Chunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !utf8.ValidString(text) {
		return nil, fmt.Errorf(
			"%w: invalid UTF-8 string",
			ErrInvalidChunk,
		)
	}

	if uint64(len(ranges)-1) > math.MaxUint32 {
		return nil, fmt.Errorf(
			"%w: chunk ordinal overflow",
			ErrLimitExceeded,
		)
	}

	chunks := make([]chunk.Chunk, 0, len(ranges))

	for i, r := range ranges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if r.StartByte < 0 ||
			r.EndByte > len(text) ||
			r.StartByte >= r.EndByte {
			return nil, fmt.Errorf(
				"%w: invalid range [%d,%d)",
				ErrInvalidChunk,
				r.StartByte,
				r.EndByte,
			)
		}

		if !utf8.RuneStart(text[r.StartByte]) ||
			(r.EndByte < len(text) && !utf8.RuneStart(text[r.EndByte])) {
			return nil, fmt.Errorf(
				"%w: range [%d,%d) splits UTF-8 rune",
				ErrInvalidChunk,
				r.StartByte,
				r.EndByte,
			)
		}

		id := chunk.ID(fmt.Sprintf("chunk-%06d", i))
		if field != fts.DefaultField {
			id = chunk.ID(fmt.Sprintf("%s/chunk-%06d", field, i))
		}

		chunks = append(chunks, chunk.Chunk{
			Ref: chunk.Ref{
				ID:        id,
				DocID:     docID,
				Field:     field,
				Ordinal:   uint32(i),
				StartByte: uint64(r.StartByte),
				EndByte:   uint64(r.EndByte),
			},
			Text: text[r.StartByte:r.EndByte],
		})
	}

	return chunks, nil
}

func (e *DocumentEncoder) validateEmbeddingSize(chunks int) error {
	dimensions := uint64(e.calculator.Dimensions())
	if dimensions > math.MaxUint64/4 {
		return fmt.Errorf("%w: embedding dimensions overflow byte size", ErrLimitExceeded)
	}
	bytesPerVector := dimensions * 4
	if uint64(chunks) > math.MaxUint64/bytesPerVector ||
		uint64(chunks)*bytesPerVector > e.limits.MaxEmbeddingBytes {
		return fmt.Errorf("%w: embeddings exceed %d bytes", ErrLimitExceeded, e.limits.MaxEmbeddingBytes)
	}
	return nil
}

func (e *DocumentEncoder) makeChunkVectors(ctx context.Context, chunks []chunk.Chunk, vectors [][]float32) ([]semantic.EncodedChunk, error) {
	if len(chunks) != len(vectors) {
		return nil, fmt.Errorf("%w: got %d vectors for %d chunks", ErrEmbeddingCountMismatch, len(vectors), len(chunks))
	}
	result := make([]semantic.EncodedChunk, len(chunks))
	for i, item := range chunks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		prepared, err := e.calculator.Prepare(vectors[i])
		if err != nil {
			return nil, fmt.Errorf("embedding %d: %w", i, err)
		}
		result[i] = semantic.EncodedChunk{Ref: item.Ref, Vector: prepared}
	}
	return result, nil
}
