// Package semanticencode prepares source documents as semantic vectors.
// It does not own or mutate a semantic index.
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
	ErrEmbeddingCountMismatch = errors.New("semanticencode: embedding count does not match chunks")
)

// Chunker defines the byte-range splitting behavior required by the encoder.
type Chunker interface {
	Split(text string) ([]chunk.Range, error)
}

// EmbeddingInput is transient provider input. Only Ref is persisted.
// Text is a view into the document source; providers must not retain it.
type EmbeddingInput struct {
	Ref  semantic.Ref
	Text string
}

// EmbeddingProvider returns one embedding per input, preserving order.
// Implementations must not retain or mutate inputs and must transfer ownership
// of returned buffers. Concurrent Encode calls require concurrent-safe providers.
type EmbeddingProvider interface {
	Embed(context.Context, []EmbeddingInput) ([][]float32, error)
}

type Limits struct {
	MaxFields         int
	MaxSourceBytes    uint64
	MaxChunks         int
	MaxEmbeddingBytes uint64
}

type Config struct {
	Schema   semantic.Schema
	Limits   Limits
	Chunker  Chunker
	Embedder EmbeddingProvider
}

func (c Config) Validate() error { _, err := c.calculator(); return err }

func (c Config) calculator() (vector.Calculator, error) {
	if c.Embedder == nil || !c.Schema.IsValid() || c.Limits.MaxFields <= 0 ||
		c.Limits.MaxSourceBytes == 0 || c.Limits.MaxChunks <= 0 || c.Limits.MaxEmbeddingBytes == 0 {
		return vector.Calculator{}, ErrInvalidConfig
	}
	calculator, err := c.Schema.Embedding.Calculator()
	if err != nil {
		return vector.Calculator{}, fmt.Errorf("%w: embedding schema: %v", ErrInvalidConfig, err)
	}
	return calculator, nil
}

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
		chunker: config.Chunker, embedder: config.Embedder,
		schema: config.Schema, limits: config.Limits, calculator: calculator,
	}, nil
}

func (e *DocumentEncoder) Descriptor() semantic.Schema { return e.schema }

func (e *DocumentEncoder) Encode(ctx context.Context, document fts.Document) ([]semantic.EncodedChunk, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	inputs, err := e.chunkDocument(ctx, document)
	if err != nil {
		return nil, err
	}
	if err := e.validateEmbeddingSize(len(inputs)); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vectors, err := e.embedder.Embed(ctx, inputs)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return e.makeChunkVectors(ctx, inputs, vectors)
}

func (e *DocumentEncoder) chunkDocument(ctx context.Context, document fts.Document) ([]EmbeddingInput, error) {
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
		if field == "" || value.Text == "" {
			return nil, ErrInvalidDocument
		}
		n := uint64(len(value.Text))
		if n > e.limits.MaxSourceBytes-totalBytes {
			return nil, fmt.Errorf("%w: source bytes exceed %d", ErrLimitExceeded, e.limits.MaxSourceBytes)
		}
		totalBytes += n
		fields = append(fields, field)
	}
	slices.Sort(fields)

	inputs := make([]EmbeddingInput, 0, min(len(fields), e.limits.MaxChunks))
	for _, field := range fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining := e.limits.MaxChunks - len(inputs)
		if remaining <= 0 {
			return nil, fmt.Errorf("%w: chunks exceed %d", ErrLimitExceeded, e.limits.MaxChunks)
		}
		text := document.Fields[field].Text
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
			inputs = append(inputs, EmbeddingInput{
				Ref: semantic.Ref{
					ID: semantic.ChunkID(id), DocID: document.ID, Field: field, Ordinal: uint32(i),
					StartByte: uint64(r.StartByte), EndByte: uint64(r.EndByte),
				},
				Text: text[r.StartByte:r.EndByte],
			})
		}
	}
	return inputs, nil
}

func (e *DocumentEncoder) validateEmbeddingSize(count int) error {
	dimensions := uint64(e.calculator.Dimensions())
	if dimensions == 0 || dimensions > math.MaxUint64/4 {
		return fmt.Errorf("%w: invalid embedding dimensions", ErrLimitExceeded)
	}
	bytesPerVector := dimensions * 4
	if uint64(count) > math.MaxUint64/bytesPerVector || uint64(count)*bytesPerVector > e.limits.MaxEmbeddingBytes {
		return fmt.Errorf("%w: embeddings exceed %d bytes", ErrLimitExceeded, e.limits.MaxEmbeddingBytes)
	}
	return nil
}

func (e *DocumentEncoder) makeChunkVectors(ctx context.Context, inputs []EmbeddingInput, vectors [][]float32) ([]semantic.EncodedChunk, error) {
	if len(inputs) != len(vectors) {
		return nil, fmt.Errorf("%w: got %d vectors for %d chunks", ErrEmbeddingCountMismatch, len(vectors), len(inputs))
	}
	result := make([]semantic.EncodedChunk, len(inputs))
	for i, input := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		prepared, err := e.calculator.Prepare(vectors[i])
		if err != nil {
			return nil, fmt.Errorf("embedding %d: %w", i, err)
		}
		result[i] = semantic.EncodedChunk{Ref: input.Ref, Vector: prepared}
	}
	return result, nil
}
