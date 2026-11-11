package semantic

import (
	"context"
	"fmt"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type modelInputLimits struct {
	maxFields         int
	maxSourceBytes    uint64
	maxChunks         int
	maxEmbeddingBytes uint64
}

type modelInputRole uint8

const (
	modelInputDocument modelInputRole = iota + 1
	modelInputQuery
)

type modelEncoder struct {
	role       modelInputRole
	splitter   *chunk.Splitter
	client     embedding.Client
	limits     modelInputLimits
	dimensions int
	prefix     string
}

func newModelEncoder(
	client embedding.Client,
	role modelInputRole,
	splitter *chunk.Splitter,
	modelConfig ModelConfig,
	limits modelInputLimits,
) (*modelEncoder, error) {
	if client == nil || modelConfig.Validate() != nil {
		return nil, ErrInvalidConfig
	}
	model := client.Model()
	if err := model.Validate(); err != nil || newModelConfig(model) != modelConfig {
		return nil, ErrInvalidConfig
	}
	prefix := model.DocumentPrefix
	if role == modelInputQuery {
		prefix = model.QueryPrefix
	} else if role != modelInputDocument {
		return nil, ErrInvalidConfig
	}
	return &modelEncoder{
		role: role, splitter: splitter, client: client,
		limits: limits, dimensions: modelConfig.Dimensions,
		prefix: prefix,
	}, nil
}

func (e *modelEncoder) EncodeDocument(ctx context.Context, document fts.Document) ([]ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if e.role != modelInputDocument {
		return nil, ErrInvalidDocument
	}
	return e.encodeDocument(ctx, document)
}

func (e *modelEncoder) EncodeQueryDocument(ctx context.Context, document fts.Document, config QueryConfig) ([]ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if e.role != modelInputQuery {
		return nil, ErrInvalidQuery
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return e.encodeDocument(ctx, document)
}

func (e *modelEncoder) encodeDocument(ctx context.Context, document fts.Document) ([]ChunkEmbedding, error) {
	inputs, err := e.documentInputs(ctx, document)
	if err != nil {
		return nil, err
	}
	return e.embedInputs(ctx, inputs)
}

func (e *modelEncoder) EncodeQuery(ctx context.Context, text string, config QueryConfig) ([]ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if e.role != modelInputQuery || text == "" || !utf8.ValidString(text) {
		return nil, ErrInvalidQuery
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	inputs, err := e.queryInputs(ctx, text)
	if err != nil {
		return nil, err
	}
	return e.embedInputs(ctx, inputs)
}

type modelInput struct {
	ref  Ref
	text string
}

func (e *modelEncoder) documentInputs(ctx context.Context, document fts.Document) ([]modelInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if document.ID == "" || len(document.Fields) == 0 {
		return nil, ErrInvalidDocument
	}
	if len(document.Fields) > e.limits.maxFields {
		return nil, fmt.Errorf("%w: fields %d exceed %d", ErrInputLimitExceeded, len(document.Fields), e.limits.maxFields)
	}
	fields := make([]string, 0, len(document.Fields))
	var totalBytes uint64
	for field, value := range document.Fields {
		text := value.Text
		if text == "" {
			text = value.Value
		}
		if field == "" || text == "" || !utf8.ValidString(text) {
			return nil, ErrInvalidDocument
		}
		if uint64(len(text)) > e.limits.maxSourceBytes-totalBytes {
			return nil, fmt.Errorf("%w: source bytes exceed %d", ErrInputLimitExceeded, e.limits.maxSourceBytes)
		}
		totalBytes += uint64(len(text))
		fields = append(fields, field)
	}
	slices.Sort(fields)

	inputs := make([]modelInput, 0, min(len(fields), e.limits.maxChunks))
	for _, field := range fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining := e.limits.maxChunks - len(inputs)
		if remaining <= 0 {
			return nil, fmt.Errorf("%w: chunks exceed %d", ErrInputLimitExceeded, e.limits.maxChunks)
		}
		value := document.Fields[field]
		text := value.Text
		if text == "" {
			text = value.Value
		}
		ranges, err := e.splitter.Split(text)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q: %w", ErrInvalidDocument, field, err)
		}
		if len(ranges) == 0 {
			return nil, fmt.Errorf("%w: field %q produced no chunks", ErrInvalidDocument, field)
		}
		if len(ranges) > remaining || uint64(len(ranges)-1) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: chunks exceed %d", ErrInputLimitExceeded, e.limits.maxChunks)
		}
		for i, byteRange := range ranges {
			if err := byteRange.Validate(text); err != nil {
				return nil, fmt.Errorf("%w: field %q: %w", ErrInvalidDocument, field, err)
			}
			chunkID := fmt.Sprintf("chunk-%06d", i)
			if field != fts.DefaultField {
				chunkID = field + "/" + chunkID
			}
			inputs = append(inputs, modelInput{
				ref: Ref{
					ID: ChunkID(chunkID), DocID: document.ID, Field: field, Ordinal: uint32(i),
					StartByte: uint64(byteRange.StartByte), EndByte: uint64(byteRange.EndByte),
				},
				text: text[byteRange.StartByte:byteRange.EndByte],
			})
		}
	}
	return inputs, nil
}

func (e *modelEncoder) queryInputs(ctx context.Context, text string) ([]modelInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if uint64(len(text)) > e.limits.maxSourceBytes {
		return nil, fmt.Errorf("%w: query bytes exceed %d", ErrInputLimitExceeded, e.limits.maxSourceBytes)
	}
	ranges, err := e.splitter.Split(text)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
	}
	if len(ranges) == 0 {
		return nil, ErrInvalidQuery
	}
	if len(ranges) > e.limits.maxChunks || uint64(len(ranges)-1) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: query chunks exceed %d", ErrInputLimitExceeded, e.limits.maxChunks)
	}

	inputs := make([]modelInput, len(ranges))
	for i, byteRange := range ranges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := byteRange.Validate(text); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
		}
		inputs[i] = queryInput(text, byteRange.StartByte, byteRange.EndByte, i)
	}
	return inputs, nil
}

func queryInput(text string, start, end, ordinal int) modelInput {
	return modelInput{
		ref: Ref{
			ID: ChunkID(fmt.Sprintf("query-%06d", ordinal)), DocID: "_query",
			Field: fts.DefaultField, Ordinal: uint32(ordinal),
			StartByte: uint64(start), EndByte: uint64(end),
		},
		text: text[start:end],
	}
}

func (e *modelEncoder) embedInputs(ctx context.Context, inputs []modelInput) ([]ChunkEmbedding, error) {
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
	if len(vectors) != len(inputs) {
		return nil, fmt.Errorf("%w: got %d vectors for %d inputs", ErrEmbeddingCountMismatch, len(vectors), len(inputs))
	}
	result := make([]ChunkEmbedding, len(inputs))
	for i, input := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result[i] = ChunkEmbedding{Ref: input.ref, Embedding: vectors[i]}
	}
	return result, nil
}

func (e *modelEncoder) validateEmbeddingSize(count int) error {
	bytesPerVector := uint64(e.dimensions) * 4
	if uint64(count) > ^uint64(0)/bytesPerVector || uint64(count)*bytesPerVector > e.limits.maxEmbeddingBytes {
		return fmt.Errorf("%w: embeddings exceed %d bytes", ErrInputLimitExceeded, e.limits.maxEmbeddingBytes)
	}
	return nil
}

var (
	_ DocumentEncoder = (*modelEncoder)(nil)
	_ QueryEncoder    = (*modelEncoder)(nil)
)
