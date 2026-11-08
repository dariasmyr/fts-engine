package semantic

import (
	"context"
	"fmt"
	"math"
	"slices"
	"unicode/utf8"

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
	role                 modelInputRole
	chunker              Chunker
	client               embedding.Client
	schema               Schema
	limits               modelInputLimits
	dimensions           int
	prefix               string
	queryConfig          QueryConfig
	retrievalFingerprint string
}

func newModelEncoder(
	client embedding.Client,
	role modelInputRole,
	chunker Chunker,
	schema Schema,
	limits modelInputLimits,
	queryConfig QueryConfig,
) (*modelEncoder, error) {
	if client == nil || !schema.IsValid() {
		return nil, ErrInvalidConfig
	}
	model := client.Model()
	if err := model.Validate(); err != nil || model.ID != schema.Embedding.ModelID ||
		model.Revision != schema.Embedding.ModelRevision || model.Dimensions != schema.Embedding.Dimensions {
		return nil, ErrInvalidConfig
	}
	prefix := model.DocumentPrefix
	if role == modelInputQuery {
		prefix = model.QueryPrefix
	} else if role != modelInputDocument {
		return nil, ErrInvalidConfig
	}
	retrievalFingerprint, err := deriveRetrievalFingerprint(schema, model, &queryConfig)
	if err != nil {
		return nil, err
	}
	return &modelEncoder{
		role: role, chunker: chunker, client: client, schema: schema,
		limits: limits, dimensions: schema.Embedding.Dimensions, prefix: prefix, queryConfig: queryConfig,
		retrievalFingerprint: retrievalFingerprint,
	}, nil
}

func (e *modelEncoder) Schema() Schema { return e.schema }

func (e *modelEncoder) RetrievalFingerprint() string { return e.retrievalFingerprint }

func (e *modelEncoder) EncodeDocument(ctx context.Context, document fts.Document) ([]ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if e.role != modelInputDocument {
		return nil, ErrInvalidDocument
	}
	return e.encodeDocument(ctx, document)
}

func (e *modelEncoder) EncodeQueryDocument(ctx context.Context, document fts.Document) ([]ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if e.role != modelInputQuery {
		return nil, ErrInvalidQuery
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

func (e *modelEncoder) EncodeQuery(ctx context.Context, text string) ([]ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if e.role != modelInputQuery || text == "" || !utf8.ValidString(text) {
		return nil, ErrInvalidQuery
	}
	inputs, err := e.queryInputs(text)
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
		ranges, err := e.chunker.Split(text)
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

func (e *modelEncoder) queryInputs(text string) ([]modelInput, error) {
	maxBytes := e.queryConfig.MaxBytes
	if len(text) <= maxBytes {
		return []modelInput{queryInput(text, 0, len(text), 0)}, nil
	}
	switch e.queryConfig.LongQuery {
	case LongQueryReject:
		return nil, fmt.Errorf("%w: query bytes %d exceed %d", ErrInputLimitExceeded, len(text), maxBytes)
	case LongQueryTruncate:
		end := utf8AlignedEnd(text, maxBytes)
		if end == 0 {
			return nil, ErrInvalidQuery
		}
		return []modelInput{queryInput(text, 0, end, 0)}, nil
	case LongQueryChunk:
		inputs := make([]modelInput, 0, (len(text)-1)/maxBytes+1)
		for start := 0; start < len(text); {
			end := utf8AlignedEnd(text[start:], maxBytes) + start
			if end <= start {
				return nil, ErrInvalidQuery
			}
			inputs = append(inputs, queryInput(text, start, end, len(inputs)))
			if len(inputs) > e.limits.maxChunks {
				return nil, fmt.Errorf("%w: query chunks exceed %d", ErrInputLimitExceeded, e.limits.maxChunks)
			}
			start = end
		}
		return inputs, nil
	default:
		return nil, ErrInvalidConfig
	}
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

func utf8AlignedEnd(text string, maxBytes int) int {
	end := min(len(text), maxBytes)
	for end > 0 && end < len(text) && !utf8.RuneStart(text[end]) {
		end--
	}
	return end
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
