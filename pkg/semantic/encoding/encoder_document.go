package encoding

import (
	"context"
	"fmt"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// EncodeDocument prepares a source document for indexing.
func (e *Encoder) EncodeDocument(ctx context.Context, document fts.Document) ([]semantic.ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	inputs, err := e.documentInputs(ctx, document, e.documentLimits)
	if err != nil {
		return nil, err
	}
	return e.embedInputs(ctx, inputs, e.documentPrefix, e.documentLimits)
}

// EncodeQueryDocument prepares an advanced structured query document.
func (e *Encoder) EncodeQueryDocument(
	ctx context.Context,
	document fts.Document,
) ([]semantic.ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	inputs, err := e.documentInputs(ctx, document, e.queryLimits)
	if err != nil {
		return nil, err
	}
	return e.embedInputs(ctx, inputs, e.queryPrefix, e.queryLimits)
}

func (e *Encoder) documentInputs(
	ctx context.Context,
	document fts.Document,
	limits InputLimits,
) ([]encoderInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if document.ID == "" || !utf8.ValidString(string(document.ID)) || len(document.Fields) == 0 {
		return nil, ErrInvalidDocument
	}
	if len(document.Fields) > limits.MaxFields {
		return nil, fmt.Errorf(
			"%w: fields %d exceed %d",
			ErrInputLimitExceeded,
			len(document.Fields),
			limits.MaxFields,
		)
	}

	fields := make([]string, 0, len(document.Fields))
	var totalBytes uint64
	for field, value := range document.Fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		text := fieldText(value)
		if field == "" || !utf8.ValidString(field) || text == "" || !utf8.ValidString(text) {
			return nil, ErrInvalidDocument
		}
		if uint64(len(text)) > limits.MaxSourceBytes-totalBytes {
			return nil, fmt.Errorf("%w: source bytes exceed %d", ErrInputLimitExceeded, limits.MaxSourceBytes)
		}
		totalBytes += uint64(len(text))
		fields = append(fields, field)
	}
	slices.Sort(fields)

	inputs := make([]encoderInput, 0, min(len(fields), limits.MaxChunks))
	for _, field := range fields {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining := limits.MaxChunks - len(inputs)
		if remaining <= 0 {
			return nil, fmt.Errorf("%w: chunks exceed %d", ErrInputLimitExceeded, limits.MaxChunks)
		}
		text := fieldText(document.Fields[field])
		ranges, err := splitEncoderText(e.splitter, text)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q: %w", ErrInvalidDocument, field, err)
		}
		if len(ranges) == 0 {
			return nil, fmt.Errorf("%w: field %q produced no chunks", ErrInvalidDocument, field)
		}
		if len(ranges) > remaining || uint64(len(ranges)-1) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: chunks exceed %d", ErrInputLimitExceeded, limits.MaxChunks)
		}
		for ordinal, byteRange := range ranges {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := byteRange.Validate(text); err != nil {
				return nil, fmt.Errorf("%w: field %q: %w", ErrInvalidDocument, field, err)
			}
			inputs = append(inputs, encoderInput{
				ref: semantic.Ref{
					ID:        documentChunkID(field, ordinal, e.splitter == nil),
					DocID:     document.ID,
					Field:     field,
					Ordinal:   uint32(ordinal),
					StartByte: uint64(byteRange.StartByte),
					EndByte:   uint64(byteRange.EndByte),
				},
				text: text[byteRange.StartByte:byteRange.EndByte],
			})
		}
	}
	return inputs, nil
}

func fieldText(value fts.FieldData) string {
	if value.Text != "" {
		return value.Text
	}
	return value.Value
}

func documentChunkID(field string, ordinal int, whole bool) semantic.ChunkID {
	if whole {
		if field == fts.DefaultField {
			return "_whole"
		}
		return semantic.ChunkID("_whole/" + field)
	}
	id := fmt.Sprintf("chunk-%06d", ordinal)
	if field != fts.DefaultField {
		id = field + "/" + id
	}
	return semantic.ChunkID(id)
}
