package semantic

import (
	"context"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// EncodeQuery prepares a direct text query with the configured splitter. With
// no splitter, the complete query is encoded as one input.
func (e *Encoder) EncodeQuery(
	ctx context.Context,
	text string,
	config QueryConfig,
) ([]ChunkEmbedding, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if text == "" || !utf8.ValidString(text) {
		return nil, ErrInvalidQuery
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	inputs, err := e.queryInputs(ctx, text)
	if err != nil {
		return nil, err
	}
	return e.embedInputs(ctx, inputs, e.queryPrefix, e.queryLimits)
}

func (e *Encoder) queryInputs(ctx context.Context, text string) ([]encoderInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if uint64(len(text)) > e.queryLimits.MaxSourceBytes {
		return nil, fmt.Errorf(
			"%w: query bytes exceed %d",
			ErrInputLimitExceeded,
			e.queryLimits.MaxSourceBytes,
		)
	}
	ranges, err := splitEncoderText(e.splitter, text)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
	}
	if len(ranges) == 0 {
		return nil, ErrInvalidQuery
	}
	if len(ranges) > e.queryLimits.MaxChunks || uint64(len(ranges)-1) > math.MaxUint32 {
		return nil, fmt.Errorf(
			"%w: query chunks exceed %d",
			ErrInputLimitExceeded,
			e.queryLimits.MaxChunks,
		)
	}

	inputs := make([]encoderInput, len(ranges))
	for ordinal, byteRange := range ranges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := byteRange.Validate(text); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
		}
		inputs[ordinal] = encoderInput{
			ref: Ref{
				ID:        ChunkID(fmt.Sprintf("query-%06d", ordinal)),
				DocID:     "_query",
				Field:     fts.DefaultField,
				Ordinal:   uint32(ordinal),
				StartByte: uint64(byteRange.StartByte),
				EndByte:   uint64(byteRange.EndByte),
			},
			text: text[byteRange.StartByte:byteRange.EndByte],
		}
	}
	return inputs, nil
}
