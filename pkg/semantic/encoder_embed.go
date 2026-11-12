package semantic

import (
	"context"
	"fmt"
	"math"
)

type encoderInput struct {
	ref  Ref
	text string
}

func (e *Encoder) embedInputs(
	ctx context.Context,
	inputs []encoderInput,
	prefix string,
	limits InputLimits,
) ([]ChunkEmbedding, error) {
	bytesPerVector := uint64(e.dimensions) * 4
	if e.dimensions <= 0 || uint64(e.dimensions) > math.MaxUint64/4 ||
		uint64(len(inputs)) > math.MaxUint64/bytesPerVector ||
		uint64(len(inputs))*bytesPerVector > limits.MaxEmbeddingBytes {
		return nil, fmt.Errorf(
			"%w: embeddings exceed %d bytes",
			ErrInputLimitExceeded,
			limits.MaxEmbeddingBytes,
		)
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
	if len(vectors) != len(inputs) {
		return nil, fmt.Errorf(
			"%w: got %d vectors for %d inputs",
			ErrEmbeddingCountMismatch,
			len(vectors),
			len(inputs),
		)
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
