package hnsw

import (
	"context"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Build constructs an immutable HNSW index over matrix rows in stable ordinal
// order 0..matrix.Len()-1. The index retains matrix for searching.
func Build(
	ctx context.Context,
	matrix *vector.Matrix,
	config Config,
) (*Index, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if matrix == nil {
		return nil, ErrInvalidSource
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	spec := matrix.Spec()
	if spec.Dimensions() <= 0 {
		return nil, ErrInvalidSource
	}

	vectorCount := matrix.Len()
	if uint64(vectorCount) >= math.MaxUint32 {
		return nil, ErrInvalidSource
	}
	components, ok := checkedMultiply(uint64(vectorCount), uint64(spec.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return nil, ErrInvalidSource
	}

	builder := newBuilder(config.Build, spec, matrix, vectorCount)

	for row := range vectorCount {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := builder.add(ctx); err != nil {
			return nil, fmt.Errorf("vector/hnsw: add matrix row %d: %w", row, err)
		}
	}

	return builder.freeze(ctx, config)
}

func checkedMultiply(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
