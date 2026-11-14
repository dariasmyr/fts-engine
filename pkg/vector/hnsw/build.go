package hnsw

import (
	"context"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Build constructs one immutable HNSW index from prepared source rows in stable
// ordinal order 0..source.Len()-1.
func Build(
	ctx context.Context,
	source vector.PreparedVectorStore,
	config Config,
) (*Index, error) {
	if ctx == nil {
		return nil, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || source.Len() < 0 {
		return nil, ErrInvalidSource
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	space, err := vector.NewSpace(source.Dimensions(), source.Metric())
	if err != nil {
		return nil, err
	}
	if space.Normalization() != source.Normalization() {
		return nil, ErrInvalidSource
	}

	vectorCount := source.Len()
	if uint64(vectorCount) >= math.MaxUint32 {
		return nil, ErrInvalidSource
	}
	components, ok := checkedMultiply(uint64(vectorCount), uint64(space.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return nil, ErrInvalidSource
	}

	builder := newBuilder(config.Build, space, vectorCount, int(components))
	vectorContainer := make([]float32, space.Dimensions())
	// for each vector in the source, read it and add it to the builder
	for row := range vectorCount {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ordinal := vector.Ordinal(row)
		if err := source.ReadVectorInto(ctx, ordinal, vectorContainer); err != nil {
			return nil, fmt.Errorf("vector/hnsw: read source vector %d: %w", ordinal, err)
		}
		if err := builder.add(ctx, vectorContainer); err != nil {
			return nil, fmt.Errorf("vector/hnsw: add source vector %d: %w", ordinal, err)
		}
	}

	// freeze the builder into an immutable index
	return builder.freeze(ctx, source, config)
}

func checkedMultiply(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
