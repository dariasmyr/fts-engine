package hnsw

import (
	"context"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Build constructs one immutable HNSW index from prepared source rows in stable
// ordinal order 0..source.Len()-1.
func Build(
	ctx context.Context,
	source vector.PreparedVectorStore,
	build BuildConfig,
	search SearchConfig,
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
	if err := build.validate(); err != nil {
		return nil, err
	}
	if err := search.validate(); err != nil {
		return nil, err
	}

	calculator, err := vector.NewCalculator(source.Dimensions(), source.Metric())
	if err != nil {
		return nil, err
	}
	if calculator.Normalization() != source.Normalization() {
		return nil, ErrInvalidSource
	}

	vectorCount := source.Len()
	if uint64(vectorCount) >= math.MaxUint32 {
		return nil, ErrInvalidSource
	}
	components, ok := checkedMultiply(uint64(vectorCount), uint64(calculator.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return nil, ErrInvalidSource
	}

	builder := newBuilder(build, calculator, vectorCount, int(components))
	scratch := make([]float32, calculator.Dimensions())
	for row := range vectorCount {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ordinal := vector.Ordinal(row)
		if err := source.ReadVectorInto(ctx, ordinal, scratch); err != nil {
			return nil, fmt.Errorf("vector/hnsw: read source row %d: %w", ordinal, err)
		}
		if err := builder.add(scratch); err != nil {
			return nil, fmt.Errorf("vector/hnsw: add source row %d: %w", ordinal, err)
		}
	}
	return builder.freeze(source, search)
}

func checkedMultiply(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}
