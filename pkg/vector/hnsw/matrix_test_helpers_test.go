package hnsw

import (
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func newCanonicalTestMatrix(dimensions int, metric vector.Metric, values []float32) (*vector.Matrix, error) {
	if dimensions <= 0 || len(values)%dimensions != 0 {
		return nil, vector.ErrDimensionMismatch
	}
	builder, err := vector.NewMatrixBuilder(dimensions, metric, len(values)/dimensions)
	if err != nil {
		return nil, err
	}
	for row := range len(values) / dimensions {
		start := row * dimensions
		if err := builder.AddCanonical(values[start : start+dimensions]); err != nil {
			return nil, fmt.Errorf("row %d: %w", row, err)
		}
	}
	return builder.Build()
}
