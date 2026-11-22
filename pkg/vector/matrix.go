package vector

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector/internal/dot"
)

// Matrix is an immutable contiguous in-memory matrix of prepared vectors.
// Rows are addressed by Ordinal. Matrix owns its storage and is safe for
// concurrent reads.
type Matrix struct {
	spec   Spec
	values []float32
}

// NewMatrix validates, prepares, and copies vectors into contiguous memory.
func NewMatrix(dimensions int, metric Metric, vectors [][]float32) (*Matrix, error) {
	builder, err := NewMatrixBuilder(dimensions, metric, len(vectors))
	if err != nil {
		return nil, err
	}
	for row, value := range vectors {
		if err := builder.Add(value); err != nil {
			return nil, fmt.Errorf("vector: prepare row %d: %w", row, err)
		}
	}
	return builder.Build()
}

func newMatrix(spec Spec, values []float32) *Matrix {
	return &Matrix{spec: spec, values: values}
}

// Len returns the number of rows.
func (m *Matrix) Len() int {
	if m == nil || m.spec.Dimensions() <= 0 {
		return 0
	}
	return len(m.values) / m.spec.Dimensions()
}

// Spec returns the immutable definition shared by all matrix rows.
func (m *Matrix) Spec() Spec {
	if m == nil {
		return Spec{}
	}
	return m.spec
}

// ReadInto copies one row into dst without exposing matrix storage.
func (m *Matrix) ReadInto(ctx context.Context, ordinal Ordinal, dst []float32) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || m.spec.Dimensions() <= 0 {
		return ErrInvalidDimensions
	}
	if len(dst) != m.spec.Dimensions() {
		return fmt.Errorf("%w: got %d, want %d", ErrDimensionMismatch, len(dst), m.spec.Dimensions())
	}
	row, err := m.row(ordinal)
	if err != nil {
		return err
	}
	copy(dst, row)
	return nil
}

// DistancePrepared returns the distance from a prepared query to one row.
func (m *Matrix) DistancePrepared(query Prepared, ordinal Ordinal) (float64, error) {
	if m == nil || !m.spec.compatiblePrepared(query) {
		return 0, ErrDimensionMismatch
	}
	row, err := m.row(ordinal)
	if err != nil {
		return 0, err
	}
	return m.spec.DistancePrepared(query, row), nil
}

// DistancePreparedBatch returns distances from a prepared query to a batch of
// up to four matrix rows. Only the first count ordinals are read; remaining
// results are zero.
func (m *Matrix) DistancePreparedBatch(query Prepared, ordinals [4]Ordinal, count int) ([4]float64, error) {
	if m == nil || !m.spec.compatiblePrepared(query) {
		return [4]float64{}, ErrDimensionMismatch
	}
	if err := validateBatchCount(count); err != nil {
		return [4]float64{}, err
	}
	return m.distancePreparedValuesBatch(query.value, ordinals, count)
}

// DistanceRows returns the distance between two rows.
func (m *Matrix) DistanceRows(a, b Ordinal) (float64, error) {
	if m == nil {
		return 0, ErrInvalidDimensions
	}
	left, err := m.row(a)
	if err != nil {
		return 0, err
	}
	right, err := m.row(b)
	if err != nil {
		return 0, err
	}
	return m.spec.DistancePreparedValues(left, right), nil
}

// DistanceRowsBatch returns distances from one row to a batch of up to four
// other rows. It selects scalar, batch x2, or batch x4 computation according
// to count. Only the first count ordinals are read; remaining results are zero.
// P.S. Inspired by Faiss method qdis.distances_batch_4().
func (m *Matrix) DistanceRowsBatch(a Ordinal, bs [4]Ordinal, count int) ([4]float64, error) {
	if m == nil {
		return [4]float64{}, ErrInvalidDimensions
	}
	if err := validateBatchCount(count); err != nil {
		return [4]float64{}, err
	}
	target, err := m.row(a)
	if err != nil {
		return [4]float64{}, err
	}
	return m.distancePreparedValuesBatch(target, bs, count)
}

func (m *Matrix) distancePreparedValuesBatch(target []float32, ordinals [4]Ordinal, count int) ([4]float64, error) {
	var rows [4][]float32
	for i := range count {
		row, err := m.row(ordinals[i])
		if err != nil {
			return [4]float64{}, err
		}
		rows[i] = row
	}
	if m.spec.Metric() != MetricCosine {
		// batch dot products currently target only the cosine build hot path. Other
		// metrics retain their existing scalar distance implementation.
		var result [4]float64
		for i := range count {
			result[i] = m.spec.DistancePreparedValues(target, rows[i])
		}
		return result, nil
	}

	var similarities [4]float64
	// select the implementation for the actual number of rows, avoiding
	// unused dot products for an incomplete batch.
	switch count {
	case 1:
		similarities[0] = dot.Float64(target, rows[0])
	case 2:
		similarities[0], similarities[1] = dot.Batch2(target, rows[0], rows[1])
	case 3:
		similarities[0], similarities[1] = dot.Batch2(target, rows[0], rows[1])
		similarities[2] = dot.Float64(target, rows[2])
	case 4:
		similarities = dot.Batch4(target, rows[0], rows[1], rows[2], rows[3])
	}
	var result [4]float64
	for i := range count {
		similarity := max(-1, min(1, similarities[i]))
		result[i] = 1 - similarity
	}
	return result, nil
}

func (m *Matrix) row(ordinal Ordinal) ([]float32, error) {
	if uint64(ordinal) >= uint64(m.Len()) {
		return nil, fmt.Errorf("%w: %d", ErrOrdinalOutOfRange, ordinal)
	}
	dimensions := m.spec.Dimensions()
	start := int(ordinal) * dimensions
	return m.values[start : start+dimensions], nil
}

func validateBatchCount(count int) error {
	if count < 1 || count > 4 {
		return fmt.Errorf("vector: invalid batch count: %d", count)
	}
	return nil
}
