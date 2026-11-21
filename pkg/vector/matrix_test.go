package vector_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

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

func TestMatrixStoresPreparedRowsWithoutAliasing(t *testing.T) {
	spec, err := vector.NewSpec(2, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	input := [][]float32{{3, 4}, {0, 2}}
	source, err := vector.NewMatrix(spec.Dimensions(), spec.Metric(), input)
	if err != nil {
		t.Fatal(err)
	}
	input[0][0] = 100

	got := make([]float32, 2)
	if err := source.ReadInto(context.Background(), 0, got); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []float32{0.6, 0.8}) {
		t.Fatalf("prepared row = %v", got)
	}
	got[0] = 99
	if err := source.ReadInto(context.Background(), 0, got); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []float32{0.6, 0.8}) {
		t.Fatalf("stored row changed after destination mutation = %v", got)
	}
}

func TestMatrixDistanceRowsBatchMatchesScalar(t *testing.T) {
	const dimensions = 13
	vectors := make([][]float32, 5)
	for row := range vectors {
		vectors[row] = make([]float32, dimensions)
		for dimension := range vectors[row] {
			vectors[row][dimension] = float32(math.Sin(float64((row + 1) * (dimension + 1))))
		}
	}
	for _, metric := range []vector.Metric{vector.MetricCosine, vector.MetricL2Squared} {
		t.Run(metric.String(), func(t *testing.T) {
			matrix, err := vector.NewMatrix(dimensions, metric, vectors)
			if err != nil {
				t.Fatal(err)
			}
			ordinals := [4]vector.Ordinal{1, 2, 3, 4}
			for count := 1; count <= len(ordinals); count++ {
				distances, distanceErr := matrix.DistanceRowsBatch(0, ordinals, count)
				if distanceErr != nil {
					t.Fatal(distanceErr)
				}
				for i := range count {
					want, scalarErr := matrix.DistanceRows(0, ordinals[i])
					if scalarErr != nil {
						t.Fatal(scalarErr)
					}
					if distances[i] != want {
						t.Fatalf("count %d distance %d = %.17g, scalar = %.17g", count, i, distances[i], want)
					}
				}
			}
		})
	}
}

func TestCanonicalMatrixValidatesShape(t *testing.T) {
	spec, err := vector.NewSpec(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newCanonicalTestMatrix(spec.Dimensions(), spec.Metric(), []float32{1}); !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Fatalf("shape error = %v, want ErrDimensionMismatch", err)
	}
}

func TestCanonicalMatrixValidatesRows(t *testing.T) {
	tests := []struct {
		name   string
		metric vector.Metric
		value  []float32
	}{
		{name: "non-finite", metric: vector.MetricL2Squared, value: []float32{1, float32(math.NaN())}},
		{name: "negative zero", metric: vector.MetricL2Squared, value: []float32{1, math.Float32frombits(1 << 31)}},
		{name: "non-unit cosine", metric: vector.MetricCosine, value: []float32{1, 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec, err := vector.NewSpec(2, test.metric)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := newCanonicalTestMatrix(spec.Dimensions(), spec.Metric(), test.value); err == nil {
				t.Fatal("MatrixBuilder.AddCanonical() accepted an invalid prepared row")
			}
		})
	}
}

func TestMatrixFromPreparedPreservesProvenance(t *testing.T) {
	spec, err := vector.NewSpec(2, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := spec.Prepare([]float32{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := vector.NewMatrixBuilder(spec.Dimensions(), spec.Metric(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.AddPrepared(prepared); err != nil {
		t.Fatal(err)
	}
	store, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	row := make([]float32, 2)
	if err := store.ReadInto(t.Context(), 0, row); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(row, []float32{0.6, 0.8}) {
		t.Fatalf("built row = %v, want [0.6 0.8]", row)
	}
}

func TestMatrixOwnershipContracts(t *testing.T) {
	spec, err := vector.NewSpec(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}

	copiedInput := [][]float32{{1, 2}}

	copied, err := vector.NewMatrix(spec.Dimensions(), spec.Metric(), copiedInput)
	if err != nil {
		t.Fatal(err)
	}

	copiedInput[0][0] = 9

	row := make([]float32, 2)
	if err := copied.ReadInto(context.Background(), 0, row); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(row, []float32{1, 2}) {
		t.Fatalf("copying constructor row = %v, want [1 2]", row)
	}

	ownedInput := []float32{3, 4}

	owned, err := newCanonicalTestMatrix(spec.Dimensions(), spec.Metric(), ownedInput)
	if err != nil {
		t.Fatal(err)
	}

	if err := owned.ReadInto(t.Context(), 0, row); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(row, ownedInput) {
		t.Fatalf("owned row = %v, want %v", row, ownedInput)
	}
}
