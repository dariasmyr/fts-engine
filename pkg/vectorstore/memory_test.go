package vectorstore

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestMemoryVectorStoreStoresPreparedRowsWithoutAliasing(t *testing.T) {
	space, err := vector.NewCalculator(2, vector.MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	input := [][]float32{{3, 4}, {0, 2}}
	source, err := NewMemoryVectorStore(space, input)
	if err != nil {
		t.Fatal(err)
	}
	input[0][0] = 100

	got := make([]float32, 2)
	if err := source.ReadVectorInto(context.Background(), 0, got); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []float32{0.6, 0.8}) {
		t.Fatalf("prepared row = %v", got)
	}
	got[0] = 99
	if err := source.ReadVectorInto(context.Background(), 0, got); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []float32{0.6, 0.8}) {
		t.Fatalf("stored row changed after destination mutation = %v", got)
	}
}

func TestPreparedMemoryVectorStoreValidatesMatrixShape(t *testing.T) {
	space, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewPreparedMemoryVectorStore(space, []float32{1}); !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Fatalf("shape error = %v, want ErrDimensionMismatch", err)
	}
}
