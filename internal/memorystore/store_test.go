package memorystore

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
	source, err := New(space, input)
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
	if _, err := NewPrepared(space, []float32{1}); !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Fatalf("shape error = %v, want ErrDimensionMismatch", err)
	}
}

func TestMemoryVectorStoreOwnershipContracts(t *testing.T) {
	space, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}

	copiedInput := [][]float32{{1, 2}}

	copied, err := New(space, copiedInput)
	if err != nil {
		t.Fatal(err)
	}

	copiedInput[0][0] = 9

	row := make([]float32, 2)
	if err := copied.ReadVectorInto(context.Background(), 0, row); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(row, []float32{1, 2}) {
		t.Fatalf("copying constructor row = %v, want [1 2]", row)
	}

	ownedInput := []float32{3, 4}

	owned, err := NewPrepared(space, ownedInput)
	if err != nil {
		t.Fatal(err)
	}

	if &owned.vectors[0] != &ownedInput[0] {
		t.Fatal("ownership-taking constructor copied its input")
	}
}
