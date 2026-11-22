package vector_test

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestMatrixBuilderPublicMethods(t *testing.T) {
	typeOfBuilder := reflect.TypeOf((*vector.MatrixBuilder)(nil))
	want := []string{"Add", "AddCanonical", "AddMatrixRow", "AddPrepared", "Build", "Len"}
	if typeOfBuilder.NumMethod() != len(want) {
		t.Fatalf("MatrixBuilder has %d public methods, want %d", typeOfBuilder.NumMethod(), len(want))
	}
	for i, name := range want {
		if got := typeOfBuilder.Method(i).Name; got != name {
			t.Fatalf("MatrixBuilder method %d = %q, want %q", i, got, name)
		}
	}
}

func TestMatrixBuilderBuildsRawRows(t *testing.T) {
	builder, err := vector.NewMatrixBuilder(2, vector.MetricCosine, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Add([]float32{3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := builder.Add([]float32{0, 2}); err != nil {
		t.Fatal(err)
	}
	matrix, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	got := make([]float32, 2)
	if err := matrix.ReadInto(t.Context(), 0, got); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []float32{0.6, 0.8}) {
		t.Fatalf("row = %v, want [0.6 0.8]", got)
	}
	if _, err := builder.Build(); !errors.Is(err, vector.ErrMatrixBuilderFinalized) {
		t.Fatalf("second Build() error = %v, want ErrMatrixBuilderFinalized", err)
	}
}
