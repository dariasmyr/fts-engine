package vector

import "testing"

func TestMatrixBuilderFastPaths(t *testing.T) {
	spec, err := NewSpec(2, MetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := spec.Prepare([]float32{3, 4})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := NewMatrixBuilder(2, MetricCosine, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.AddPrepared(prepared); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddCanonical([]float32{1, 1}); err == nil {
		t.Fatal("AddCanonical() accepted a non-unit cosine row")
	}
	matrix, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	copyBuilder, err := NewMatrixBuilder(2, MetricCosine, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyBuilder.AddMatrixRow(matrix, 0); err != nil {
		t.Fatal(err)
	}
}
