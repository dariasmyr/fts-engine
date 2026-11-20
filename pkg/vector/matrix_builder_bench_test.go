package vector

import "testing"

var matrixBuilderBenchmarkResult *Matrix

func BenchmarkMatrixBuilderBulk(b *testing.B) {
	const (
		rows       = 256
		dimensions = 768
	)
	spec, err := NewSpec(dimensions, MetricL2Squared)
	if err != nil {
		b.Fatal(err)
	}
	flat := make([]float32, rows*dimensions)
	for i := range flat {
		flat[i] = float32(i%101) / 101
	}
	prepared := make([]Prepared, rows)
	for row := range rows {
		start := row * dimensions
		prepared[row], err = spec.Prepare(flat[start : start+dimensions])
		if err != nil {
			b.Fatal(err)
		}
	}
	sourceRows := make([][]float32, rows)
	for row := range rows {
		start := row * dimensions
		sourceRows[row] = flat[start : start+dimensions]
	}
	source, err := NewMatrix(dimensions, MetricL2Squared, sourceRows)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("canonical-import-validates", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			builder, err := NewMatrixBuilder(dimensions, MetricL2Squared, rows)
			if err != nil {
				b.Fatal(err)
			}
			for row := range rows {
				start := row * dimensions
				if err := builder.AddCanonical(flat[start : start+dimensions]); err != nil {
					b.Fatal(err)
				}
			}
			matrixBuilderBenchmarkResult, err = builder.Build()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("opaque-prepared-no-scan", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			builder, err := NewMatrixBuilder(dimensions, MetricL2Squared, rows)
			if err != nil {
				b.Fatal(err)
			}
			for row := range rows {
				if err := builder.AddPrepared(prepared[row]); err != nil {
					b.Fatal(err)
				}
			}
			matrixBuilderBenchmarkResult, err = builder.Build()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("immutable-matrix-no-scan", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			builder, err := NewMatrixBuilder(dimensions, MetricL2Squared, rows)
			if err != nil {
				b.Fatal(err)
			}
			for row := range rows {
				if err := builder.AddMatrixRow(source, Ordinal(row)); err != nil {
					b.Fatal(err)
				}
			}
			matrixBuilderBenchmarkResult, err = builder.Build()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
