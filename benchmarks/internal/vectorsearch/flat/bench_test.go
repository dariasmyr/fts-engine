package flat

import (
	"context"
	"fmt"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func BenchmarkAppendBatch(b *testing.B) {
	const dimensions = 384
	for _, batchSize := range []int{1, 32, 256} {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			vectors := benchmarkVectors(batchSize, dimensions)
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * dimensions * 4))
			b.ResetTimer()
			for b.Loop() {
				idx, err := New(Config{
					Dimensions:            dimensions,
					Metric:                vector.MetricL2Squared,
					MaxVectors:            batchSize,
					MaxK:                  1,
					InitialVectorCapacity: batchSize,
				})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := idx.AppendBatch(vectors); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSearch(b *testing.B) {
	for _, dimensions := range []int{128, 384} {
		for _, rows := range []int{1_000, 10_000} {
			b.Run(fmt.Sprintf("dims=%d/rows=%d", dimensions, rows), func(b *testing.B) {
				idx, err := New(Config{
					Dimensions:            dimensions,
					Metric:                vector.MetricL2Squared,
					MaxVectors:            rows,
					MaxK:                  10,
					InitialVectorCapacity: rows,
				})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := idx.AppendBatch(benchmarkVectors(rows, dimensions)); err != nil {
					b.Fatal(err)
				}
				query := make([]float32, dimensions)
				b.ReportAllocs()
				b.SetBytes(int64(rows * dimensions * 4))
				b.ResetTimer()
				for b.Loop() {
					if _, err := idx.Search(context.Background(), query, 10, vector.SearchOptions{}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func benchmarkVectors(count, dimensions int) [][]float32 {
	vectors := make([][]float32, count)
	for row := range vectors {
		value := make([]float32, dimensions)
		for column := range value {
			value[column] = float32((row+1)*(column+1)%101) / 101
		}
		vectors[row] = value
	}
	return vectors
}
