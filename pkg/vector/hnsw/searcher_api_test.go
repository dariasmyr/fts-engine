package hnsw_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestPreparedReadersReadVectorInto(t *testing.T) {
	flatReader := testFlatReader(t, [][]float32{{1, 2}, {3, 4}}, vector.MetricL2Squared)
	hnswReader := testBuild(t, flatReader, 2, 2, vector.MetricL2Squared)
	for name, source := range map[string]vector.PreparedVectorSource{"flat": flatReader, "hnsw source": hnswReader.VectorSource()} {
		t.Run(name, func(t *testing.T) {
			dst := []float32{9, 9}
			if err := source.ReadVectorInto(context.Background(), 1, dst); err != nil || !slices.Equal(dst, []float32{3, 4}) {
				t.Fatalf("ReadVectorInto = (%v, %v)", dst, err)
			}
			dst[0] = 100
			got := make([]float32, 2)
			if err := source.ReadVectorInto(context.Background(), 1, got); err != nil || !slices.Equal(got, []float32{3, 4}) {
				t.Fatalf("reader storage was exposed: %v, %v", got, err)
			}
			if err := source.ReadVectorInto(context.Background(), 2, got); !errors.Is(err, vector.ErrOrdinalOutOfRange) {
				t.Fatalf("ordinal error = %v", err)
			}
			if err := source.ReadVectorInto(context.Background(), 0, got[:1]); !errors.Is(err, vector.ErrDimensionMismatch) {
				t.Fatalf("dimension error = %v", err)
			}
			if err := source.ReadVectorInto(nil, 0, got); !errors.Is(err, vector.ErrNilContext) {
				t.Fatalf("nil context error = %v", err)
			}
		})
	}
}

func TestReaderReportAccessors(t *testing.T) {
	flatReader := testFlatReader(t, [][]float32{{0, 0}, {1, 0}, {2, 0}}, vector.MetricL2Squared)
	reader := testBuild(t, flatReader, 2, 3, vector.MetricL2Squared)
	if reader.SearchConfig() != testSearchConfig(3) {
		t.Fatalf("search config = %+v", reader.SearchConfig())
	}
	storage := reader.StorageStats()
	graph := reader.GraphStats()
	if storage.VectorRows != 3 || storage.GraphNodes != 3 || storage.LevelPlacements != sumInts(graph.LevelNodeCounts) ||
		storage.DirectedLinks != sumInts(graph.LevelLinkCounts) || storage.VectorBytes != 24 || storage.TotalBytes == 0 {
		t.Fatalf("storage stats = %+v, graph = %+v", storage, graph)
	}
}

func sumInts(values []int) int {
	var sum int
	for _, value := range values {
		sum += value
	}
	return sum
}
