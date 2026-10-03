package hnsw_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func TestPreparedReadersReadVectorInto(t *testing.T) {
	source := testMemoryStore(t, [][]float32{{1, 2}, {3, 4}}, vector.MetricL2Squared)
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
}

func TestReaderReportAccessors(t *testing.T) {
	source := testMemoryStore(t, [][]float32{{0, 0}, {1, 0}, {2, 0}}, vector.MetricL2Squared)
	reader := testBuild(t, source, 2, 3, vector.MetricL2Squared)
	report := reader.Report()
	if report.Search != testSearchConfig(3) {
		t.Fatalf("search config = %+v", report.Search)
	}
	storage := report.Storage
	graph := report.Graph
	if storage.VectorRows != 3 || storage.GraphNodes != 3 || storage.LevelPlacements != sumInts(graph.LevelNodeCounts) ||
		storage.DirectedLinks != sumInts(graph.LevelLinkCounts) || storage.VectorBytes != 24 || storage.TotalBytes == 0 {
		t.Fatalf("storage stats = %+v, graph = %+v", storage, graph)
	}
}

func TestIndexValidateSource(t *testing.T) {
	source := testMemoryStore(t, [][]float32{{1, 2}, {3, 4}}, vector.MetricL2Squared)
	index := testBuild(t, source, 2, 2, vector.MetricL2Squared)
	if err := index.ValidateSource(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	equal := testMemoryStore(t, [][]float32{{1, 2}, {3, 4}}, vector.MetricL2Squared)
	if err := index.ValidateSource(context.Background(), equal); err != nil {
		t.Fatalf("equal source error = %v", err)
	}
	different := testMemoryStore(t, [][]float32{{1, 2}, {3, 5}}, vector.MetricL2Squared)
	if err := index.ValidateSource(context.Background(), different); !errors.Is(err, hnsw.ErrBuildSourceMismatch) {
		t.Fatalf("different source error = %v", err)
	}
	if err := index.ValidateSource(nil, source); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil context error = %v", err)
	}
}

func TestIndexValidateSourceWithNonComparableValueStore(t *testing.T) {
	calculator, err := vector.NewCalculator(2, vector.MetricL2Squared)
	if err != nil {
		t.Fatal(err)
	}
	source := valuePreparedStore{
		calculator: calculator,
		rows:       any([][]float32{{1, 2}, {3, 4}}),
	}
	index := testBuild(t, source, 2, source.Len(), vector.MetricL2Squared)
	if err := index.ValidateSource(context.Background(), source); err != nil {
		t.Fatal(err)
	}
}

type valuePreparedStore struct {
	calculator vector.Calculator
	rows       any
}

func (s valuePreparedStore) Len() int                            { return len(s.rows.([][]float32)) }
func (s valuePreparedStore) Dimensions() int                     { return s.calculator.Dimensions() }
func (s valuePreparedStore) Metric() vector.Metric               { return s.calculator.Metric() }
func (s valuePreparedStore) Normalization() vector.Normalization { return s.calculator.Normalization() }

func (s valuePreparedStore) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	copy(dst, s.rows.([][]float32)[ordinal])
	return nil
}

func sumInts(values []int) int {
	var sum int
	for _, value := range values {
		sum += value
	}
	return sum
}
