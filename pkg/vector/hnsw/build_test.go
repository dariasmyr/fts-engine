package hnsw_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func testBuildConfig(dimensions, count int, metric vector.Metric) hnsw.BuildConfig {
	return hnsw.BuildConfig{
		Dimensions: dimensions, Metric: metric, MaxVectors: max(1, count), MaxVectorBytes: uint64(max(1, dimensions*count*4)),
		MaxNeighbors: 4, EfConstruction: 32, Seed: 17,
	}
}

func testSearchConfig(count int) hnsw.SearchConfig {
	limit := max(1, count)
	return hnsw.SearchConfig{
		DefaultEfSearch: min(8, limit), MaxEfSearch: limit,
		DefaultVisitLimit: limit, MaxVisitLimit: limit, MaxK: limit,
	}
}

func testMemoryStore(t testing.TB, values [][]float32, metric vector.Metric) *vectorstore.MemoryVectorStore {
	t.Helper()
	calculator, err := vector.NewCalculator(len(values[0]), metric)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := vectorstore.NewMemoryVectorStore(calculator, values)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func testBuild(t testing.TB, source vectorstore.PreparedVectorStore, dimensions, count int, metric vector.Metric) *hnsw.Index {
	t.Helper()
	reader, err := hnsw.Build(context.Background(), source, hnsw.BuildOptions{
		Build: testBuildConfig(dimensions, count, metric), Search: testSearchConfig(count),
	})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func readPreparedVector(source vectorstore.PreparedVectorStore, ordinal vector.Ordinal) ([]float32, bool) {
	if source == nil || uint64(ordinal) >= uint64(source.Len()) {
		return nil, false
	}
	value := make([]float32, source.Dimensions())
	if err := source.ReadVectorInto(context.Background(), ordinal, value); err != nil {
		return nil, false
	}
	return value, true
}

func TestBuildProgressStableOrderAndPreparedCosineBits(t *testing.T) {
	source := testMemoryStore(t, [][]float32{{3, 4}, {-5, 12}, {8, 15}}, vector.MetricCosine)
	var progress []hnsw.BuildProgress
	reader, err := hnsw.Build(context.Background(), source, hnsw.BuildOptions{
		Build:  testBuildConfig(2, source.Len(), vector.MetricCosine),
		Search: testSearchConfig(source.Len()),
		Progress: func(value hnsw.BuildProgress) {
			progress = append(progress, value)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantProgress := []hnsw.BuildProgress{
		{Phase: hnsw.BuildPhasePreflight, Total: 3},
		{Phase: hnsw.BuildPhaseVectors, Total: 3},
		{Phase: hnsw.BuildPhaseVectors, Completed: 1, Total: 3},
		{Phase: hnsw.BuildPhaseVectors, Completed: 2, Total: 3},
		{Phase: hnsw.BuildPhaseVectors, Completed: 3, Total: 3},
		{Phase: hnsw.BuildPhaseFreeze, Completed: 3, Total: 3},
		{Phase: hnsw.BuildPhaseComplete, Completed: 3, Total: 3},
	}
	if !slices.Equal(progress, wantProgress) {
		t.Fatalf("progress = %+v, want %+v", progress, wantProgress)
	}
	for row := range source.Len() {
		query, _ := readPreparedVector(source, vector.Ordinal(row))
		result, err := reader.Search(context.Background(), query, 1, vector.SearchOptions{EfSearch: source.Len(), VisitLimit: source.Len()})
		if err != nil || len(result.Hits) != 1 || result.Hits[0].Ordinal != vector.Ordinal(row) || math.Float32bits(float32(result.Hits[0].Distance)) != 0 {
			t.Fatalf("row %d search = (%+v, %v)", row, result, err)
		}
	}
}

func TestBuildTopologyUsesTheBoundSourceValues(t *testing.T) {
	values := [][]float32{{0, 0}, {10, 0}, {0, 10}, {10, 10}, {5, 5}}
	source := testMemoryStore(t, values, vector.MetricL2Squared)
	reader := testBuild(t, source, 2, len(values), vector.MetricL2Squared)
	for ordinal, query := range values {
		result, err := reader.Search(context.Background(), query, 1, vector.SearchOptions{EfSearch: len(values), VisitLimit: len(values)})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Hits) != 1 || result.Hits[0].Ordinal != vector.Ordinal(ordinal) {
			t.Fatalf("query %d returned %+v, want ordinal %d", ordinal, result.Hits, ordinal)
		}
	}
}

func TestBuildSearchReadsRetainedPreparedSource(t *testing.T) {
	source := &testSource{
		values:     [][]float32{{1, 2}, {3, 4}},
		dimensions: 2,
		metric:     vector.MetricL2Squared,
	}
	reader := testBuild(t, source, 2, 2, vector.MetricL2Squared)
	source.reads = nil

	if _, err := reader.Search(context.Background(), []float32{3, 4}, 1, vector.SearchOptions{EfSearch: 2, VisitLimit: 2}); err != nil {
		t.Fatal(err)
	}
	if len(source.reads) == 0 {
		t.Fatalf("reader did not retain prepared source, reads = %v", source.reads)
	}
}

type testSource struct {
	values        [][]float32
	dimensions    int
	metric        vector.Metric
	normalization vector.Normalization
	readErrAt     int
	readErr       error
	partial       bool
	reads         []vector.Ordinal
}

func (s *testSource) Len() int                            { return len(s.values) }
func (s *testSource) Dimensions() int                     { return s.dimensions }
func (s *testSource) Metric() vector.Metric               { return s.metric }
func (s *testSource) Normalization() vector.Normalization { return s.normalization }
func (s *testSource) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.reads = append(s.reads, ordinal)
	if int(ordinal) == s.readErrAt && s.readErr != nil {
		return s.readErr
	}
	if s.partial {
		copy(dst[:1], s.values[ordinal][:1])
		return nil
	}
	copy(dst, s.values[ordinal])
	return nil
}

func TestBuildPreflightReadErrorsAndContext(t *testing.T) {
	options := hnsw.BuildOptions{Build: testBuildConfig(2, 2, vector.MetricL2Squared), Search: testSearchConfig(2)}
	mismatch := &testSource{values: [][]float32{{1}, {2}}, dimensions: 1, metric: vector.MetricL2Squared}
	if _, err := hnsw.Build(context.Background(), mismatch, options); !errors.Is(err, hnsw.ErrBuildSourceMismatch) {
		t.Fatalf("metadata error = %v", err)
	}
	if len(mismatch.reads) != 0 {
		t.Fatalf("metadata mismatch read rows %v", mismatch.reads)
	}

	sentinel := errors.New("source read failed")
	failing := &testSource{values: [][]float32{{1, 2}, {3, 4}}, dimensions: 2, metric: vector.MetricL2Squared, readErrAt: 1, readErr: sentinel}
	if _, err := hnsw.Build(context.Background(), failing, options); !errors.Is(err, sentinel) {
		t.Fatalf("read error = %v", err)
	}
	if !slices.Equal(failing.reads, []vector.Ordinal{0, 1}) {
		t.Fatalf("read order = %v", failing.reads)
	}
	partial := &testSource{values: [][]float32{{1, 2}, {3, 4}}, dimensions: 2, metric: vector.MetricL2Squared, partial: true}
	if _, err := hnsw.Build(context.Background(), partial, options); !errors.Is(err, vector.ErrNonFiniteVector) {
		t.Fatalf("partial source row error = %v", err)
	}

	if _, err := hnsw.Build(nil, failing, options); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hnsw.Build(canceled, failing, options); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context error = %v", err)
	}

	progressSource := &testSource{values: [][]float32{{1, 2}, {3, 4}}, dimensions: 2, metric: vector.MetricL2Squared}
	progressCtx, progressCancel := context.WithCancel(context.Background())
	options.Progress = func(progress hnsw.BuildProgress) {
		if progress.Phase == hnsw.BuildPhaseVectors && progress.Completed == 1 {
			progressCancel()
		}
	}
	if _, err := hnsw.Build(progressCtx, progressSource, options); !errors.Is(err, context.Canceled) {
		t.Fatalf("progress cancellation error = %v", err)
	}
	if !slices.Equal(progressSource.reads, []vector.Ordinal{0}) {
		t.Fatalf("progress cancellation reads = %v", progressSource.reads)
	}
}
