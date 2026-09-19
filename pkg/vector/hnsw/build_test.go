package hnsw_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/flat"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
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

func testFlatReader(t testing.TB, values [][]float32, metric vector.Metric) *flat.Reader {
	t.Helper()
	idx, err := flat.New(flat.Config{Dimensions: len(values[0]), Metric: metric, MaxVectors: len(values), MaxK: len(values)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idx.AppendBatch(values); err != nil {
		t.Fatal(err)
	}
	return idx.Freeze()
}

func testBuild(t testing.TB, source hnsw.PreparedVectorSource, dimensions, count int, metric vector.Metric) *hnsw.Reader {
	t.Helper()
	reader, err := hnsw.BuildIndexReader(context.Background(), source, hnsw.BuildOptions{
		BuildConfig: testBuildConfig(dimensions, count, metric), SearchConfig: testSearchConfig(count),
	})
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func TestBuildProgressStableOrderAndPreparedCosineBits(t *testing.T) {
	source := testFlatReader(t, [][]float32{{3, 4}, {-5, 12}, {8, 15}}, vector.MetricCosine)
	var progress []hnsw.BuildProgress
	reader, err := hnsw.BuildIndexReader(context.Background(), source, hnsw.BuildOptions{
		BuildConfig:  testBuildConfig(2, source.Len(), vector.MetricCosine),
		SearchConfig: testSearchConfig(source.Len()),
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
		want, _ := source.Vector(vector.Ordinal(row))
		got, _ := reader.Vector(vector.Ordinal(row))
		if !slices.EqualFunc(got, want, func(a, b float32) bool { return math.Float32bits(a) == math.Float32bits(b) }) {
			t.Fatalf("row %d bits changed: got %v want %v", row, got, want)
		}
	}
	for node := range reader.NodeCount() {
		value, ok := reader.Vector(vector.Ordinal(node))
		if !ok || len(value) != 2 {
			t.Fatalf("dense row %d unavailable", node)
		}
	}
}

func TestBuildRetainsPreparedSource(t *testing.T) {
	source := &testSource{
		values:     [][]float32{{1, 2}, {3, 4}},
		dimensions: 2,
		metric:     vector.MetricL2Squared,
	}
	reader := testBuild(t, source, 2, 2, vector.MetricL2Squared)
	source.reads = nil

	if _, ok := reader.Vector(1); !ok {
		t.Fatal("reader.Vector(1) failed")
	}
	if !slices.Equal(source.reads, []vector.Ordinal{1}) {
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
	options := hnsw.BuildOptions{BuildConfig: testBuildConfig(2, 2, vector.MetricL2Squared), SearchConfig: testSearchConfig(2)}
	var nilFlat *flat.Reader
	if _, err := hnsw.BuildIndexReader(context.Background(), nilFlat, options); !errors.Is(err, hnsw.ErrBuildSourceMismatch) {
		t.Fatalf("typed nil source error = %v", err)
	}
	mismatch := &testSource{values: [][]float32{{1}, {2}}, dimensions: 1, metric: vector.MetricL2Squared}
	if _, err := hnsw.BuildIndexReader(context.Background(), mismatch, options); !errors.Is(err, hnsw.ErrBuildSourceMismatch) {
		t.Fatalf("metadata error = %v", err)
	}
	if len(mismatch.reads) != 0 {
		t.Fatalf("metadata mismatch read rows %v", mismatch.reads)
	}

	sentinel := errors.New("source read failed")
	failing := &testSource{values: [][]float32{{1, 2}, {3, 4}}, dimensions: 2, metric: vector.MetricL2Squared, readErrAt: 1, readErr: sentinel}
	if _, err := hnsw.BuildIndexReader(context.Background(), failing, options); !errors.Is(err, sentinel) {
		t.Fatalf("read error = %v", err)
	}
	if !slices.Equal(failing.reads, []vector.Ordinal{0, 1}) {
		t.Fatalf("read order = %v", failing.reads)
	}
	partial := &testSource{values: [][]float32{{1, 2}, {3, 4}}, dimensions: 2, metric: vector.MetricL2Squared, partial: true}
	if _, err := hnsw.BuildIndexReader(context.Background(), partial, options); !errors.Is(err, vector.ErrNonFiniteVector) {
		t.Fatalf("partial source row error = %v", err)
	}

	if _, err := hnsw.BuildIndexReader(nil, failing, options); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hnsw.BuildIndexReader(canceled, failing, options); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context error = %v", err)
	}

	progressSource := &testSource{values: [][]float32{{1, 2}, {3, 4}}, dimensions: 2, metric: vector.MetricL2Squared}
	progressCtx, progressCancel := context.WithCancel(context.Background())
	options.Progress = func(progress hnsw.BuildProgress) {
		if progress.Phase == hnsw.BuildPhaseVectors && progress.Completed == 1 {
			progressCancel()
		}
	}
	if _, err := hnsw.BuildIndexReader(progressCtx, progressSource, options); !errors.Is(err, context.Canceled) {
		t.Fatalf("progress cancellation error = %v", err)
	}
	if !slices.Equal(progressSource.reads, []vector.Ordinal{0}) {
		t.Fatalf("progress cancellation reads = %v", progressSource.reads)
	}
}
