package vectorsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func TestDatasetAndHashDeterministic(t *testing.T) {
	for _, kind := range []DatasetKind{DatasetUniform, DatasetClustered, DatasetDuplicate} {
		config := DatasetConfig{Kind: kind, Dimensions: 6, Vectors: 20, Queries: 7, Seed: 42, Clusters: 3}
		first, err := GenerateDataset(config)
		if err != nil {
			t.Fatal(err)
		}
		second, err := GenerateDataset(config)
		if err != nil {
			t.Fatal(err)
		}
		if first.Hash == "" || first.Hash != second.Hash || !equalRows(first.Vectors, second.Vectors) || !equalRows(first.Queries, second.Queries) {
			t.Fatalf("%s dataset is not deterministic: %q != %q", kind, first.Hash, second.Hash)
		}
		changed := config
		changed.Seed++
		third, err := GenerateDataset(changed)
		if err != nil {
			t.Fatal(err)
		}
		if third.Hash == first.Hash {
			t.Fatalf("%s hash did not change with seed", kind)
		}
	}
}

func TestNonclusteredHashIgnoresClusterSetting(t *testing.T) {
	for _, kind := range []DatasetKind{DatasetUniform, DatasetDuplicate} {
		first, err := GenerateDataset(DatasetConfig{Kind: kind, Dimensions: 3, Vectors: 8, Queries: 2, Seed: 4, Clusters: 2, ChunksPerDocument: 2})
		if err != nil {
			t.Fatal(err)
		}
		second, err := GenerateDataset(DatasetConfig{Kind: kind, Dimensions: 3, Vectors: 8, Queries: 2, Seed: 4, Clusters: 99, ChunksPerDocument: 2})
		if err != nil {
			t.Fatal(err)
		}
		if first.Config.Clusters != 0 || second.Config.Clusters != 0 || first.Hash != second.Hash {
			t.Fatalf("%s retained irrelevant cluster provenance: first=%+v second=%+v", kind, first.Config, second.Config)
		}
	}
}

func TestDuplicateDatasetContainsDuplicatesAndHeldOutQueries(t *testing.T) {
	dataset, err := GenerateDataset(DatasetConfig{Kind: DatasetDuplicate, Dimensions: 4, Vectors: 16, Queries: 5, Seed: 9})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dataset.Vectors[0], dataset.Vectors[4]) {
		t.Fatal("duplicate dataset did not repeat its base vectors")
	}
	for queryIndex, query := range dataset.Queries {
		for vectorIndex, value := range dataset.Vectors {
			if slices.Equal(query, value) {
				t.Fatalf("held-out query %d equals stored vector %d", queryIndex, vectorIndex)
			}
		}
	}
}

func TestStrictRecallAtKUsesOrdinalIntersectionAndStrictDenominator(t *testing.T) {
	want := []vector.Hit{{Ordinal: 1}, {Ordinal: 2}, {Ordinal: 3}, {Ordinal: 4}}
	got := []vector.Hit{{Ordinal: 4}, {Ordinal: 2}, {Ordinal: 9}}
	if recall := StrictRecallAtK(got, want, 4); recall != 0.5 {
		t.Fatalf("recall = %v, want 0.5", recall)
	}
	if recall := StrictRecallAtK(got[:1], want, 4); recall != 0.25 {
		t.Fatalf("short result recall = %v, want 0.25", recall)
	}
	if recall := StrictRecallAtK(got, want, 0); recall != 0 {
		t.Fatalf("zero-k recall = %v, want 0", recall)
	}
	if recall := StrictRecallAtK(want[:2], want[:2], 10); recall != 1 {
		t.Fatalf("sparse eligible recall = %v, want 1", recall)
	}
	if recall := StrictRecallAtK(nil, nil, 10); recall != 1 {
		t.Fatalf("empty eligible recall = %v, want 1", recall)
	}
}

func TestDeterministicFilterSelectivityAndNesting(t *testing.T) {
	full, fullReport, err := deterministicFilter(101, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	half, halfReport, err := deterministicFilter(101, 0.5, 7)
	if err != nil {
		t.Fatal(err)
	}
	onePercent, oneReport, err := deterministicFilter(101, 0.01, 7)
	if err != nil {
		t.Fatal(err)
	}
	if full.AllowedOrdinalCount() != 101 || fullReport.EligibleOrdinals != 101 || half.AllowedOrdinalCount() != 51 || halfReport.EligibleOrdinals != 51 || onePercent.AllowedOrdinalCount() != 2 || oneReport.EligibleOrdinals != 2 {
		t.Fatalf("filter cardinalities full=%+v half=%+v one=%+v", fullReport, halfReport, oneReport)
	}
	for ordinal := range 101 {
		if onePercent.Allows(vector.Ordinal(ordinal)) && !half.Allows(vector.Ordinal(ordinal)) {
			t.Fatalf("1%% filter ordinal %d is absent from nested 50%% filter", ordinal)
		}
	}
}

func TestDocumentRecallAndCardinality(t *testing.T) {
	want := []vector.Hit{{Ordinal: 0}, {Ordinal: 1}, {Ordinal: 4}, {Ordinal: 2}}
	got := []vector.Hit{{Ordinal: 1}, {Ordinal: 5}, {Ordinal: 8}}
	if recall := DocumentRecallAtK(got, want, 3, 2); recall != 2.0/3.0 {
		t.Fatalf("document recall = %v, want %v", recall, 2.0/3.0)
	}
	if cardinality := documentCardinality(want, 3, 2); cardinality != 3 {
		t.Fatalf("document cardinality = %d, want 3", cardinality)
	}
}

func TestExactTruthGroupsOverAllEligibleChunks(t *testing.T) {
	reader, err := newFlatReader([][]float32{{0}, {0.1}, {0.2}, {0.3}, {0.4}}, 1, vector.MetricL2Squared, 5)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{K: 3, ChunksPerDocument: 2, FilterSelectivities: []float64{1}, FilterSeed: 1}
	truth, err := exactTruthSweeps(context.Background(), reader, [][]float32{{0}}, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(truth) != 1 || len(truth[0].hits) != 1 || len(truth[0].hits[0]) != 5 {
		t.Fatalf("exact truth did not retain all eligible chunks: %+v", truth)
	}
	if cardinality := documentCardinality(truth[0].hits[0], config.K, config.ChunksPerDocument); cardinality != 3 {
		t.Fatalf("exact grouped cardinality = %d, want 3", cardinality)
	}
}

func TestPercentileNearestRankWithoutMutatingInput(t *testing.T) {
	values := []time.Duration{5, 1, 4, 2, 3}
	original := slices.Clone(values)
	if got := Percentile(values, 0.50); got != 3 {
		t.Fatalf("p50 = %v, want 3", got)
	}
	if got := Percentile(values, 0.95); got != 5 {
		t.Fatalf("p95 = %v, want 5", got)
	}
	if !slices.Equal(values, original) {
		t.Fatalf("percentile mutated input: %v", values)
	}
	if got := Percentile(nil, 0.50); got != 0 {
		t.Fatalf("empty percentile = %v", got)
	}
}

func TestReportJSON(t *testing.T) {
	report := Report{SchemaVersion: SchemaVersion, Benchmark: BenchmarkName, Runs: []RunReport{{
		Dataset: DatasetReport{Kind: DatasetUniform, Hash: "abc", Metric: "l2_squared", Dimensions: 4, VectorCount: 8, QueryCount: 2},
		Request: RequestParameters{K: 2, RequestedEfSearch: 1, EffectiveEfSearch: 2, VisitLimit: 8,
			ResultFilter: FilterParametersReport{RequestedSelectivity: 1, EffectiveSelectivity: 1, EligibleOrdinals: 8, Method: "all_ordinals"}},
		Mode: "hnsw_ann",
	}}}
	var output bytes.Buffer
	if err := WriteJSON(&output, report); err != nil {
		t.Fatal(err)
	}
	var decoded Report
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("invalid report JSON: %v\n%s", err, output.String())
	}
	if decoded.SchemaVersion != SchemaVersion || decoded.Benchmark != BenchmarkName || len(decoded.Runs) != 1 || decoded.Runs[0].Dataset.Hash != "abc" {
		t.Fatalf("decoded report = %+v", decoded)
	}
}

func TestTableDisplaysSubPercentSelectivity(t *testing.T) {
	report := Report{Runs: []RunReport{{
		Dataset: DatasetReport{Kind: DatasetUniform, Metric: "l2_squared"},
		Request: RequestParameters{ResultFilter: FilterParametersReport{EffectiveSelectivity: 0.001}},
	}}}
	var output bytes.Buffer
	if err := WriteTable(&output, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "0.1%") {
		t.Fatalf("table omitted 0.1%% selectivity:\n%s", output.String())
	}
}

func TestSmokeRunner(t *testing.T) {
	config := Config{
		DatasetKinds:        []DatasetKind{DatasetUniform, DatasetClustered, DatasetDuplicate},
		Metrics:             []vector.Metric{vector.MetricL2Squared},
		Dimensions:          4,
		VectorCount:         32,
		QueryCount:          5,
		DatasetSeed:         3,
		Clusters:            3,
		ChunksPerDocument:   2,
		K:                   3,
		MaxNeighbors:        []int{2},
		EfConstruction:      []int{8},
		EfSearch:            []int{1, 8},
		BuildSeeds:          []uint64{7, 11},
		BuildOrders:         []BuildOrder{{Name: "ascending"}},
		FilterSelectivities: []float64{1, 0.5, 0.1, 0.01, 0.001},
		FilterSeed:          5,
		VisitLimit:          32,
	}
	report, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	wantRuns := len(config.DatasetKinds) * len(config.EfSearch) * len(config.BuildSeeds) * len(config.FilterSelectivities)
	if len(report.Runs) != wantRuns {
		t.Fatalf("runs = %d, want %d", len(report.Runs), wantRuns)
	}
	for i, run := range report.Runs {
		if run.Mode != "hnsw_ann" {
			t.Fatalf("run %d did not force HNSW: %+v", i, run)
		}
		if run.BuildPath != BuildPathProduction || run.Dataset.GeneratorVersion != SyntheticGeneratorVersion {
			t.Fatalf("run %d provenance = path %q generator %q", i, run.BuildPath, run.Dataset.GeneratorVersion)
		}
		if run.Request.EffectiveEfSearch != max(config.K, run.Request.RequestedEfSearch) {
			t.Fatalf("run %d efSearch = %+v", i, run.Request)
		}
		if run.GraphStats.VectorCount != config.VectorCount || run.StorageStats.VectorRows != config.VectorCount {
			t.Fatalf("run %d cardinality mismatch: graph=%+v storage=%+v", i, run.GraphStats, run.StorageStats)
		}
		if run.Quality.QueryCount != config.QueryCount || run.Quality.MeanRecallAtK < 0 || run.Quality.MeanRecallAtK > 1 {
			t.Fatalf("run %d quality = %+v", i, run.Quality)
		}
	}

	config.DatasetKinds = config.DatasetKinds[:1]
	config.EfSearch = config.EfSearch[:1]
	config.BuildSeeds = config.BuildSeeds[:1]
	config.FilterSelectivities = []float64{1}
	config.VisitLimit = 1
	limitedReport, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if limitedReport.Runs[0].SearchOutcome.IncompleteRate == 0 || limitedReport.Runs[0].SearchOutcome.VisitLimitTerminationRate == 0 {
		t.Fatalf("visit-limit outcome was not recorded: %+v", limitedReport.Runs[0].SearchOutcome)
	}
}

func TestQualityGateSmokeRunner(t *testing.T) {
	config := Config{
		DatasetKinds:        []DatasetKind{DatasetUniform, DatasetClustered, DatasetDuplicate},
		Metrics:             []vector.Metric{vector.MetricL2Squared, vector.MetricCosine},
		Dimensions:          4,
		VectorCount:         64,
		QueryCount:          8,
		DatasetSeed:         19,
		Clusters:            4,
		ChunksPerDocument:   2,
		K:                   5,
		MaxNeighbors:        []int{8},
		EfConstruction:      []int{32},
		EfSearch:            []int{64},
		BuildSeeds:          []uint64{23},
		BuildOrders:         []BuildOrder{{Name: "ascending"}},
		FilterSelectivities: []float64{1},
		FilterSeed:          7,
		VisitLimit:          64,
	}
	report, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	gates := make([]QualityGate, 0, len(config.DatasetKinds)*len(config.Metrics))
	for _, dataset := range config.DatasetKinds {
		for _, metric := range config.Metrics {
			gates = append(gates, QualityGate{
				Dataset: dataset, Metric: metric.String(), EffectiveEfSearch: 64,
				MinRecallAtK: 0.90, MinDocumentRecallAtK: 0.90, RequireComplete: true,
			})
		}
	}
	if failures := CheckQualityGates(report, gates); len(failures) != 0 {
		for _, failure := range failures {
			t.Errorf("quality gate failed dataset=%s metric=%s ef=%d metric=%s got=%v expected=%v", failure.Run.Dataset.Kind, failure.Run.Dataset.Metric, failure.Run.Request.EffectiveEfSearch, failure.Metric, failure.Got, failure.Expected)
		}
	}
}

func TestShuffledBuildPreservesOriginalOrdinals(t *testing.T) {
	dataset, err := GenerateDataset(DatasetConfig{Kind: DatasetUniform, Dimensions: 3, Vectors: 24, Queries: 2, Seed: 8, ChunksPerDocument: 1})
	if err != nil {
		t.Fatal(err)
	}
	source, err := newFlatReader(dataset.Vectors, 3, vector.MetricL2Squared, 3)
	if err != nil {
		t.Fatal(err)
	}
	buildConfig := hnsw.BuildConfig{
		Dimensions: 3, Metric: vector.MetricL2Squared, MaxVectors: 24, MaxVectorBytes: 24 * 3 * 4,
		MaxNeighbors: 2, EfConstruction: 8, Seed: 4,
	}
	searchConfig := hnsw.SearchConfig{DefaultEfSearch: 3, MaxEfSearch: 8, DefaultVisitLimit: 24, MaxVisitLimit: 24, MaxK: 3}
	reader, path, _, err := buildReader(context.Background(), source, dataset.Vectors, BuildOrder{Name: "shuffled", Seed: 12}, buildConfig, searchConfig, Progress{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if path != BuildPathBuilder {
		t.Fatalf("build path = %q, want %q", path, BuildPathBuilder)
	}
	for ordinal, want := range dataset.Vectors {
		got, ok := readPreparedVector(reader.VectorSource(), vector.Ordinal(ordinal))
		if !ok || !slices.Equal(got, want) {
			t.Fatalf("ordinal %d changed identity: got %v want %v", ordinal, got, want)
		}
	}
}

func TestBuildTimingSeparatesProgressCallback(t *testing.T) {
	dataset, err := GenerateDataset(DatasetConfig{Kind: DatasetUniform, Dimensions: 2, Vectors: 8, Queries: 1, Seed: 2})
	if err != nil {
		t.Fatal(err)
	}
	source, err := newFlatReader(dataset.Vectors, 2, vector.MetricL2Squared, 1)
	if err != nil {
		t.Fatal(err)
	}
	buildConfig := hnsw.BuildConfig{
		Dimensions: 2, Metric: vector.MetricL2Squared, MaxVectors: 8, MaxVectorBytes: 8 * 2 * 4,
		MaxNeighbors: 2, EfConstruction: 4, Seed: 1,
	}
	searchConfig := hnsw.SearchConfig{DefaultEfSearch: 1, MaxEfSearch: 1, DefaultVisitLimit: 8, MaxVisitLimit: 8, MaxK: 1}
	_, path, timing, err := buildReader(context.Background(), source, dataset.Vectors, BuildOrder{Name: "ascending"}, buildConfig, searchConfig, Progress{}, func(Progress) {
		time.Sleep(100 * time.Microsecond)
	})
	if err != nil {
		t.Fatal(err)
	}
	if path != BuildPathProduction || timing.ProgressDuration <= 0 || timing.WallDuration < timing.ProgressDuration || timing.Duration != timing.WallDuration-timing.ProgressDuration {
		t.Fatalf("build timing/path = %q %+v", path, timing)
	}
}

func equalRows(left, right [][]float32) bool {
	return slices.EqualFunc(left, right, func(a, b []float32) bool { return slices.Equal(a, b) })
}
