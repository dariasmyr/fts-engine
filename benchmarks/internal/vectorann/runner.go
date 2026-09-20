package vectorann

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/flat"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

const (
	BuildPathProduction = "hnsw.BuildSearcher"
	BuildPathBuilder    = "hnsw.Builder"
)

type Config struct {
	DatasetKinds        []DatasetKind
	Metrics             []vector.Metric
	Dimensions          int
	VectorCount         int
	QueryCount          int
	DatasetSeed         uint64
	Clusters            int
	ChunksPerDocument   int
	K                   int
	MaxNeighbors        []int
	EfConstruction      []int
	EfSearch            []int
	BuildSeeds          []uint64
	BuildOrders         []BuildOrder
	FilterSelectivities []float64
	FilterSeed          uint64
	VisitLimit          int
	Progress            func(Progress)
}

type Progress struct {
	Dataset        DatasetKind
	Metric         string
	Build          int
	Builds         int
	MaxNeighbors   int
	EfConstruction int
	Seed           uint64
	Order          BuildOrder
	BuildPath      string
	BuildProgress  hnsw.BuildProgress
}

type buildTiming struct {
	Duration         time.Duration
	WallDuration     time.Duration
	ProgressDuration time.Duration
}

type truthSweep struct {
	filter vector.ResultFilter
	report FilterParametersReport
	hits   [][]vector.Hit
}

func DefaultConfig() Config {
	return Config{
		DatasetKinds:        []DatasetKind{DatasetUniform, DatasetClustered, DatasetDuplicate},
		Metrics:             []vector.Metric{vector.MetricL2Squared, vector.MetricCosine},
		Dimensions:          32,
		VectorCount:         1_000,
		QueryCount:          100,
		DatasetSeed:         1,
		Clusters:            8,
		ChunksPerDocument:   4,
		K:                   10,
		MaxNeighbors:        []int{8, 16},
		EfConstruction:      []int{32, 64},
		EfSearch:            []int{16, 32, 64},
		BuildSeeds:          []uint64{1, 2},
		BuildOrders:         []BuildOrder{{Name: "ascending"}, {Name: "shuffled", Seed: 1}},
		FilterSelectivities: []float64{1, 0.5, 0.1, 0.01, 0.001},
		FilterSeed:          1,
		VisitLimit:          1_000,
	}
}

func Run(ctx context.Context, config Config) (Report, error) {
	if ctx == nil {
		return Report{}, vector.ErrNilContext
	}
	if err := validateConfig(config); err != nil {
		return Report{}, err
	}
	report := Report{SchemaVersion: SchemaVersion, Benchmark: BenchmarkName}
	builds := len(config.DatasetKinds) * len(config.Metrics) * len(config.MaxNeighbors) * len(config.EfConstruction) * len(config.BuildSeeds) * len(config.BuildOrders)
	buildNumber := 0
	for _, kind := range config.DatasetKinds {
		dataset, err := GenerateDataset(DatasetConfig{
			Kind: kind, Dimensions: config.Dimensions, Vectors: config.VectorCount,
			Queries: config.QueryCount, Seed: config.DatasetSeed, Clusters: config.Clusters,
			ChunksPerDocument: config.ChunksPerDocument,
		})
		if err != nil {
			return Report{}, err
		}
		for _, metric := range config.Metrics {
			exact, err := newFlatReader(dataset.Vectors, config.Dimensions, metric, config.VectorCount)
			if err != nil {
				return Report{}, fmt.Errorf("vectorann: build flat ground truth: %w", err)
			}
			truth, err := exactTruthSweeps(ctx, exact, dataset.Queries, config)
			if err != nil {
				return Report{}, err
			}
			for _, order := range config.BuildOrders {
				for _, maxNeighbors := range config.MaxNeighbors {
					for _, efConstruction := range config.EfConstruction {
						for _, seed := range config.BuildSeeds {
							buildNumber++
							runs, err := runBuild(ctx, config, dataset, metric, order, exact, truth, maxNeighbors, efConstruction, seed, buildNumber, builds)
							if err != nil {
								return Report{}, err
							}
							report.Runs = append(report.Runs, runs...)
						}
					}
				}
			}
		}
	}
	return report, nil
}

func runBuild(ctx context.Context, config Config, dataset Dataset, metric vector.Metric, order BuildOrder, source *flat.Searcher, truth []truthSweep, maxNeighbors, efConstruction int, seed uint64, buildNumber, builds int) ([]RunReport, error) {
	maxEfSearch := max(config.K, maxSlice(config.EfSearch))
	searchConfig := hnsw.SearchConfig{
		DefaultEfSearch: max(config.K, config.EfSearch[0]), MaxEfSearch: maxEfSearch,
		DefaultVisitLimit: config.VisitLimit, MaxVisitLimit: config.VisitLimit, MaxK: config.K,
	}
	buildConfig := hnsw.BuildConfig{
		Dimensions: config.Dimensions, Metric: metric, MaxVectors: config.VectorCount,
		MaxVectorBytes: uint64(config.VectorCount) * uint64(config.Dimensions) * 4,
		MaxNeighbors:   maxNeighbors, EfConstruction: efConstruction, Seed: seed,
	}
	progress := Progress{
		Dataset: dataset.Config.Kind, Metric: metric.String(), Build: buildNumber, Builds: builds,
		MaxNeighbors: maxNeighbors, EfConstruction: efConstruction, Seed: seed, Order: order,
	}
	reader, buildPath, timing, err := buildReader(ctx, source, dataset.Vectors, order, buildConfig, searchConfig, progress, config.Progress)
	if err != nil {
		return nil, err
	}

	runs := make([]RunReport, 0, len(config.EfSearch)*len(truth))
	for _, requestedEfSearch := range config.EfSearch {
		for _, filteredTruth := range truth {
			run, err := runQueries(ctx, config, dataset, metric, order, buildPath, reader, filteredTruth, buildConfig, searchConfig, timing, requestedEfSearch)
			if err != nil {
				return nil, err
			}
			runs = append(runs, run)
		}
	}
	return runs, nil
}

func buildReader(ctx context.Context, source *flat.Searcher, rawVectors [][]float32, order BuildOrder, buildConfig hnsw.BuildConfig, searchConfig hnsw.SearchConfig, progress Progress, callback func(Progress)) (*hnsw.Searcher, string, buildTiming, error) {
	buildPath := BuildPathProduction
	if order.Name == "shuffled" {
		buildPath = BuildPathBuilder
	}
	progress.BuildPath = buildPath
	var callbackDuration time.Duration
	reportProgress := func(value hnsw.BuildProgress) {
		if callback == nil {
			return
		}
		started := time.Now()
		progress.BuildProgress = value
		callback(progress)
		callbackDuration += time.Since(started)
	}

	started := time.Now()
	var reader *hnsw.Searcher
	var err error
	switch order.Name {
	case "ascending":
		reader, err = hnsw.BuildSearcher(ctx, source.VectorSource(), hnsw.BuildOptions{
			BuildConfig: buildConfig, SearchConfig: searchConfig, Progress: reportProgress,
		})
	case "shuffled":
		reader, err = buildShuffled(ctx, rawVectors, order.Seed, buildConfig, searchConfig, reportProgress)
	default:
		err = fmt.Errorf("vectorann: unknown build order %q", order.Name)
	}
	wallDuration := time.Since(started)
	timing := buildTiming{
		Duration: wallDuration - callbackDuration, WallDuration: wallDuration, ProgressDuration: callbackDuration,
	}
	if timing.Duration < 0 {
		timing.Duration = 0
	}
	if err != nil {
		return nil, buildPath, timing, fmt.Errorf("vectorann: %s build: %w", buildPath, err)
	}
	return reader, buildPath, timing, nil
}

func buildShuffled(ctx context.Context, vectors [][]float32, seed uint64, buildConfig hnsw.BuildConfig, searchConfig hnsw.SearchConfig, progress func(hnsw.BuildProgress)) (*hnsw.Searcher, error) {
	total := len(vectors)
	progress(hnsw.BuildProgress{Phase: hnsw.BuildPhasePreflight, Total: total})
	builder, err := hnsw.NewBuilder(buildConfig, searchConfig, total)
	if err != nil {
		return nil, err
	}
	order := ordinalOrder(total, seed)
	progress(hnsw.BuildProgress{Phase: hnsw.BuildPhaseVectors, Total: total})
	for completed, ordinal := range order {
		if _, err := builder.Add(ctx, vector.Ordinal(ordinal), vectors[ordinal]); err != nil {
			return nil, fmt.Errorf("add original ordinal %d: %w", ordinal, err)
		}
		progress(hnsw.BuildProgress{Phase: hnsw.BuildPhaseVectors, Completed: completed + 1, Total: total})
	}
	progress(hnsw.BuildProgress{Phase: hnsw.BuildPhaseFreeze, Completed: total, Total: total})
	reader, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	progress(hnsw.BuildProgress{Phase: hnsw.BuildPhaseComplete, Completed: total, Total: total})
	return reader, nil
}

func ordinalOrder(count int, seed uint64) []int {
	order := make([]int, count)
	for i := range order {
		order[i] = i
	}
	rng := newRNG(seed, 0x6275696c646f7264)
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}

func runQueries(ctx context.Context, config Config, dataset Dataset, metric vector.Metric, order BuildOrder, buildPath string, reader *hnsw.Searcher, truth truthSweep, buildConfig hnsw.BuildConfig, searchConfig hnsw.SearchConfig, timing buildTiming, requestedEfSearch int) (RunReport, error) {
	effectiveEfSearch := max(config.K, requestedEfSearch)
	latencies := make([]time.Duration, len(dataset.Queries))
	var recall, documentRecall, exactDocuments, annDocuments float64
	var visited, expanded, distances, rejected, incomplete, visitLimit int
	request := vector.SearchOptions{EfSearch: effectiveEfSearch, VisitLimit: config.VisitLimit, ResultFilter: truth.filter}
	for i, query := range dataset.Queries {
		started := time.Now()
		result, err := reader.Search(ctx, query, config.K, request)
		latencies[i] = time.Since(started)
		if err != nil {
			return RunReport{}, fmt.Errorf("vectorann: search query %d: %w", i, err)
		}
		recall += StrictRecallAtK(result.Hits, truth.hits[i], config.K)
		documentRecall += DocumentRecallAtK(result.Hits, truth.hits[i], config.K, config.ChunksPerDocument)
		exactDocuments += float64(documentCardinality(truth.hits[i], config.K, config.ChunksPerDocument))
		annDocuments += float64(documentCardinality(result.Hits, config.K, config.ChunksPerDocument))
		visited += result.Stats.VisitedNodes
		expanded += result.Stats.ExpandedNodes
		distances += result.Stats.DistanceComputations
		rejected += result.Stats.RejectedNodes
		if result.Incomplete {
			incomplete++
		}
		if result.Stats.Termination == vector.TerminationVisitLimit {
			visitLimit++
		}
	}
	count := float64(len(dataset.Queries))
	graph := reader.GraphStats()
	storage := reader.StorageStats()
	buildInfo := reader.BuildInfo()
	run := RunReport{
		Dataset: DatasetReport{
			Kind: dataset.Config.Kind, Hash: dataset.Hash, Metric: metric.String(), Dimensions: dataset.Config.Dimensions,
			VectorCount: len(dataset.Vectors), QueryCount: len(dataset.Queries), Seed: dataset.Config.Seed, Clusters: dataset.Config.Clusters,
			GeneratorVersion: SyntheticGeneratorVersion, ChunksPerDocument: dataset.Config.ChunksPerDocument,
			DocumentCount: (len(dataset.Vectors) + dataset.Config.ChunksPerDocument - 1) / dataset.Config.ChunksPerDocument,
		},
		BuildOrder: order,
		BuildPath:  buildPath,
		BuildParameters: BuildParameters{
			Dimensions: buildConfig.Dimensions, Metric: buildConfig.Metric.String(), MaxVectors: buildConfig.MaxVectors,
			MaxVectorBytes: buildConfig.MaxVectorBytes, MaxNeighbors: buildConfig.MaxNeighbors,
			EfConstruction: buildConfig.EfConstruction, Seed: buildConfig.Seed,
		},
		BuildInfo: BuildInfoReport{
			BuildVersion: buildInfo.BuildVersion, LevelGeneratorVersion: buildInfo.LevelGeneratorVersion,
			MaxNeighbors: buildInfo.MaxNeighbors, LevelZeroMaxNeighbors: buildInfo.LevelZeroMaxNeighbors,
			EfConstruction: buildInfo.EfConstruction, Seed: buildInfo.Seed,
		},
		SearchParameters: SearchParameters{
			DefaultEfSearch: searchConfig.DefaultEfSearch, MaxEfSearch: searchConfig.MaxEfSearch,
			DefaultVisitLimit: searchConfig.DefaultVisitLimit, MaxVisitLimit: searchConfig.MaxVisitLimit, MaxK: searchConfig.MaxK,
		},
		Request: RequestParameters{
			K: config.K, RequestedEfSearch: requestedEfSearch, EffectiveEfSearch: effectiveEfSearch,
			VisitLimit: config.VisitLimit, ResultFilter: truth.report,
		},
		GraphStats: GraphStatsReport{
			NodeCount: graph.NodeCount, VectorCount: graph.VectorCount, MaxLevel: graph.MaxLevel,
			LevelNodeCounts: graph.LevelNodeCounts, LevelLinkCounts: graph.LevelLinkCounts,
			ReachableNodes: graph.ReachableNodes, UnreachableNodes: graph.UnreachableNodes, ZeroDegreeNodes: graph.ZeroDegreeNodes,
		},
		StorageStats: StorageStatsReport{
			VectorRows: storage.VectorRows, GraphNodes: storage.GraphNodes, LevelPlacements: storage.LevelPlacements,
			DirectedLinks: storage.DirectedLinks, VectorBytes: storage.VectorBytes, NodeMetadataBytes: storage.NodeMetadataBytes,
			OffsetBytes: storage.OffsetBytes, LinkBytes: storage.LinkBytes, TotalBytes: storage.TotalBytes,
		},
		BuildDurationNS: timing.Duration.Nanoseconds(), BuildWallDurationNS: timing.WallDuration.Nanoseconds(),
		ProgressCallbackDurationNS: timing.ProgressDuration.Nanoseconds(),
		Latency: LatencyReport{
			P50NS: Percentile(latencies, 0.50).Nanoseconds(), P95NS: Percentile(latencies, 0.95).Nanoseconds(),
			P99NS: Percentile(latencies, 0.99).Nanoseconds(),
		},
		Quality: QualityReport{
			K: config.K, QueryCount: len(dataset.Queries), MeanRecallAtK: recall / count,
			MeanDocumentRecallAtK: documentRecall / count, MeanExactDocumentCardinalityAtK: exactDocuments / count,
			MeanANNDocumentCardinalityAtK: annDocuments / count,
		},
		SearchWork: SearchWorkReport{
			AverageVisitedNodes: float64(visited) / count, AverageExpandedNodes: float64(expanded) / count,
			AverageDistanceComputations: float64(distances) / count, AverageRejectedNodes: float64(rejected) / count,
		},
		SearchOutcome: SearchOutcomeReport{
			IncompleteRate: float64(incomplete) / count, VisitLimitTerminationRate: float64(visitLimit) / count,
		},
		Mode: "hnsw_ann",
	}
	return run, nil
}

func newFlatReader(values [][]float32, dimensions int, metric vector.Metric, k int) (*flat.Searcher, error) {
	index, err := flat.New(flat.Config{Dimensions: dimensions, Metric: metric, MaxVectors: len(values), MaxK: k})
	if err != nil {
		return nil, err
	}
	if _, err := index.AppendBatch(values); err != nil {
		return nil, err
	}
	return index.Freeze(), nil
}

func exactTruthSweeps(ctx context.Context, reader *flat.Searcher, queries [][]float32, config Config) ([]truthSweep, error) {
	truth := make([]truthSweep, 0, len(config.FilterSelectivities))
	for _, selectivity := range config.FilterSelectivities {
		filter, report, err := deterministicFilter(reader.Len(), selectivity, config.FilterSeed)
		if err != nil {
			return nil, err
		}
		hits := make([][]vector.Hit, len(queries))
		for i, query := range queries {
			result, err := reader.Search(ctx, query, reader.Len(), vector.SearchOptions{ResultFilter: filter})
			if err != nil {
				return nil, fmt.Errorf("vectorann: flat query %d at selectivity %g: %w", i, selectivity, err)
			}
			hits[i] = result.Hits
		}
		truth = append(truth, truthSweep{filter: filter, report: report, hits: hits})
	}
	return truth, nil
}

func deterministicFilter(count int, selectivity float64, seed uint64) (vector.ResultFilter, FilterParametersReport, error) {
	eligible := min(count, max(1, int(math.Ceil(float64(count)*selectivity))))
	report := FilterParametersReport{
		RequestedSelectivity: selectivity, EffectiveSelectivity: float64(eligible) / float64(count),
		EligibleOrdinals: eligible, Seed: seed, Method: "deterministic_ordinal_permutation_prefix",
	}
	if eligible == count {
		report.Seed = 0
		report.Method = "all_ordinals"
		filter := vector.NewFullBitSet(uint32(count))
		return filter, report, nil
	}
	order := ordinalOrder(count, seed^0x66696c746572)
	allowed := make([]vector.Ordinal, eligible)
	for i, ordinal := range order[:eligible] {
		allowed[i] = vector.Ordinal(ordinal)
	}
	filter, err := vector.NewBitSet(uint32(count), allowed...)
	if err != nil {
		return nil, FilterParametersReport{}, fmt.Errorf("vectorann: construct result filter: %w", err)
	}
	return filter, report, nil
}

func DocumentRecallAtK(got, want []vector.Hit, k, chunksPerDocument int) float64 {
	if k <= 0 || chunksPerDocument <= 0 {
		return 0
	}
	wantDocuments := topDocumentSet(want, k, chunksPerDocument)
	if len(wantDocuments) == 0 {
		return 1
	}
	gotDocuments := topDocumentSet(got, k, chunksPerDocument)
	matches := 0
	for document := range wantDocuments {
		if _, ok := gotDocuments[document]; ok {
			matches++
		}
	}
	return float64(matches) / float64(len(wantDocuments))
}

func documentCardinality(hits []vector.Hit, k, chunksPerDocument int) int {
	return len(topDocumentSet(hits, k, chunksPerDocument))
}

// topDocumentSet groups the distance-ordered chunk stream by the best chunk per
// document, then retains the first k documents.
func topDocumentSet(hits []vector.Hit, k, chunksPerDocument int) map[int]struct{} {
	documents := make(map[int]struct{}, min(k, len(hits)))
	for _, hit := range hits {
		documents[int(hit.Ordinal)/chunksPerDocument] = struct{}{}
		if len(documents) == k {
			break
		}
	}
	return documents
}

func validateConfig(config Config) error {
	if configListsEmpty(config) {
		return fmt.Errorf("vectorann: dataset, metric, filter, and parameter sweeps must not be empty")
	}
	if config.Dimensions <= 0 || config.VectorCount <= 0 || config.QueryCount <= 0 || config.K <= 0 || config.K > config.VectorCount {
		return fmt.Errorf("vectorann: invalid dimensions, counts, or k")
	}
	if config.ChunksPerDocument <= 0 {
		return fmt.Errorf("vectorann: chunks per document must be positive")
	}
	if config.VisitLimit <= 0 || config.VisitLimit > config.VectorCount {
		return fmt.Errorf("vectorann: visit limit must be in [1, vector count]")
	}
	for _, metric := range config.Metrics {
		if !metric.Valid() {
			return fmt.Errorf("vectorann: invalid metric %d", metric)
		}
	}
	for _, order := range config.BuildOrders {
		if order.Name != "ascending" && order.Name != "shuffled" {
			return fmt.Errorf("vectorann: unknown build order %q", order.Name)
		}
	}
	for _, maxNeighbors := range config.MaxNeighbors {
		for _, efConstruction := range config.EfConstruction {
			if maxNeighbors < 2 || efConstruction < maxNeighbors {
				return fmt.Errorf("vectorann: ef construction must be >= max neighbors >= 2")
			}
		}
	}
	for _, efSearch := range config.EfSearch {
		if efSearch <= 0 {
			return fmt.Errorf("vectorann: requested ef search must be positive")
		}
	}
	for _, selectivity := range config.FilterSelectivities {
		if selectivity <= 0 || selectivity > 1 || math.IsNaN(selectivity) {
			return fmt.Errorf("vectorann: filter selectivity must be in (0, 1]")
		}
	}
	return nil
}

func configListsEmpty(config Config) bool {
	return len(config.DatasetKinds) == 0 || len(config.Metrics) == 0 || len(config.MaxNeighbors) == 0 ||
		len(config.EfConstruction) == 0 || len(config.EfSearch) == 0 || len(config.BuildSeeds) == 0 ||
		len(config.BuildOrders) == 0 || len(config.FilterSelectivities) == 0
}

func maxSlice(values []int) int {
	maximum := values[0]
	for _, value := range values[1:] {
		maximum = max(maximum, value)
	}
	return maximum
}
