package vectorsearch

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

const (
	SchemaVersion = "vector-ann.synthetic-phase6-baseline.v3"
	BenchmarkName = "synthetic_phase6_baseline"
)

type Report struct {
	SchemaVersion string      `json:"schema_version"`
	Benchmark     string      `json:"benchmark"`
	Runs          []RunReport `json:"runs"`
}

type RunReport struct {
	Dataset                    DatasetReport       `json:"dataset"`
	BuildOrder                 BuildOrder          `json:"build_order"`
	BuildPath                  string              `json:"build_path"`
	BuildParameters            BuildParameters     `json:"build_parameters"`
	BuildInfo                  BuildInfoReport     `json:"build_info"`
	SearchParameters           SearchParameters    `json:"search_parameters"`
	Request                    RequestParameters   `json:"request_parameters"`
	GraphStats                 GraphStatsReport    `json:"graph_stats"`
	StorageStats               StorageStatsReport  `json:"storage_stats"`
	BuildDurationNS            int64               `json:"build_duration_ns"`
	BuildWallDurationNS        int64               `json:"build_wall_duration_ns"`
	ProgressCallbackDurationNS int64               `json:"progress_callback_duration_ns"`
	Latency                    LatencyReport       `json:"latency"`
	Quality                    QualityReport       `json:"quality"`
	SearchWork                 SearchWorkReport    `json:"search_work"`
	SearchOutcome              SearchOutcomeReport `json:"search_outcome"`
	Mode                       string              `json:"mode"`
}

type DatasetReport struct {
	Kind              DatasetKind `json:"kind"`
	Hash              string      `json:"hash"`
	Metric            string      `json:"metric"`
	Dimensions        int         `json:"dimensions"`
	VectorCount       int         `json:"vector_count"`
	QueryCount        int         `json:"query_count"`
	Seed              uint64      `json:"seed"`
	Clusters          int         `json:"clusters,omitempty"`
	GeneratorVersion  string      `json:"generator_version"`
	ChunksPerDocument int         `json:"chunks_per_document"`
	DocumentCount     int         `json:"document_count"`
}

type BuildOrder struct {
	Name string `json:"name"`
	Seed uint64 `json:"seed,omitempty"`
}

type BuildParameters struct {
	Dimensions     int    `json:"dimensions"`
	Metric         string `json:"metric"`
	MaxVectors     int    `json:"max_vectors"`
	MaxVectorBytes uint64 `json:"max_vector_bytes"`
	MaxNeighbors   int    `json:"max_neighbors"`
	EfConstruction int    `json:"ef_construction"`
	Seed           uint64 `json:"seed"`
}

type BuildInfoReport struct {
	BuildVersion          uint32 `json:"build_version"`
	LevelGeneratorVersion uint32 `json:"level_generator_version"`
	MaxNeighbors          int    `json:"max_neighbors"`
	LevelZeroMaxNeighbors int    `json:"level_zero_max_neighbors"`
	EfConstruction        int    `json:"ef_construction"`
	Seed                  uint64 `json:"seed"`
}

type SearchParameters struct {
	DefaultEfSearch   int `json:"default_ef_search"`
	MaxEfSearch       int `json:"max_ef_search"`
	DefaultVisitLimit int `json:"default_visit_limit"`
	MaxVisitLimit     int `json:"max_visit_limit"`
	MaxK              int `json:"max_k"`
}

type RequestParameters struct {
	K                 int                    `json:"k"`
	RequestedEfSearch int                    `json:"requested_ef_search"`
	EffectiveEfSearch int                    `json:"effective_ef_search"`
	VisitLimit        int                    `json:"visit_limit"`
	ResultFilter      FilterParametersReport `json:"result_filter"`
}

type FilterParametersReport struct {
	RequestedSelectivity float64 `json:"requested_selectivity"`
	EffectiveSelectivity float64 `json:"effective_selectivity"`
	EligibleOrdinals     int     `json:"eligible_ordinals"`
	Seed                 uint64  `json:"seed"`
	Method               string  `json:"method"`
}

type GraphStatsReport struct {
	NodeCount        int   `json:"node_count"`
	VectorCount      int   `json:"vector_count"`
	MaxLevel         int   `json:"max_level"`
	LevelNodeCounts  []int `json:"level_node_counts"`
	LevelLinkCounts  []int `json:"level_link_counts"`
	ReachableNodes   int   `json:"reachable_nodes"`
	UnreachableNodes int   `json:"unreachable_nodes"`
	ZeroDegreeNodes  int   `json:"zero_degree_nodes"`
}

type StorageStatsReport struct {
	VectorRows        int    `json:"vector_rows"`
	GraphNodes        int    `json:"graph_nodes"`
	LevelPlacements   int    `json:"level_placements"`
	DirectedLinks     int    `json:"directed_links"`
	VectorBytes       uint64 `json:"vector_bytes"`
	NodeMetadataBytes uint64 `json:"node_metadata_bytes"`
	OffsetBytes       uint64 `json:"offset_bytes"`
	LinkBytes         uint64 `json:"link_bytes"`
	TotalBytes        uint64 `json:"total_bytes"`
}

type LatencyReport struct {
	P50NS int64 `json:"p50_ns"`
	P95NS int64 `json:"p95_ns"`
	P99NS int64 `json:"p99_ns"`
}

type QualityReport struct {
	K                               int     `json:"k"`
	QueryCount                      int     `json:"query_count"`
	MeanRecallAtK                   float64 `json:"mean_recall_at_k"`
	MeanDocumentRecallAtK           float64 `json:"mean_document_recall_at_k"`
	MeanExactDocumentCardinalityAtK float64 `json:"mean_exact_document_cardinality_at_k"`
	MeanANNDocumentCardinalityAtK   float64 `json:"mean_ann_document_cardinality_at_k"`
}

type SearchWorkReport struct {
	AverageVisitedNodes         float64 `json:"average_visited_nodes"`
	AverageExpandedNodes        float64 `json:"average_expanded_nodes"`
	AverageDistanceComputations float64 `json:"average_distance_computations"`
	AverageRejectedNodes        float64 `json:"average_rejected_nodes"`
}

type SearchOutcomeReport struct {
	IncompleteRate            float64 `json:"incomplete_rate"`
	VisitLimitTerminationRate float64 `json:"visit_limit_termination_rate"`
}

func StrictRecallAtK(got, want []vector.Hit, k int) float64 {
	if k <= 0 {
		return 0
	}
	gotOrdinals := make(map[vector.Ordinal]struct{}, min(k, len(got)))
	for _, hit := range got[:min(k, len(got))] {
		gotOrdinals[hit.Ordinal] = struct{}{}
	}
	denominator := min(k, len(want))
	if denominator == 0 {
		return 1
	}
	matches := 0
	for _, hit := range want[:denominator] {
		if _, ok := gotOrdinals[hit.Ordinal]; ok {
			matches++
		}
	}
	return float64(matches) / float64(denominator)
}

func Percentile(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	if percentile <= 0 {
		return ordered[0]
	}
	if percentile >= 1 {
		return ordered[len(ordered)-1]
	}
	index := int(math.Ceil(percentile*float64(len(ordered)))) - 1
	return ordered[index]
}

func WriteJSON(writer io.Writer, report Report) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func WriteTable(writer io.Writer, report Report) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "DATASET\tMETRIC\tORDER/PATH\tFILTER\tM\tEFC\tEFS(req/eff)\tSEED\tRECALL@K\tDOC-RECALL\tP50(us)\tP95(us)\tP99(us)\tVISITED\tDISTANCES\tINCOMPLETE"); err != nil {
		return err
	}
	for _, run := range report.Runs {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s/%s\t%.1f%%\t%d\t%d\t%d/%d\t%d\t%.4f\t%.4f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f%%\n",
			run.Dataset.Kind, run.Dataset.Metric, run.BuildOrder.Name, run.BuildPath,
			run.Request.ResultFilter.EffectiveSelectivity*100, run.BuildParameters.MaxNeighbors,
			run.BuildParameters.EfConstruction, run.Request.RequestedEfSearch, run.Request.EffectiveEfSearch, run.BuildParameters.Seed,
			run.Quality.MeanRecallAtK, run.Quality.MeanDocumentRecallAtK,
			float64(run.Latency.P50NS)/1e3, float64(run.Latency.P95NS)/1e3,
			float64(run.Latency.P99NS)/1e3, run.SearchWork.AverageVisitedNodes, run.SearchWork.AverageDistanceComputations, run.SearchOutcome.IncompleteRate*100); err != nil {
			return err
		}
	}
	return table.Flush()
}
