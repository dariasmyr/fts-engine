package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	vectorann "github.com/dariasmyr/fts-engine/benchmarks/internal/vectorsearch"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	config, format, output, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	lastProgress := make(map[int]string)
	config.Progress = func(progress vectorann.Progress) {
		production := progress.BuildProgress
		bucket := 0
		if production.Total > 0 {
			bucket = production.Completed * 10 / production.Total
		}
		state := fmt.Sprintf("%s/%d", production.Phase, bucket)
		if lastProgress[progress.Build] == state && production.Phase == hnsw.BuildPhaseVectors {
			return
		}
		lastProgress[progress.Build] = state
		fmt.Fprintf(stderr, "build %d/%d dataset=%s metric=%s order=%s path=%s M=%d efC=%d seed=%d phase=%s %d/%d\n",
			progress.Build, progress.Builds, progress.Dataset, progress.Metric, progress.Order.Name,
			progress.BuildPath, progress.MaxNeighbors, progress.EfConstruction, progress.Seed,
			production.Phase, production.Completed, production.Total)
	}

	report, err := vectorann.Run(ctx, config)
	if err != nil {
		return err
	}
	if output != "" {
		if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
			return fmt.Errorf("vector-search: create JSON output directory: %w", err)
		}
		file, err := os.Create(output)
		if err != nil {
			return fmt.Errorf("vector-search: create JSON output: %w", err)
		}
		if err := vectorann.WriteJSON(file, report); err != nil {
			_ = file.Close()
			return fmt.Errorf("vector-search: write JSON output: %w", err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("vector-search: close JSON output: %w", err)
		}
	}
	if format == "json" {
		return vectorann.WriteJSON(stdout, report)
	}
	return vectorann.WriteTable(stdout, report)
}

func parseFlags(args []string, stderr io.Writer) (vectorann.Config, string, string, error) {
	defaults := vectorann.DefaultConfig()
	flags := flag.NewFlagSet("vector-search", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Synthetic Phase 6 ANN baseline (not the complete real-corpus quality gate).")
		fmt.Fprintf(stderr, "Usage: %s [flags]\n", flags.Name())
		flags.PrintDefaults()
	}
	datasets := flags.String("datasets", joinKinds(defaults.DatasetKinds), "comma-separated uniform,clustered,duplicate datasets")
	metrics := flags.String("metrics", "l2_squared,cosine", "comma-separated l2_squared,cosine metrics")
	dimensions := flags.Int("dimensions", defaults.Dimensions, "vector dimensions")
	vectors := flags.Int("vectors", defaults.VectorCount, "stored vector count")
	queries := flags.Int("queries", defaults.QueryCount, "held-out query count")
	datasetSeed := flags.Uint64("dataset-seed", defaults.DatasetSeed, "dataset generation seed")
	clusters := flags.Int("clusters", defaults.Clusters, "cluster count for clustered data")
	chunksPerDocument := flags.Int("chunks-per-document", defaults.ChunksPerDocument, "contiguous synthetic chunks assigned to each document")
	k := flags.Int("k", defaults.K, "strict top-k recall depth")
	neighbors := flags.String("max-neighbors", joinInts(defaults.MaxNeighbors), "comma-separated MaxNeighbors sweep")
	efConstruction := flags.String("ef-construction", joinInts(defaults.EfConstruction), "comma-separated EfConstruction sweep")
	efSearch := flags.String("ef-search", joinInts(defaults.EfSearch), "comma-separated EfSearch sweep")
	seeds := flags.String("build-seeds", joinUint64s(defaults.BuildSeeds), "comma-separated HNSW build seeds")
	orders := flags.String("build-orders", "ascending,shuffled", "comma-separated ascending,shuffled build orders")
	shuffleSeed := flags.Uint64("shuffle-seed", 1, "shuffled build-order seed")
	filterSelectivities := flags.String("filter-selectivities", joinFloat64s(defaults.FilterSelectivities), "comma-separated result-filter selectivities")
	filterSeed := flags.Uint64("filter-seed", defaults.FilterSeed, "deterministic result-filter seed")
	visitLimit := flags.Int("visit-limit", 0, "request visit limit (0 = vector count)")
	format := flags.String("format", "table", "stdout format: table or json")
	output := flags.String("out", "", "optional JSON output file")
	if err := flags.Parse(args); err != nil {
		return vectorann.Config{}, "", "", err
	}
	if flags.NArg() != 0 {
		return vectorann.Config{}, "", "", fmt.Errorf("vector-search: unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}

	config := defaults
	var err error
	config.DatasetKinds, err = parseKinds(*datasets)
	if err != nil {
		return vectorann.Config{}, "", "", err
	}
	config.Metrics, err = parseMetrics(*metrics)
	if err != nil {
		return vectorann.Config{}, "", "", err
	}
	config.MaxNeighbors, err = parseInts(*neighbors)
	if err != nil {
		return vectorann.Config{}, "", "", fmt.Errorf("vector-search: max-neighbors: %w", err)
	}
	config.EfConstruction, err = parseInts(*efConstruction)
	if err != nil {
		return vectorann.Config{}, "", "", fmt.Errorf("vector-search: ef-construction: %w", err)
	}
	config.EfSearch, err = parseInts(*efSearch)
	if err != nil {
		return vectorann.Config{}, "", "", fmt.Errorf("vector-search: ef-search: %w", err)
	}
	config.BuildSeeds, err = parseUint64s(*seeds)
	if err != nil {
		return vectorann.Config{}, "", "", fmt.Errorf("vector-search: build-seeds: %w", err)
	}
	config.BuildOrders, err = parseOrders(*orders, *shuffleSeed)
	if err != nil {
		return vectorann.Config{}, "", "", err
	}
	config.FilterSelectivities, err = parseFloat64s(*filterSelectivities)
	if err != nil {
		return vectorann.Config{}, "", "", fmt.Errorf("vector-search: filter-selectivities: %w", err)
	}
	config.Dimensions = *dimensions
	config.VectorCount = *vectors
	config.QueryCount = *queries
	config.DatasetSeed = *datasetSeed
	config.Clusters = *clusters
	config.ChunksPerDocument = *chunksPerDocument
	config.FilterSeed = *filterSeed
	config.K = *k
	config.VisitLimit = *visitLimit
	if config.VisitLimit == 0 {
		config.VisitLimit = config.VectorCount
	}
	if *format != "table" && *format != "json" {
		return vectorann.Config{}, "", "", fmt.Errorf("vector-search: unknown format %q", *format)
	}
	return config, *format, strings.TrimSpace(*output), nil
}

func parseKinds(value string) ([]vectorann.DatasetKind, error) {
	parts := splitCSV(value)
	kinds := make([]vectorann.DatasetKind, 0, len(parts))
	for _, part := range parts {
		kind := vectorann.DatasetKind(part)
		switch kind {
		case vectorann.DatasetUniform, vectorann.DatasetClustered, vectorann.DatasetDuplicate:
			kinds = append(kinds, kind)
		default:
			return nil, fmt.Errorf("vector-search: unknown dataset %q", part)
		}
	}
	return kinds, nil
}

func parseMetrics(value string) ([]vector.Metric, error) {
	parts := splitCSV(value)
	metrics := make([]vector.Metric, 0, len(parts))
	for _, part := range parts {
		switch part {
		case "l2", "l2_squared":
			metrics = append(metrics, vector.MetricL2Squared)
		case "cosine":
			metrics = append(metrics, vector.MetricCosine)
		default:
			return nil, fmt.Errorf("vector-search: unknown metric %q", part)
		}
	}
	return metrics, nil
}

func parseOrders(value string, shuffleSeed uint64) ([]vectorann.BuildOrder, error) {
	parts := splitCSV(value)
	orders := make([]vectorann.BuildOrder, 0, len(parts))
	for _, part := range parts {
		switch part {
		case "ascending":
			orders = append(orders, vectorann.BuildOrder{Name: part})
		case "shuffled":
			orders = append(orders, vectorann.BuildOrder{Name: part, Seed: shuffleSeed})
		default:
			return nil, fmt.Errorf("vector-search: unknown build order %q", part)
		}
	}
	return orders, nil
}

func parseInts(value string) ([]int, error) {
	parts := splitCSV(value)
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		values = append(values, parsed)
	}
	return values, nil
}

func parseUint64s(value string) ([]uint64, error) {
	parts := splitCSV(value)
	values := make([]uint64, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, err
		}
		values = append(values, parsed)
	}
	return values, nil
}

func parseFloat64s(value string) ([]float64, error) {
	parts := splitCSV(value)
	values := make([]float64, 0, len(parts))
	for _, part := range parts {
		parsed, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return nil, err
		}
		values = append(values, parsed)
	}
	return values, nil
}

func splitCSV(value string) []string {
	var parts []string
	for _, part := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func joinKinds(values []vectorann.DatasetKind) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = string(value)
	}
	return strings.Join(parts, ",")
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}

func joinUint64s(values []uint64) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.FormatUint(value, 10)
	}
	return strings.Join(parts, ",")
}

func joinFloat64s(values []float64) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.FormatFloat(value, 'g', -1, 64)
	}
	return strings.Join(parts, ",")
}
