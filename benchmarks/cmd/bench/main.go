package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/benchmarks/adapters/mock"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/dataset"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/harness"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/metrics"
	"github.com/dariasmyr/fts-engine/benchmarks/internal/quality"
)

type config struct {
	Engines       []string
	Dataset       string
	K             int
	Out           string
	Work          string
	Batch         int
	Warmup        float64
	Concurrency   int
	Seed          uint64
	SynthDocs     int
	SynthQueries  int
	WordsPerDoc   int
	WordsPerQuery int
	VocabSize     int
	ZipfS         float64
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		engines       = fs.String("engines", "mock", "comma-separated engine list")
		dataset = fs.String("dataset", "synthetic", "dataset name")
		k             = fs.Int("k", 10, "top-k results")
		out           = fs.String("out", "", "JSON output path")
		work          = fs.String("work", "./work", "work directory for engine state")
		batch         = fs.Int("batch", 1000, "indexing batch size")
		warmup        = fs.Float64("warmup", 0.10, "warmup fraction in [0, 0.5]")
		concurrency   = fs.Int("concurrency", 1, "search concurrency")
		seed          = fs.Uint64("seed", 0xC0FFEE, "deterministic seed")
		synthDocs     = fs.Int("synth-docs", 5000, "synthetic document count")
		synthQueries  = fs.Int("synth-queries", 500, "synthetic query count")
		wordsPerDoc   = fs.Int("words-per-doc", 60, "synthetic words per document")
		wordsPerQuery = fs.Int("words-per-query", 3, "synthetic words per query")
		vocabSize     = fs.Int("vocab-size", 20000, "synthetic vocabulary size")
		zipfS         = fs.Float64("zipf-s", 1.07, "synthetic Zipf exponent")
	)

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := config{
		Engines:       splitCSV(*engines),
		Dataset:       strings.TrimSpace(*dataset),
		K:             *k,
		Out:           strings.TrimSpace(*out),
		Work:          strings.TrimSpace(*work),
		Batch:         *batch,
		Warmup:        *warmup,
		Concurrency:   *concurrency,
		Seed:          *seed,
		SynthDocs:     *synthDocs,
		SynthQueries:  *synthQueries,
		WordsPerDoc:   *wordsPerDoc,
		WordsPerQuery: *wordsPerQuery,
		VocabSize:     *vocabSize,
		ZipfS:         *zipfS,
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func (c config) validate() error {
	if len(c.Engines) == 0 {
		return errors.New("bench: at least one engine must be provided via -engines")
	}
	if c.Dataset == "" {
		return errors.New("bench: -dataset must not be empty")
	}
	if c.K <= 0 {
		return fmt.Errorf("bench: -k must be > 0, got %d", c.K)
	}
	if c.Batch <= 0 {
		return fmt.Errorf("bench: -batch must be > 0, got %d", c.Batch)
	}
	if c.Warmup < 0 || c.Warmup > 0.5 {
		return fmt.Errorf("bench: -warmup must be in [0, 0.5], got %g", c.Warmup)
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("bench: -concurrency must be > 0, got %d", c.Concurrency)
	}
	if c.Work == "" {
		return errors.New("bench: -work must not be empty")
	}
	if c.Dataset != "synthetic" {
		return fmt.Errorf("bench: unsupported dataset %q in iteration 2", c.Dataset)
	}
	if c.SynthDocs <= 0 {
		return fmt.Errorf("bench: -synth-docs must be > 0, got %d", c.SynthDocs)
	}
	if c.SynthQueries <= 0 {
		return fmt.Errorf("bench: -synth-queries must be > 0, got %d", c.SynthQueries)
	}
	if c.WordsPerDoc <= 0 {
		return fmt.Errorf("bench: -words-per-doc must be > 0, got %d", c.WordsPerDoc)
	}
	if c.WordsPerQuery <= 0 {
		return fmt.Errorf("bench: -words-per-query must be > 0, got %d", c.WordsPerQuery)
	}
	if c.VocabSize <= 1 {
		return fmt.Errorf("bench: -vocab-size must be > 1, got %d", c.VocabSize)
	}
	if c.ZipfS <= 1 {
		return fmt.Errorf("bench: -zipf-s must be > 1, got %g", c.ZipfS)
	}
	return nil
}

func run(cfg config) error {
	ctx := context.Background()
	corpus, err := loadDataset(cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.Work, 0o755); err != nil {
		return fmt.Errorf("bench: create work dir: %w", err)
	}

	records := make([]metrics.Record, 0, len(cfg.Engines))
	for _, engineName := range cfg.Engines {
		eng, err := buildEngine(engineName)
		if err != nil {
			return err
		}

		rep, err := harness.Run(ctx, eng, corpus.Docs, corpus.Queries, harness.RunConfig{
			Dir:         filepath.Join(cfg.Work, engineName),
			BatchSize:   cfg.Batch,
			WarmupFrac:  cfg.Warmup,
			Concurrency: cfg.Concurrency,
			Seed:        cfg.Seed,
		})
		if err != nil {
			return fmt.Errorf("bench: run %s: %w", engineName, err)
		}

		qs := quality.Compute(rep.QueryResults, corpus.Qrels, cfg.K)
		records = append(records, metrics.Build(rep, qs, metrics.RunMeta{
			Dataset:     cfg.Dataset,
			Concurrency: cfg.Concurrency,
		}))
	}

	if cfg.Out != "" {
		if err := writeJSON(cfg.Out, records); err != nil {
			return err
		}
	}

	return metrics.WriteTable(os.Stdout, records)
}

func loadDataset(cfg config) (*dataset.Corpus, error) {
	if cfg.Dataset != "synthetic" {
		return nil, fmt.Errorf("bench: unsupported dataset %q", cfg.Dataset)
	}
	return dataset.Synthetic(dataset.SyntheticConfig{
		NumDocs:       cfg.SynthDocs,
		NumQueries:    cfg.SynthQueries,
		WordsPerDoc:   cfg.WordsPerDoc,
		WordsPerQuery: cfg.WordsPerQuery,
		VocabSize:     cfg.VocabSize,
		ZipfS:         cfg.ZipfS,
		K:             cfg.K,
		Seed:          cfg.Seed,
	}), nil
}

func buildEngine(name string) (harness.Engine, error) {
	switch name {
	case "mock":
		return mock.New(), nil
	default:
		return nil, fmt.Errorf("bench: unknown engine %q", name)
	}
}

func writeJSON(path string, records []metrics.Record) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("bench: create output dir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("bench: create output file: %w", err)
	}
	defer f.Close()
	if err := metrics.WriteJSON(f, records); err != nil {
		return fmt.Errorf("bench: write JSON: %w", err)
	}
	return nil
}

func splitCSV(input string) []string {
	parts := strings.Split(input, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}
