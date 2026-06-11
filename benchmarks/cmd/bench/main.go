package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/benchmarks/adapters/bleve"
	"github.com/dariasmyr/fts-engine/benchmarks/adapters/bluge"
	benchftsengine "github.com/dariasmyr/fts-engine/benchmarks/adapters/ftsengine"
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
	MSMARCODir    string
	MaxDocs       int
	MaxQueries    int
	SynthDocs     int
	SynthQueries  int
	WordsPerDoc   int
	WordsPerQuery int
	VocabSize     int
	ZipfS         float64
	Index         string
	Scorer        string
	Lang          string
	Filter        string
	Persist       string
	Diagnostics   bool
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
		engines       = fs.String("engines", "fts-engine", "comma-separated engine list")
		dataset       = fs.String("dataset", "synthetic", "dataset name")
		k             = fs.Int("k", 10, "top-k results")
		out           = fs.String("out", "", "JSON output path")
		work          = fs.String("work", "./work", "work directory for engine state")
		batch         = fs.Int("batch", 1000, "indexing batch size")
		warmup        = fs.Float64("warmup", 0.10, "warmup fraction in [0, 0.5]")
		concurrency   = fs.Int("concurrency", 1, "search concurrency")
		seed          = fs.Uint64("seed", 0xC0FFEE, "deterministic seed")
		msmarcoDir    = fs.String("msmarco-dir", "", "directory with collection.tsv, queries.dev.small.tsv, qrels.dev.small.tsv")
		maxDocs       = fs.Int("max-docs", 0, "cap dataset documents (0 = all)")
		maxQueries    = fs.Int("max-queries", 0, "cap dataset queries (0 = all)")
		synthDocs     = fs.Int("synth-docs", 5000, "synthetic document count")
		synthQueries  = fs.Int("synth-queries", 500, "synthetic query count")
		wordsPerDoc   = fs.Int("words-per-doc", 60, "synthetic words per document")
		wordsPerQuery = fs.Int("words-per-query", 3, "synthetic words per query")
		vocabSize     = fs.Int("vocab-size", 20000, "synthetic vocabulary size")
		zipfS         = fs.Float64("zipf-s", 1.07, "synthetic Zipf exponent")
		index         = fs.String("index", "slicedradix", "fts-engine index: slicedradix|hamt")
		scorer        = fs.String("scorer", "bm25", "fts-engine scorer: none|bm25|tfidf")
		lang          = fs.String("lang", "en", "fts-engine language preset: en|ru|multi|none")
		filter        = fs.String("filter", "none", "fts-engine filter: none|bloom|cuckoo|ribbon")
		persist       = fs.String("persist", "none", "fts-engine persistence: none|snapshot|segment")
		diagnostics   = fs.Bool("diagnostics", false, "enable fts-engine search diagnostics")
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
		MSMARCODir:    strings.TrimSpace(*msmarcoDir),
		MaxDocs:       *maxDocs,
		MaxQueries:    *maxQueries,
		SynthDocs:     *synthDocs,
		SynthQueries:  *synthQueries,
		WordsPerDoc:   *wordsPerDoc,
		WordsPerQuery: *wordsPerQuery,
		VocabSize:     *vocabSize,
		ZipfS:         *zipfS,
		Index:         strings.TrimSpace(*index),
		Scorer:        strings.TrimSpace(*scorer),
		Lang:          strings.TrimSpace(*lang),
		Filter:        strings.TrimSpace(*filter),
		Persist:       strings.TrimSpace(*persist),
		Diagnostics:   *diagnostics,
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
	if c.MaxDocs < 0 {
		return fmt.Errorf("bench: -max-docs must be >= 0, got %d", c.MaxDocs)
	}
	if c.MaxQueries < 0 {
		return fmt.Errorf("bench: -max-queries must be >= 0, got %d", c.MaxQueries)
	}
	if err := validateDatasetConfig(c); err != nil {
		return err
	}
	if err := validateFTSEngineConfig(c); err != nil {
		return err
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
		eng, err := buildEngineWithConfig(engineName, cfg)
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
		rec := metrics.Build(rep, qs, metrics.RunMeta{
			Concurrency: cfg.Concurrency,
			BatchSize:   cfg.Batch,
			WarmupFrac:  cfg.Warmup,
		})
		rec.Dataset = datasetMetadata(cfg, corpus)
		if provider, ok := eng.(harness.MetadataProvider); ok {
			rec.Config = provider.BenchmarkMetadata()
		}
		if provider, ok := eng.(harness.ExtrasProvider); ok {
			rec.Extras = provider.BenchmarkExtras()
		}
		records = append(records, rec)
	}

	if cfg.Out != "" {
		if err := writeJSON(cfg.Out, records); err != nil {
			return err
		}
	}

	return metrics.WriteTable(os.Stdout, records)
}

func loadDataset(cfg config) (*dataset.Corpus, error) {
	switch cfg.Dataset {
	case "synthetic":
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
	case "msmarco":
		return dataset.LoadMSMARCO(dataset.MSMARCOConfig{
			Dir:        cfg.MSMARCODir,
			MaxDocs:    cfg.MaxDocs,
			MaxQueries: cfg.MaxQueries,
			K:          cfg.K,
			Seed:       cfg.Seed,
		})
	default:
		return nil, fmt.Errorf("bench: unsupported dataset %q", cfg.Dataset)
	}
}

func datasetMetadata(cfg config, corpus *dataset.Corpus) metrics.DatasetMeta {
	meta := metrics.DatasetMeta{
		Name:       cfg.Dataset,
		NumDocs:    len(corpus.Docs),
		NumQueries: len(corpus.Queries),
	}
	if cfg.Dataset == "synthetic" {
		meta.Params = map[string]any{
			"seed":            cfg.Seed,
			"synth_docs":      cfg.SynthDocs,
			"synth_queries":   cfg.SynthQueries,
			"words_per_doc":   cfg.WordsPerDoc,
			"words_per_query": cfg.WordsPerQuery,
			"vocab_size":      cfg.VocabSize,
			"zipf_s":          cfg.ZipfS,
		}
	} else if cfg.Dataset == "msmarco" {
		meta.Params = map[string]any{
			"seed":        cfg.Seed,
			"msmarco_dir": cfg.MSMARCODir,
			"max_docs":    cfg.MaxDocs,
			"max_queries": cfg.MaxQueries,
		}
	}
	return meta
}

func validateDatasetConfig(c config) error {
	switch c.Dataset {
	case "synthetic":
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
	case "msmarco":
		if c.MSMARCODir == "" {
			return errors.New("bench: -msmarco-dir must not be empty when -dataset=msmarco")
		}
	default:
		return fmt.Errorf("bench: unsupported dataset %q", c.Dataset)
	}
	return nil
}

func buildEngineWithConfig(name string, cfg config) (harness.Engine, error) {
	switch name {
	case "mock":
		return mock.New(), nil
	case "fts-engine":
		return benchftsengine.New(benchftsengine.Config{
			Index:       cfg.Index,
			Scorer:      cfg.Scorer,
			Lang:        cfg.Lang,
			Filter:      cfg.Filter,
			Persist:     cfg.Persist,
			Diagnostics: cfg.Diagnostics,
		}), nil
	case "bleve":
		return bleve.New(), nil
	case "bluge":
		return bluge.New(), nil
	default:
		return nil, fmt.Errorf("bench: unknown engine %q", name)
	}
}

func validateFTSEngineConfig(cfg config) error {
	switch cfg.Index {
	case "slicedradix", "hamt":
	default:
		return fmt.Errorf("bench: unsupported -index %q", cfg.Index)
	}
	switch cfg.Scorer {
	case "", "none", "bm25", "tfidf":
	default:
		return fmt.Errorf("bench: unsupported -scorer %q", cfg.Scorer)
	}
	switch cfg.Lang {
	case "", "none", "en", "ru", "multi":
	default:
		return fmt.Errorf("bench: unsupported -lang %q", cfg.Lang)
	}
	switch cfg.Filter {
	case "", "none", "bloom", "cuckoo", "ribbon":
	default:
		return fmt.Errorf("bench: unsupported -filter %q", cfg.Filter)
	}
	switch cfg.Persist {
	case "", "none", "snapshot", "segment":
	default:
		return fmt.Errorf("bench: unsupported -persist %q", cfg.Persist)
	}
	return nil
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
