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
	WikiDump      string
	WikiCacheDir  string
	MaxDocs       int
	MaxQueries    int
	SynthDocs     int
	SynthQueries  int
	WordsPerDoc   int
	WordsPerQuery int
	VocabSize     int
	ZipfS         float64
	TypedQueries  int
	QueryTypes    []string
	HighSkipTop   int
	HighPool      int
	LowPool       int
	PrefixMinExp  int
	PrefixMaxExp  int
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
		wikiDump      = fs.String("wiki-dump", "", "path to real-data wiki dump (.xml|.xml.gz|.xml.bz2) for -dataset=wiki-typed")
		wikiCacheDir  = fs.String("wiki-cache-dir", "", "directory for cached wiki-typed query artifacts (default: sibling .wiki-typed-cache)")
		maxDocs       = fs.Int("max-docs", 0, "cap dataset documents (0 = all)")
		maxQueries    = fs.Int("max-queries", 0, "cap dataset queries (0 = all)")
		synthDocs     = fs.Int("synth-docs", 5000, "synthetic document count")
		synthQueries  = fs.Int("synth-queries", 500, "synthetic query count")
		wordsPerDoc   = fs.Int("words-per-doc", 60, "synthetic words per document")
		wordsPerQuery = fs.Int("words-per-query", 3, "synthetic words per query")
		vocabSize     = fs.Int("vocab-size", 20000, "synthetic vocabulary size")
		zipfS         = fs.Float64("zipf-s", 1.07, "synthetic Zipf exponent")
		typedQueries  = fs.Int("typed-queries", 200, "queries per class for typed real-data datasets")
		queryTypes    = fs.String("query-types", "", "comma-separated typed query classes (term,and-hh,and-hl,or-hh,phrase,prefix)")
		highSkipTop   = fs.Int("high-skip-top", 30, "skip this many top DF terms before building high-frequency pools")
		highPool      = fs.Int("high-pool", 300, "number of high-DF terms in the query generator pool")
		lowPool       = fs.Int("low-pool", 5000, "number of lower/mid-DF terms in the query generator pool")
		prefixMinExp  = fs.Int("prefix-min-expand", 2, "minimum number of term expansions for generated prefix queries")
		prefixMaxExp  = fs.Int("prefix-max-expand", 32, "maximum number of term expansions for generated prefix queries")
		index         = fs.String("index", "slicedradix", "fts-engine index: slicedradix|hamt|flat")
		scorer        = fs.String("scorer", "bm25", "fts-engine scorer: none|bm25|tfidf")
		lang          = fs.String("lang", "en", "fts-engine analysis preset: en|ru|multi|observability|none")
		filter        = fs.String("filter", "none", "fts-engine filter: none|bloom|cuckoo|ribbon")
		persist       = fs.String("persist", "snapshot", "fts-engine persistence: none|snapshot|segment")
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
		WikiDump:      strings.TrimSpace(*wikiDump),
		WikiCacheDir:  strings.TrimSpace(*wikiCacheDir),
		MaxDocs:       *maxDocs,
		MaxQueries:    *maxQueries,
		SynthDocs:     *synthDocs,
		SynthQueries:  *synthQueries,
		WordsPerDoc:   *wordsPerDoc,
		WordsPerQuery: *wordsPerQuery,
		VocabSize:     *vocabSize,
		ZipfS:         *zipfS,
		TypedQueries:  *typedQueries,
		QueryTypes:    splitCSV(*queryTypes),
		HighSkipTop:   *highSkipTop,
		HighPool:      *highPool,
		LowPool:       *lowPool,
		PrefixMinExp:  *prefixMinExp,
		PrefixMaxExp:  *prefixMaxExp,
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
	logf("bench: loading dataset=%s", cfg.Dataset)
	corpus, err := loadDataset(cfg)
	if err != nil {
		return err
	}
	logf("bench: dataset loaded docs=%d queries=%d groups=%d", len(corpus.Docs), len(corpus.Queries), len(corpusQueryGroups(corpus)))
	for _, key := range []string{"docs_cache_used", "query_cache_used", "docs_cache_file", "queries_cache_file", "qrels_cache_file", "manifest_file"} {
		if value, ok := corpus.Meta[key]; ok {
			logf("bench: dataset meta %s=%v", key, value)
		}
	}
	if err := os.MkdirAll(cfg.Work, 0o755); err != nil {
		return fmt.Errorf("bench: create work dir: %w", err)
	}

	records := make([]metrics.Record, 0, len(cfg.Engines))
	groups := corpusQueryGroups(corpus)
	for _, engineName := range cfg.Engines {
		logf("bench: preparing engine=%s", engineName)
		if err := runEngineGroups(ctx, engineName, cfg, corpus, groups, &records); err != nil {
			return err
		}
	}

	if cfg.Out != "" {
		logf("bench: writing json output=%s", cfg.Out)
		if err := writeJSON(cfg.Out, records); err != nil {
			return err
		}
	}

	logf("bench: writing summary table records=%d", len(records))
	return metrics.WriteTable(os.Stdout, records)
}

func runEngineGroups(ctx context.Context, engineName string, cfg config, corpus *dataset.Corpus, groups []dataset.QueryGroup, records *[]metrics.Record) error {
	eng, err := buildEngineWithConfig(engineName, cfg)
	if err != nil {
		return err
	}
	defer eng.Close()

	buildReport, err := harness.Prepare(ctx, eng, corpus.Docs, harness.RunConfig{
		Dir:       filepath.Join(cfg.Work, engineName),
		BatchSize: cfg.Batch,
		Seed:      cfg.Seed,
	})
	if err != nil {
		return fmt.Errorf("bench: prepare %s: %w", engineName, err)
	}
	logf("bench: engine=%s index ready build_ms=%d index_bytes=%d", engineName, buildReport.IndexBuildDur.Milliseconds(), buildReport.IndexBytes)

	for _, group := range groups {
		logf("bench: running engine=%s query_class=%s docs=%d queries=%d", engineName, groupNameOrDefault(group.Name), len(corpus.Docs), len(group.Queries))
		rep, err := harness.RunQueries(ctx, eng, group.Queries, harness.RunConfig{
			WarmupFrac:  cfg.Warmup,
			Concurrency: cfg.Concurrency,
			Seed:        cfg.Seed,
		}, buildReport)
		if err != nil {
			return fmt.Errorf("bench: run %s/%s: %w", engineName, groupNameOrDefault(group.Name), err)
		}
		logf("bench: completed engine=%s query_class=%s build_ms=%d qps=%.0f", engineName, groupNameOrDefault(group.Name), rep.IndexBuildDur.Milliseconds(), safeQPS(rep))

		qs := quality.Compute(rep.QueryResults, group.Qrels, cfg.K)
		rec := metrics.Build(rep, qs, metrics.RunMeta{
			Concurrency: cfg.Concurrency,
			BatchSize:   cfg.Batch,
			WarmupFrac:  cfg.Warmup,
		})
		rec.QueryClass = group.Name
		rec.Dataset = datasetMetadata(cfg, corpus, group)
		if provider, ok := eng.(harness.MetadataProvider); ok {
			rec.Config = provider.BenchmarkMetadata()
		}
		if provider, ok := eng.(harness.ExtrasProvider); ok {
			rec.Extras = provider.BenchmarkExtras()
		}
		*records = append(*records, rec)
	}
	return nil
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
	case "wiki-typed":
		return dataset.LoadWikiTyped(dataset.WikiTypedConfig{
			DumpPath:         cfg.WikiDump,
			CacheDir:         cfg.WikiCacheDir,
			MaxDocs:          cfg.MaxDocs,
			K:                cfg.K,
			Seed:             cfg.Seed,
			QueriesPerClass:  cfg.TypedQueries,
			HighSkipTop:      cfg.HighSkipTop,
			HighPool:         cfg.HighPool,
			LowPool:          cfg.LowPool,
			PrefixMinExpand:  cfg.PrefixMinExp,
			PrefixMaxExpand:  cfg.PrefixMaxExp,
			QueryTypes:       cfg.QueryTypes,
			IncludeTitleText: true,
			Logf:             logf,
		})
	default:
		return nil, fmt.Errorf("bench: unsupported dataset %q", cfg.Dataset)
	}
}

func datasetMetadata(cfg config, corpus *dataset.Corpus, group dataset.QueryGroup) metrics.DatasetMeta {
	meta := metrics.DatasetMeta{
		Name:       cfg.Dataset,
		NumDocs:    len(corpus.Docs),
		NumQueries: len(group.Queries),
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
	} else if cfg.Dataset == "wiki-typed" {
		meta.Params = map[string]any{
			"seed":              cfg.Seed,
			"wiki_dump":         cfg.WikiDump,
			"wiki_cache_dir":    cfg.WikiCacheDir,
			"max_docs":          cfg.MaxDocs,
			"typed_queries":     cfg.TypedQueries,
			"high_skip_top":     cfg.HighSkipTop,
			"high_pool":         cfg.HighPool,
			"low_pool":          cfg.LowPool,
			"prefix_min_expand": cfg.PrefixMinExp,
			"prefix_max_expand": cfg.PrefixMaxExp,
			"query_type_filter": strings.Join(cfg.QueryTypes, ","),
			"query_class":       group.Name,
		}
		for key, value := range corpus.Meta {
			meta.Params[key] = value
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
	case "wiki-typed":
		if c.WikiDump == "" {
			return errors.New("bench: -wiki-dump must not be empty when -dataset=wiki-typed")
		}
		if c.TypedQueries <= 0 {
			return fmt.Errorf("bench: -typed-queries must be > 0, got %d", c.TypedQueries)
		}
		if c.HighPool <= 0 {
			return fmt.Errorf("bench: -high-pool must be > 0, got %d", c.HighPool)
		}
		if c.LowPool <= 0 {
			return fmt.Errorf("bench: -low-pool must be > 0, got %d", c.LowPool)
		}
		if c.PrefixMinExp <= 0 || c.PrefixMaxExp <= 0 || c.PrefixMinExp > c.PrefixMaxExp {
			return fmt.Errorf("bench: invalid prefix expansion range %d..%d", c.PrefixMinExp, c.PrefixMaxExp)
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
	case "slicedradix", "hamt", "flat":
	default:
		return fmt.Errorf("bench: unsupported -index %q", cfg.Index)
	}
	switch cfg.Scorer {
	case "", "none", "bm25", "tfidf":
	default:
		return fmt.Errorf("bench: unsupported -scorer %q", cfg.Scorer)
	}
	switch cfg.Lang {
	case "", "none", "en", "ru", "multi", "observability":
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

func corpusQueryGroups(corpus *dataset.Corpus) []dataset.QueryGroup {
	if corpus != nil && len(corpus.Groups) > 0 {
		return corpus.Groups
	}
	return []dataset.QueryGroup{{Queries: corpus.Queries, Qrels: corpus.Qrels}}
}

func groupNameOrDefault(name string) string {
	if name == "" {
		return "default"
	}
	return name
}

func logf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func safeQPS(rep *harness.Report) float64 {
	if rep == nil || rep.Wall <= 0 || len(rep.Latencies) == 0 {
		return 0
	}
	return float64(len(rep.Latencies)) / rep.Wall.Seconds()
}
