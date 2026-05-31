package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dariasmyr/fts-engine/internal/adapters/loader/wiki"
	"github.com/dariasmyr/fts-engine/internal/bench"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftspreset"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
)

func main() {
	var (
		dumpPath    = flag.String("dump", "", "path to wiki dump (.xml.gz) - required")
		gtPath      = flag.String("ground-truth", "", "path to ground truth JSON - required")
		indexKind   = flag.String("index", "radix", "index impl: radix | slicedradix | hamt | hamtpointered")
		lang        = flag.String("lang", "en", "pipeline preset: en | ru | multi | none")
		field       = flag.String("field", "abstract", "document field to index: abstract | extract | title")
		k           = flag.Int("k", 10, "top-k used for nDCG and Recall")
		limit       = flag.Int("limit", 0, "cap documents indexed (0 = all)")
		worst       = flag.Int("worst", 5, "print N worst queries by nDCG and postings_read (0 = none)")
		warnMissing = flag.Bool("warn-missing", true, "warn if ground-truth titles don't resolve in the corpus")
		scorer      = flag.String("scorer", "none", "ranking: none | bm25 | tfidf")
		bm25K1      = flag.Float64("bm25-k1", 1.2, "BM25 k1 parameter (only with -scorer=bm25)")
		bm25B       = flag.Float64("bm25-b", 0.75, "BM25 b parameter (only with -scorer=bm25)")
		diagnostics = flag.Bool("diagnostics", true, "enable per-query diagnostics collection during benchmark runs")
		observer    = flag.Bool("observer", false, "enable search observer aggregation during benchmark runs")
		repeat      = flag.Int("repeat", 1, "repeat the full query set N times for measured runs")
		warmup      = flag.Int("warmup", 0, "run N warmup searches before measured runs (not included in metrics)")
		shuffle     = flag.Bool("shuffle", false, "shuffle query order for warmup and each measured repeat using a fixed seed")
	)
	flag.Parse()

	if *dumpPath == "" || *gtPath == "" {
		fmt.Fprintln(os.Stderr, "usage: bench -dump FILE -ground-truth FILE [flags]")
		flag.PrintDefaults()
		os.Exit(2)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := run(ctx, log, runOpts{
		dumpPath:    *dumpPath,
		gtPath:      *gtPath,
		indexKind:   *indexKind,
		lang:        *lang,
		field:       *field,
		k:           *k,
		limit:       *limit,
		worst:       *worst,
		warnMissing: *warnMissing,
		scorer:      *scorer,
		bm25K1:      *bm25K1,
		bm25B:       *bm25B,
		diagnostics: *diagnostics,
		observer:    *observer,
		repeat:      *repeat,
		warmup:      *warmup,
		shuffle:     *shuffle,
	})
	if err != nil {
		log.Error("bench failed", "error", err)
		os.Exit(1)
	}
}

type runOpts struct {
	dumpPath    string
	gtPath      string
	indexKind   string
	lang        string
	field       string
	k           int
	limit       int
	worst       int
	warnMissing bool
	scorer      string
	bm25K1      float64
	bm25B       float64
	diagnostics bool
	observer    bool
	repeat      int
	warmup      int
	shuffle     bool
}

func run(ctx context.Context, log *slog.Logger, o runOpts) error {
	index, err := ftsbuiltin.BuildIndex(o.indexKind)
	if err != nil {
		return fmt.Errorf("build index: %w", err)
	}

	pipelineOpt, err := selectPreset(o.lang)
	if err != nil {
		return err
	}
	scorerOpt, err := selectScorer(o.scorer, o.bm25K1, o.bm25B)
	if err != nil {
		return err
	}

	opts := make([]fts.Option, 0, 2)
	if pipelineOpt != nil {
		opts = append(opts, pipelineOpt)
	}
	if scorerOpt != nil {
		opts = append(opts, scorerOpt)
	}
	svc := fts.New(index, keygen.Word, opts...)

	loader := wiki.New(log, o.dumpPath)
	docs, err := loader.LoadDocuments(ctx)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("dump file not found: %s", o.dumpPath)
		}
		return fmt.Errorf("load documents: %w", err)
	}
	if o.limit > 0 && len(docs) > o.limit {
		docs = docs[:o.limit]
	}
	corpus := bench.Corpus(docs)
	log.Info("corpus loaded", "documents", len(corpus))

	selector, err := selectField(o.field)
	if err != nil {
		return err
	}

	gt, err := bench.LoadGroundTruth(o.gtPath)
	if err != nil {
		return err
	}
	log.Info("ground truth loaded", "queries", len(gt.Queries))

	titleIdx := corpus.TitleIndex()
	if o.warnMissing {
		if missing := bench.CountMissingTitles(gt, titleIdx); missing > 0 {
			log.Warn("ground truth has unresolved titles", "missing", missing, "note", "check spelling or increase -limit")
		}
	}
	if o.repeat <= 0 {
		return fmt.Errorf("repeat must be >= 1, got %d", o.repeat)
	}
	if o.warmup < 0 {
		return fmt.Errorf("warmup must be >= 0, got %d", o.warmup)
	}

	idxReport, err := bench.IndexCorpus(ctx, svc, corpus, selector)
	if err != nil {
		return fmt.Errorf("index corpus: %w", err)
	}
	log.Info("indexing done", "documents", idxReport.DocumentCount, "duration", idxReport.Duration, "heap_mb", idxReport.HeapAllocMB)

	var observerStats *ftsstats.SearchStats
	if o.observer {
		observerStats = ftsstats.NewSearchStats(64)
	}

	queryReports, err := bench.RunQueries(ctx, svc, gt, titleIdx, o.k, bench.RunQueryOptions{
		Diagnostics: o.diagnostics,
		Observer:    observerStats,
		Repeat:      o.repeat,
		Warmup:      o.warmup,
		Shuffle:     o.shuffle,
	})
	if err != nil {
		return fmt.Errorf("run queries: %w", err)
	}

	report := bench.Aggregate(o.k, idxReport, queryReports, o.diagnostics)
	fmt.Fprintf(os.Stdout, "\n=== bench result: index=%s lang=%s field=%s scorer=%s diagnostics=%t observer=%t repeat=%d warmup=%d shuffle=%t ===\n", o.indexKind, o.lang, o.field, o.scorer, o.diagnostics, o.observer, o.repeat, o.warmup, o.shuffle)
	bench.WriteReport(os.Stdout, report, o.worst)
	if observerStats != nil {
		snap := observerStats.Snapshot()
		fmt.Fprintf(os.Stdout, "\nObserver: total=%d errors=%d zero_results=%d strategies=%d\n", snap.TotalSearches, snap.ErrorsTotal, snap.ZeroResults, len(snap.ByStrategy))
	}
	return nil
}

func selectPreset(lang string) (fts.Option, error) {
	switch lang {
	case "en":
		return ftspreset.English(), nil
	case "ru":
		return ftspreset.Russian(), nil
	case "multi":
		return ftspreset.Multilingual(), nil
	case "none", "":
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown lang preset %q", lang)
	}
}

func selectScorer(kind string, k1, b float64) (fts.Option, error) {
	switch kind {
	case "", "none":
		return nil, nil
	case "bm25":
		return fts.WithScorer(&fts.BM25Scorer{K1: k1, B: b}), nil
	case "tfidf":
		return fts.WithScorer(fts.TFIDF()), nil
	default:
		return nil, fmt.Errorf("unknown scorer %q", kind)
	}
}

func selectField(name string) (bench.ContentSelector, error) {
	switch name {
	case "abstract":
		return bench.SelectAbstract, nil
	case "extract":
		return bench.SelectExtract, nil
	case "title":
		return bench.SelectTitle, nil
	default:
		return nil, fmt.Errorf("unknown field %q", name)
	}
}
