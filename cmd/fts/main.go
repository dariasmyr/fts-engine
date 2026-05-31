package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/dariasmyr/fts-engine/internal/utils"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/dariasmyr/fts-engine/config"
	"github.com/dariasmyr/fts-engine/internal/adapters/cui"
	"github.com/dariasmyr/fts-engine/internal/adapters/loader/wiki"
	"github.com/dariasmyr/fts-engine/internal/domain/models"
	"github.com/dariasmyr/fts-engine/internal/lib/logger/sl"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

const (
	_readinessDrainDelay = 5 * time.Second
)

func ensureDir(p string) {
	os.MkdirAll(p, 0755)
}

func main() {
	cfg, cfgSource := config.MustLoad()

	ensureDir("data")
	ensureDir("data/fts")

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := setupLogger(cfg.Env)
	if cfgSource == "defaults" {
		log.Warn("No config file found; using built-in defaults", "dump_path", cfg.DumpPath, "persistence_path", cfg.FTS.Persistence.Path)
	} else {
		log.Info("Loaded configuration", "source", cfgSource)
	}
	log.Info("fts", "env", cfg.Env)
	log.Info("fts", "engine", cfg.FTS.Engine)
	log.Info("fts", "index", cfg.FTS.Index)
	log.Info("fts", "keygen", cfg.FTS.KeyGen)
	log.Info("fts", "scorer", cfg.FTS.Scorer)
	log.Info("fts", "filter", cfg.FTS.Filter)
	log.Info("fts", "compaction_load_factor", cfg.FTS.Compaction.LoadFactor)
	log.Info("fts", "compaction_auto_check", cfg.FTS.Compaction.AutoCheck)
	log.Info("fts", "mode", cfg.Mode.Type)

	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		panic(err)
	}

	go func() {
		<-rootCtx.Done()
		stop()
		log.Info("Received shutdown signal, shutting down...")

		time.Sleep(_readinessDrainDelay)
		log.Info("Readiness check propagated, now waiting for ongoing processes to finish.")

		cancel()
	}()

	documentsByID := make(map[string]models.Document)

	var ftsEngine cui.SearchEngine

	switch cfg.FTS.Engine {
	case "trie":
		keyGen, err := selectKeyGenerator(cfg.FTS.KeyGen)
		if err != nil {
			log.Error("Failed to select keygen", "error", sl.Err(err))
			return
		}

		pipeline := buildPipeline(cfg)
		svc, loadedFromSnapshot, err := buildService(log, cfg, keyGen, pipeline)
		if err != nil {
			log.Error("Failed to initialize trie service", "error", sl.Err(err))
			return
		}
		ftsEngine = &serviceAdapter{service: svc, snapshotLoaded: loadedFromSnapshot, searchStats: ftsstats.NewSearchStats(64)}
	default:
		log.Error("unknown fts engine", "engine", cfg.FTS.Engine)
		return
	}

	log.Info("FTS engine initialised")

	dumpLoader := wiki.New(log, cfg.DumpPath)
	log.Info("Loader initialised")

	startTime := time.Now()
	documents, err := dumpLoader.LoadDocuments(ctx)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Warn("Dump file not found; starting with an empty corpus", "path", cfg.DumpPath)
			documents = nil
		} else {
			log.Error("Failed to load documents", "error", sl.Err(err))
			return
		}
	}

	duration := time.Since(startTime)
	log.Info(fmt.Sprintf("Unpacked & parsed %d documents in %v", len(documents), duration))

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	if cfg.Mode.Type == "experiment" {
		startTime = time.Now()
		memStats := utils.MeasureMemory(func() {
			for _, doc := range documents {
				_ = ftsEngine.IndexDocument(ctx, doc.ID, doc.Abstract)
			}
		})
		duration = time.Since(startTime)
		log.Info(fmt.Sprintf("Indexed %d documents in %v", len(documents), duration))

		analyzeTrie(cfg, ftsEngine, memStats, log)
		return
	}

	startTime = time.Now()
	for i := range documents {
		doc := documents[i]
		documentsByID[doc.ID] = doc

		select {
		case <-rootCtx.Done():
			log.Info("Received shutdown signal, shutting down...")
			return
		default:
			if indexErr := ftsEngine.IndexDocument(ctx, doc.ID, doc.Abstract); indexErr != nil {
				log.Error("could not index document:", "error", indexErr)
			}
		}
	}

	adapter, ok := ftsEngine.(*serviceAdapter)
	if !ok {
		log.Error("unexpected search engine type")
		return
	}

	if !adapter.snapshotLoaded {
		if err := buildFilterIfNeeded(log, adapter.service); err != nil {
			log.Error("Failed to finalize search filter", "error", sl.Err(err))
			return
		}

		if err := savePersistenceIfEnabled(log, cfg, adapter.service); err != nil {
			log.Error("Failed to persist state", "error", sl.Err(err))
			return
		}
	} else {
		log.Info("Skipping re-indexing: persisted state loaded", "path", cfg.FTS.Persistence.Path)
	}

	appCUI := cui.New(ctx, log, ftsEngine, documentsByID, 10)

	cuiErr := appCUI.Start()
	if cuiErr != nil {
		log.Error("Failed to start appCUI", "error", sl.Err(cuiErr))
		return
	}
	if snapshot, ok := adapter.SearchStatsSnapshot(); ok && snapshot.TotalSearches > 0 {
		logSearchStats(log, snapshot)
	}
}

func analyzeTrie(
	cfg *config.Config,
	engine cui.SearchEngine,
	memStats runtime.MemStats,
	log *slog.Logger,
) {
	statsProvider, ok := engine.(interface {
		AnalyzeStats() (pkgfts.Stats, bool)
	})
	if !ok {
		log.Warn("analyzeTrie: engine does not support analysis")
		return
	}

	stats, ok := statsProvider.AnalyzeStats()
	if !ok {
		log.Warn("analyzeTrie: engine does not support analysis")
		return
	}

	log.Info("FTS analysis result",
		"engine", cfg.FTS.Engine,
		"index", cfg.FTS.Index,
		"nodes", stats.Nodes,
		"leafNodes", stats.Leaves,
		"maxDepth", stats.MaxDepth,
		"avgDepth", stats.AvgDepth,
		"totalDocs", stats.TotalDocs,
		"totalChildren", stats.TotalChildren,
		"heapMB", memStats.HeapAlloc/1024/1024,
		"heapObjects", memStats.HeapObjects,
		"totalAllocMB", memStats.TotalAlloc/1024/1024,
	)

	for level, avg := range stats.AvgChildrenPerLevel {
		log.Info(fmt.Sprintf("Level %d: avg children = %.2f", level, avg))
	}

}

type serviceAdapter struct {
	service        *pkgfts.Service
	snapshotLoaded bool
	searchStats    *ftsstats.SearchStats
}

func (s *serviceAdapter) IndexDocument(ctx context.Context, docID string, content string) error {
	return s.service.IndexDocument(ctx, pkgfts.DocID(docID), content)
}

func (s *serviceAdapter) HighlightText(query string, text string) string {
	if s == nil || s.service == nil || strings.TrimSpace(query) == "" || text == "" {
		return text
	}

	fragments := s.service.Highlight(query, text, pkgfts.Highlighter{
		PreTag:       "\033[31m",
		PostTag:      "\033[0m",
		MaxFragments: 3,
		FragmentSize: 180,
		Separator:    " ... ",
	})
	if len(fragments) == 0 {
		return text
	}

	out := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		out = append(out, fragment.Text)
	}
	return strings.Join(out, "\n")
}

func (s *serviceAdapter) SearchDocuments(ctx context.Context, query string, maxResults int) (*models.SearchResult, error) {
	ctx = pkgfts.WithDiagnostics(ctx)
	result, err := s.service.SearchDocuments(ctx, query, maxResults)
	if s.searchStats != nil {
		s.searchStats.ObserveResult(query, result, err)
	}
	if err != nil {
		return nil, err
	}

	out := make([]models.ResultData, 0, len(result.Results))
	for _, item := range result.Results {
		out = append(out, models.ResultData{
			ID:            string(item.ID),
			UniqueMatches: item.UniqueMatches,
			TotalMatches:  item.TotalMatches,
		})
	}

	return &models.SearchResult{
		ResultData:        out,
		TotalResultsCount: result.TotalResultsCount,
		Diagnostics:       projectDiagnostics(result.Diagnostics),
	}, nil
}

func (s *serviceAdapter) AnalyzeStats() (pkgfts.Stats, bool) {
	return s.service.Analyze()
}

func (s *serviceAdapter) SearchStatsSnapshot() (ftsstats.Snapshot, bool) {
	if s.searchStats == nil {
		return ftsstats.Snapshot{}, false
	}
	return s.searchStats.Snapshot(), true
}

func logSearchStats(log *slog.Logger, snapshot ftsstats.Snapshot) {
	log.Info("search diagnostics summary",
		"total_searches", snapshot.TotalSearches,
		"errors_total", snapshot.ErrorsTotal,
		"zero_results", snapshot.ZeroResults,
		"strategies", len(snapshot.ByStrategy),
	)
	for strategy, stats := range snapshot.ByStrategy {
		log.Info("search diagnostics strategy",
			"strategy", strategy,
			"count", stats.Count,
			"avg_duration", stats.AvgDuration(),
			"max_duration", stats.MaxDuration,
			"avg_postings", stats.AvgPostings(),
		)
	}
}

func formatDiagnosticsTimings(diag *pkgfts.QueryDiagnostics) map[string]string {
	if diag == nil {
		return map[string]string{}
	}
	out := make(map[string]string, 3)
	if diag.Timings.HasPreprocess() {
		out["preprocess"] = formatAppDuration(diag.Timings.Preprocess)
	}
	if diag.Timings.HasSearchTokens() {
		out["search_tokens"] = formatAppDuration(diag.Timings.SearchTokens)
	}
	if diag.Timings.HasTotal() {
		out["total"] = formatAppDuration(diag.Timings.Total)
	}
	return out
}

func projectDiagnostics(diag *pkgfts.QueryDiagnostics) *models.SearchDiagnostics {
	if diag == nil {
		return nil
	}
	return &models.SearchDiagnostics{
		LogicalQueryType:   diag.LogicalQueryType,
		ExecutionStrategy:  diag.ExecutionStrategy,
		StrategySkipReason: diag.StrategySkipReason,
		Timings:            formatDiagnosticsTimings(diag),
		ProcessedTokens:    diag.ProcessedTokens,
		FieldsVisited:      diag.FieldsVisited,
		GeneratedKeys:      diag.GeneratedKeys,
		IndexSearches:      diag.IndexSearches,
		FilterChecks:       diag.FilterChecks,
		FilterRejects:      diag.FilterRejects,
		PostingEntriesRead: diag.PostingEntriesRead,
		CandidateDocs:      diag.CandidateDocs,
		MatchedDocs:        diag.MatchedDocs,
		ReturnedDocs:       diag.ReturnedDocs,
	}
}

func formatAppDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

func selectScorer(kind string) (pkgfts.Option, error) {
	switch kind {
	case "", "none":
		return nil, nil
	case "bm25":
		return pkgfts.WithScorer(pkgfts.BM25()), nil
	case "tfidf":
		return pkgfts.WithScorer(pkgfts.TFIDF()), nil
	default:
		return nil, fmt.Errorf("unknown scorer %q", kind)
	}
}

func buildService(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, pipeline textproc.Pipeline) (*pkgfts.Service, bool, error) {
	if cfg == nil {
		return nil, false, fmt.Errorf("nil config")
	}

	scorerOpt, err := selectScorer(cfg.FTS.Scorer)
	if err != nil {
		return nil, false, err
	}

	serviceOpts := []pkgfts.Option{pkgfts.WithPipeline(pipeline)}
	if scorerOpt != nil {
		serviceOpts = append(serviceOpts, scorerOpt)
	}
	serviceOpts = append(serviceOpts,
		pkgfts.WithCompactionLoadFactor(cfg.FTS.Compaction.LoadFactor),
		pkgfts.WithAutoCompactionCheck(cfg.FTS.Compaction.AutoCheck),
		pkgfts.WithCompactionCallback(func(stats pkgfts.CompactionStats) {
			log.Warn("FTS compaction threshold reached",
				"load_factor", stats.TombstoneLoadFactor,
				"threshold", cfg.FTS.Compaction.LoadFactor,
				"tombstoned_docs", stats.TombstonedDocs,
				"live_docs", stats.LiveDocs,
				"total_assigned_docs", stats.TotalAssignedDocs,
			)
		}),
	)

	if cfg.Mode.Type == "prod" && cfg.FTS.Persistence.Enabled && cfg.FTS.Persistence.LoadOnStart {
		svc, ok, err := tryLoadPersistence(log, cfg, keyGen, serviceOpts)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return svc, true, nil
		}
	}

	index, err := selectIndex(cfg.FTS.Index)
	if err != nil {
		return nil, false, err
	}

	searchFilter, err := selectFilter(cfg)
	if err != nil {
		return nil, false, err
	}

	if searchFilter != nil {
		serviceOpts = append(serviceOpts, pkgfts.WithFilter(searchFilter))
	}

	svc := pkgfts.New(index, keyGen, serviceOpts...)
	return svc, false, nil
}

func tryLoadPersistence(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, serviceOpts []pkgfts.Option) (*pkgfts.Service, bool, error) {
	if cfg == nil || cfg.FTS.Persistence.Path == "" {
		return nil, false, nil
	}

	expectedFilter := cfg.FTS.Filter
	if expectedFilter == "none" {
		expectedFilter = ""
	}

	switch cfg.FTS.Persistence.Format {
	case "snapshot":
		indexPath := persistenceSnapshotIndexPath(cfg)
		filterPath := persistenceSnapshotFilterPath(cfg)
		if expectedFilter == "" {
			if _, err := os.Stat(filterPath); errors.Is(err, os.ErrNotExist) {
				filterPath = ""
			} else if err != nil && filterPath != "" {
				return nil, false, fmt.Errorf("check persistence filter path: %w", err)
			}
		}
		loaded, err := ftspersist.LoadSnapshot(ftspersist.SnapshotPaths{IndexPath: indexPath, FilterPath: filterPath}, keyGen, serviceOpts...)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, false, nil
			}
			return nil, false, err
		}
		if loaded.IndexName != "" && loaded.IndexName != cfg.FTS.Index {
			log.Warn("Snapshot index type differs from config",
				"snapshot_index", loaded.IndexName,
				"config_index", cfg.FTS.Index,
				"path", indexPath,
			)
		}
		if loaded.FilterName != expectedFilter {
			log.Warn("Snapshot filter type differs from config",
				"snapshot_filter", loaded.FilterName,
				"config_filter", cfg.FTS.Filter,
				"path", filterPath,
			)
		}
		log.Info("Loaded snapshot persistence", "index_path", indexPath, "filter_path", filterPath)
		return loaded.Service, true, nil
	case "segment":
		loaded, err := ftspersist.LoadSegment(
			ftspersist.SegmentPaths{Dir: cfg.FTS.Persistence.Path},
			keyGen,
			ftspersist.SegmentLoadOptions{Access: persistenceAccessMode(cfg)},
			serviceOpts...,
		)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, false, nil
			}
			return nil, false, err
		}
		if loaded.FilterName != expectedFilter {
			log.Warn("Segment filter type differs from config",
				"segment_filter", loaded.FilterName,
				"config_filter", cfg.FTS.Filter,
				"path", cfg.FTS.Persistence.Path,
			)
		}
		log.Info("Loaded segment persistence", "dir_path", cfg.FTS.Persistence.Path, "access", cfg.FTS.Persistence.Access)
		return loaded.Service, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported persistence format %q", cfg.FTS.Persistence.Format)
	}
}

func savePersistenceIfEnabled(log *slog.Logger, cfg *config.Config, svc *pkgfts.Service) error {
	if cfg == nil || svc == nil {
		return nil
	}

	if !cfg.FTS.Persistence.Enabled || !cfg.FTS.Persistence.SaveOnBuild {
		return nil
	}

	filterName := cfg.FTS.Filter
	if filterName == "none" {
		filterName = ""
	}
	opts := ftspersist.SaveOptions{
		BufferSize:     cfg.FTS.Persistence.BufferSize,
		FlushThreshold: cfg.FTS.Persistence.FlushThreshold,
		SyncFile:       cfg.FTS.Persistence.SyncFile,
	}

	switch cfg.FTS.Persistence.Format {
	case "snapshot":
		indexPath := persistenceSnapshotIndexPath(cfg)
		filterPath := persistenceSnapshotFilterPath(cfg)
		if err := ftspersist.SaveSnapshot(ftspersist.SnapshotPaths{IndexPath: indexPath, FilterPath: filterPath}, svc, cfg.FTS.Index, filterName, opts); err != nil {
			return err
		}
		log.Info("FTS snapshot persisted", "index_path", indexPath, "filter_path", filterPath)
		return nil
	case "segment":
		if err := ftspersist.SaveSegment(ftspersist.SegmentPaths{Dir: cfg.FTS.Persistence.Path}, svc, filterName, opts); err != nil {
			return err
		}
		log.Info("FTS segment persisted", "dir_path", cfg.FTS.Persistence.Path)
		return nil
	default:
		return fmt.Errorf("unsupported persistence format %q", cfg.FTS.Persistence.Format)
	}
}

func persistenceSnapshotIndexPath(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return filepath.Join(cfg.FTS.Persistence.Path, "index.fidx")
}

func persistenceSnapshotFilterPath(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return filepath.Join(cfg.FTS.Persistence.Path, "filter.fidx")
}

func persistenceAccessMode(cfg *config.Config) ftspersist.AccessMode {
	if cfg == nil {
		return ftspersist.AccessFile
	}
	switch cfg.FTS.Persistence.Access {
	case "mmap":
		return ftspersist.AccessMmap
	default:
		return ftspersist.AccessFile
	}
}

func selectKeyGenerator(kind string) (pkgfts.KeyGenerator, error) {
	switch kind {
	case "word":
		return keygen.Word, nil
	default:
		return nil, fmt.Errorf("unknown keygen %q", kind)
	}
}

func selectIndex(name string) (pkgfts.Index, error) {
	return ftsbuiltin.BuildIndex(name)
}

func selectFilter(cfg *config.Config) (pkgfts.Filter, error) {
	return ftsbuiltin.BuildFilter(cfg.FTS.Filter, buildFilterOptions(cfg))
}

func buildFilterOptions(cfg *config.Config) ftsbuiltin.FilterOptions {
	if cfg == nil {
		return ftsbuiltin.FilterOptions{}
	}

	return ftsbuiltin.FilterOptions{
		BloomExpectedItems:  cfg.FTS.Bloom.ExpectedItems,
		BloomBitsPerItem:    cfg.FTS.Bloom.BitsPerItem,
		BloomK:              cfg.FTS.Bloom.K,
		CuckooBucketCount:   cfg.FTS.Cuckoo.BucketCount,
		CuckooBucketSize:    cfg.FTS.Cuckoo.BucketSize,
		CuckooMaxKicks:      cfg.FTS.Cuckoo.MaxKicks,
		RibbonExpectedItems: cfg.FTS.Ribbon.ExpectedItems,
		RibbonExtraCells:    cfg.FTS.Ribbon.ExtraCells,
		RibbonWindowSize:    cfg.FTS.Ribbon.WindowSize,
		RibbonSeed:          cfg.FTS.Ribbon.Seed,
		RibbonMaxAttempts:   cfg.FTS.Ribbon.MaxAttempts,
	}
}

func buildFilterIfNeeded(log *slog.Logger, svc *pkgfts.Service) error {
	if svc == nil {
		return nil
	}

	startedAt := time.Now()
	if err := svc.BuildFilter(); err != nil {
		return fmt.Errorf("build search filter: %w", err)
	}

	log.Info("Search filter finalized", "duration", time.Since(startedAt))
	return nil
}

func buildPipeline(cfg *config.Config) textproc.Pipeline {
	filters := make([]textproc.Filter, 0, 4)

	if cfg.FTS.Pipeline.Lowercase {
		filters = append(filters, textproc.LowercaseFilter{})
	}

	if cfg.FTS.Pipeline.MinLength > 0 {
		filters = append(filters, textproc.MinLengthOrNumericFilter{MinLength: cfg.FTS.Pipeline.MinLength})
	}

	if cfg.FTS.Pipeline.StopwordsEN {
		filters = append(filters, textproc.EnglishStopwordFilter{})
	}

	if cfg.FTS.Pipeline.StopwordsRU {
		filters = append(filters, textproc.RussianStopwordFilter{})
	}

	if cfg.FTS.Pipeline.StemEN {
		filters = append(filters, textproc.EnglishStemFilter{})
	}

	if cfg.FTS.Pipeline.StemRU {
		filters = append(filters, textproc.RussianStemFilter{})
	}

	return textproc.NewPipeline(textproc.AlnumTokenizer{}, filters...)
}

func setupLogger(env string) *slog.Logger {
	logFile, err := os.OpenFile("data/app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Failed to open log file:", err)
		os.Exit(1)
	}

	multiWriter := io.MultiWriter(os.Stdout, logFile)

	var log *slog.Logger
	switch env {
	case envLocal:
		log = slog.New(
			slog.NewTextHandler(multiWriter, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envDev:
		log = slog.New(
			slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envProd:
		log = slog.New(
			slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	}

	return log
}
