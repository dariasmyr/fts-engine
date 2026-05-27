package main

import (
	"context"
	"errors"
	"fmt"
	ftspersist "github.com/dariasmyr/fts-engine/internal/services/fts/persist"
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
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/segment"
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
	ensureDir("data/segments")

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := setupLogger(cfg.Env)
	if cfgSource == "defaults" {
		log.Warn("No config file found; using built-in defaults", "dump_path", cfg.DumpPath, "snapshot_path", cfg.FTS.Snapshot.Path)
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

		if err := saveSnapshotIfEnabled(log, cfg, adapter.service); err != nil {
			log.Error("Failed to persist snapshot", "error", sl.Err(err))
			return
		}
	} else {
		log.Info("Skipping re-indexing: snapshot loaded", "path", cfg.FTS.Snapshot.Path)
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

	if cfg.Mode.Type == "prod" && cfg.FTS.Snapshot.Enabled && cfg.FTS.Snapshot.LoadOnStart {
		svc, ok, err := tryLoadSnapshot(log, cfg, keyGen, serviceOpts)
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

func tryLoadSnapshot(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, serviceOpts []pkgfts.Option) (*pkgfts.Service, bool, error) {
	svc, ok, err := tryLoadBundleSnapshot(log, cfg, keyGen, serviceOpts)
	if err != nil || ok {
		return svc, ok, err
	}
	return tryLoadSplitSnapshot(log, cfg, keyGen, serviceOpts)
}

func tryLoadBundleSnapshot(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, serviceOpts []pkgfts.Option) (*pkgfts.Service, bool, error) {
	bundlePath := snapshotBundlePath(cfg)
	filterPath := snapshotFilterPath(cfg)
	expectedFilter := cfg.FTS.Filter
	if expectedFilter == "none" {
		expectedFilter = ""
	}
	if bundlePath == "" {
		return nil, false, nil
	}

	if _, err := os.Stat(bundlePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("check segment bundle path: %w", err)
	}

	bundleFile, err := os.Open(bundlePath)
	if err != nil {
		return nil, false, fmt.Errorf("open segment bundle: %w", err)
	}
	defer bundleFile.Close()

	loadedBundle, err := segment.LoadBundle(bundleFile)
	if err != nil {
		return nil, false, fmt.Errorf("load segment bundle: %w", err)
	}

	builtOpts := append([]pkgfts.Option(nil), serviceOpts...)
	if expectedFilter != "" {
		if filterPath == "" {
			return nil, false, nil
		}
		if _, statErr := os.Stat(filterPath); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				log.Info("Segment bundle filter snapshot is missing, rebuilding from source", "path", filterPath)
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("check filter snapshot path: %w", statErr)
		}
		filterFile, openErr := os.Open(filterPath)
		if openErr != nil {
			return nil, false, fmt.Errorf("open filter snapshot: %w", openErr)
		}
		loadedFilter, loadErr := pkgfts.LoadFilterSnapshot(filterFile)
		_ = filterFile.Close()
		if loadErr != nil {
			return nil, false, fmt.Errorf("load filter snapshot: %w", loadErr)
		}
		if loadedFilter.FilterName != expectedFilter {
			log.Warn("Bundle filter type differs from config",
				"bundle_filter", loadedFilter.FilterName,
				"config_filter", cfg.FTS.Filter,
				"path", filterPath,
			)
		}
		if loadedFilter.Filter != nil {
			builtOpts = append(builtOpts, pkgfts.WithFilter(loadedFilter.Filter))
		}
	}

	svc, err := segment.RestoreService(loadedBundle, keyGen, builtOpts...)
	if err != nil {
		return nil, false, fmt.Errorf("restore service from segment bundle: %w", err)
	}
	log.Info("Loaded segment bundle", "bundle_path", bundlePath, "filter_path", filterPath)
	return svc, true, nil
}

func tryLoadSplitSnapshot(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, serviceOpts []pkgfts.Option) (*pkgfts.Service, bool, error) {
	indexPath := snapshotIndexPath(cfg)
	filterPath := snapshotFilterPath(cfg)
	expectedFilter := cfg.FTS.Filter
	if expectedFilter == "none" {
		expectedFilter = ""
	}
	if indexPath == "" {
		return nil, false, nil
	}

	if _, err := os.Stat(indexPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("check index snapshot path: %w", err)
	}

	indexFile, err := os.Open(indexPath)
	if err != nil {
		return nil, false, fmt.Errorf("open index snapshot: %w", err)
	}
	defer indexFile.Close()

	loadedIndex, err := pkgfts.LoadIndexSnapshot(indexFile)
	if err != nil {
		return nil, false, fmt.Errorf("load index snapshot: %w", err)
	}

	if loadedIndex.IndexName != cfg.FTS.Index {
		log.Warn("Snapshot index type differs from config",
			"snapshot_index", loadedIndex.IndexName,
			"config_index", cfg.FTS.Index,
			"path", indexPath,
		)
	}

	builtOpts := append([]pkgfts.Option(nil), serviceOpts...)
	if len(loadedIndex.Registry) > 0 {
		builtOpts = append(builtOpts, pkgfts.WithDocRegistrySnapshot(loadedIndex.Registry))
	}
	if len(loadedIndex.Tombstones) > 0 {
		builtOpts = append(builtOpts, pkgfts.WithTombstonesSnapshot(loadedIndex.Tombstones))
	}
	if loadedIndex.CollectionStats != nil {
		builtOpts = append(builtOpts, pkgfts.WithCollectionStatsSnapshot(loadedIndex.CollectionStats))
	}

	if expectedFilter != "" {
		if filterPath == "" {
			return nil, false, nil
		}

		if _, statErr := os.Stat(filterPath); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				log.Info("Split filter snapshot is missing, rebuilding from source", "path", filterPath)
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("check filter snapshot path: %w", statErr)
		}

		filterFile, openErr := os.Open(filterPath)
		if openErr != nil {
			return nil, false, fmt.Errorf("open filter snapshot: %w", openErr)
		}

		loadedFilter, loadErr := pkgfts.LoadFilterSnapshot(filterFile)
		_ = filterFile.Close()
		if loadErr != nil {
			return nil, false, fmt.Errorf("load filter snapshot: %w", loadErr)
		}

		if loadedFilter.FilterName != expectedFilter {
			log.Warn("Snapshot filter type differs from config",
				"snapshot_filter", loadedFilter.FilterName,
				"config_filter", cfg.FTS.Filter,
				"path", filterPath,
			)
		}

		if loadedFilter.Filter != nil {
			builtOpts = append(builtOpts, pkgfts.WithFilter(loadedFilter.Filter))
		}
	} else if filterPath != "" {
		if _, statErr := os.Stat(filterPath); statErr == nil {
			filterFile, openErr := os.Open(filterPath)
			if openErr != nil {
				return nil, false, fmt.Errorf("open filter snapshot: %w", openErr)
			}

			loadedFilter, loadErr := pkgfts.LoadFilterSnapshot(filterFile)
			_ = filterFile.Close()
			if loadErr != nil {
				return nil, false, fmt.Errorf("load filter snapshot: %w", loadErr)
			}

			if loadedFilter.FilterName != expectedFilter {
				log.Warn("Snapshot filter type differs from config",
					"snapshot_filter", loadedFilter.FilterName,
					"config_filter", cfg.FTS.Filter,
					"path", filterPath,
				)
			}

			if loadedFilter.Filter != nil {
				builtOpts = append(builtOpts, pkgfts.WithFilter(loadedFilter.Filter))
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, false, fmt.Errorf("check filter snapshot path: %w", statErr)
		}
	}

	svc := pkgfts.New(loadedIndex.Index, keyGen, builtOpts...)
	log.Info("Loaded split FTS snapshots", "index_path", indexPath, "filter_path", filterPath)
	return svc, true, nil
}

func saveSnapshotIfEnabled(log *slog.Logger, cfg *config.Config, svc *pkgfts.Service) error {
	if cfg == nil || svc == nil {
		return nil
	}

	if !cfg.FTS.Snapshot.Enabled || !cfg.FTS.Snapshot.SaveOnBuild {
		return nil
	}

	if err := saveBundleSnapshot(log, cfg, svc); err == nil {
		return nil
	} else if !errors.Is(err, errSegmentBundleUnsupported) {
		return err
	}

	return saveSplitSnapshots(log, cfg, svc)
}

var errSegmentBundleUnsupported = errors.New("segment bundle unsupported")

func saveBundleSnapshot(log *slog.Logger, cfg *config.Config, svc *pkgfts.Service) error {
	if cfg == nil || svc == nil {
		return nil
	}

	bundlePath := snapshotBundlePath(cfg)
	filterPath := snapshotFilterPath(cfg)
	if bundlePath == "" {
		return errSegmentBundleUnsupported
	}

	index, searchFilter := svc.SnapshotComponents()
	fields, _ := svc.SnapshotFields()
	sources := make(map[string]segment.Source, len(fields))
	for fieldName, fieldIndex := range fields {
		source, ok := fieldIndex.(segment.Source)
		if !ok {
			return errSegmentBundleUnsupported
		}
		sources[fieldName] = source
	}
	if len(sources) == 0 && index != nil {
		source, ok := index.(segment.Source)
		if !ok {
			return errSegmentBundleUnsupported
		}
		sources[packageDefaultField()] = source
	}

	filterName := cfg.FTS.Filter
	if filterName == "none" {
		filterName = ""
	}

	stats := svc.SnapshotCollectionStats()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()
	opts := ftspersist.SaveOptions{
		BufferSize:     cfg.FTS.Snapshot.BufferSize,
		FlushThreshold: cfg.FTS.Snapshot.FlushThreshold,
		SyncFile:       cfg.FTS.Snapshot.SyncFile,
	}

	if err := ftspersist.SaveAtomicWithOptions(bundlePath, opts, func(w io.Writer) error {
		return segment.SaveMultiFieldBundle(w, sources, stats, registry, tombstones)
	}); err != nil {
		return err
	}

	if searchFilter != nil && filterName != "" {
		if err := ftspersist.SaveAtomicWithOptions(filterPath, opts, func(w io.Writer) error {
			return pkgfts.SaveFilterSnapshot(w, filterName, searchFilter)
		}); err != nil {
			return err
		}
	} else if filterPath != "" {
		if err := os.Remove(filterPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale filter snapshot: %w", err)
		}
	}

	log.Info("FTS segment bundle persisted", "bundle_path", bundlePath, "filter_path", filterPath)
	return nil
}

func saveSplitSnapshots(log *slog.Logger, cfg *config.Config, svc *pkgfts.Service) error {
	if cfg == nil || svc == nil {
		return nil
	}

	indexPath := snapshotIndexPath(cfg)
	filterPath := snapshotFilterPath(cfg)
	if indexPath == "" {
		return fmt.Errorf("snapshot index path is empty")
	}

	indexName := cfg.FTS.Index
	filterName := cfg.FTS.Filter
	if filterName == "none" {
		filterName = ""
	}

	index, searchFilter := svc.SnapshotComponents()
	stats := svc.SnapshotCollectionStats()
	registry := svc.SnapshotRegistry()
	tombstones := svc.SnapshotTombstones()

	opts := ftspersist.SaveOptions{
		BufferSize:     cfg.FTS.Snapshot.BufferSize,
		FlushThreshold: cfg.FTS.Snapshot.FlushThreshold,
		SyncFile:       cfg.FTS.Snapshot.SyncFile,
	}

	if err := ftspersist.SaveAtomicWithOptions(indexPath, opts, func(w io.Writer) error {
		return pkgfts.SaveIndexSnapshotWithState(w, indexName, index, stats, registry, tombstones)
	}); err != nil {
		return err
	}

	if searchFilter != nil && filterName != "" {
		if err := ftspersist.SaveAtomicWithOptions(filterPath, opts, func(w io.Writer) error {
			return pkgfts.SaveFilterSnapshot(w, filterName, searchFilter)
		}); err != nil {
			return err
		}
	} else if filterPath != "" {
		if err := os.Remove(filterPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale filter snapshot: %w", err)
		}
	}

	log.Info("FTS snapshots persisted", "index_path", indexPath, "filter_path", filterPath)
	return nil
}

func snapshotIndexPath(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}

	if cfg.FTS.Snapshot.IndexPath != "" {
		return cfg.FTS.Snapshot.IndexPath
	}

	base := cfg.FTS.Snapshot.Path
	if base == "" {
		return ""
	}

	ext := filepath.Ext(base)
	if ext == "" {
		return base + ".index"
	}

	return base[:len(base)-len(ext)] + ".index" + ext
}

func snapshotBundlePath(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}

	base := cfg.FTS.Snapshot.Path
	if base == "" {
		return ""
	}

	ext := filepath.Ext(base)
	if ext == "" {
		return base + ".bundle"
	}

	return base[:len(base)-len(ext)] + ".bundle" + ext
}

func packageDefaultField() string {
	return pkgfts.DefaultField
}

func snapshotFilterPath(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}

	if cfg.FTS.Snapshot.FilterPath != "" {
		return cfg.FTS.Snapshot.FilterPath
	}

	base := cfg.FTS.Snapshot.Path
	if base == "" {
		return ""
	}

	ext := filepath.Ext(base)
	if ext == "" {
		return base + ".filter"
	}

	return base[:len(base)-len(ext)] + ".filter" + ext
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
