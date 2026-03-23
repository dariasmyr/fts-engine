package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/dariasmyr/fts-engine/internal/services/fts/kv"
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
	"sync"
	"syscall"
	"time"

	"github.com/dariasmyr/fts-engine/config"
	"github.com/dariasmyr/fts-engine/internal/adapters/cui"
	"github.com/dariasmyr/fts-engine/internal/adapters/loader/wiki"
	"github.com/dariasmyr/fts-engine/internal/adapters/storage/leveldb"
	"github.com/dariasmyr/fts-engine/internal/domain/models"
	"github.com/dariasmyr/fts-engine/internal/lib/logger/sl"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
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
	cfg := config.MustLoad()

	ensureDir("data")

	var workerCount = runtime.NumCPU()

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := setupLogger(cfg.Env)
	log.Info("fts", "env", cfg.Env)
	log.Info("fts", "engine", cfg.FTS.Engine)
	log.Info("fts", "index", cfg.FTS.Index)
	log.Info("fts", "keygen", cfg.FTS.KeyGen)
	log.Info("fts", "filter", cfg.FTS.Filter)
	log.Info("fts", "mode", cfg.Mode.Type)

	storage, err := leveldb.NewStorage(log, cfg.StoragePath)
	if err != nil {
		panic(err)
	}
	log.Info("Storage initialised")

	go func() {
		<-rootCtx.Done()
		stop()
		log.Info("Received shutdown signal, shutting down...")

		time.Sleep(_readinessDrainDelay)
		log.Info("Readiness check propagated, now waiting for ongoing processes to finish.")

		closeStorageErr := storage.Close()
		if closeStorageErr != nil {
			log.Error("Failed to close database", "error", sl.Err(closeStorageErr))
		}

		cancel()
	}()

	var ftsEngine cui.SearchEngine

	switch cfg.FTS.Engine {

	case "kv":
		ftsEngine = kv.New(log, storage, storage)
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
		ftsEngine = &serviceAdapter{service: svc, snapshotLoaded: loadedFromSnapshot}
	}

	log.Info("FTS engine initialised")

	dumpLoader := wiki.New(log, cfg.DumpPath)
	log.Info("Loader initialised")

	startTime := time.Now()
	documents, err := dumpLoader.LoadDocuments(ctx)
	if err != nil {
		log.Error("Failed to load documents", "error", sl.Err(err))
		return
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

	adapter, ok := ftsEngine.(*serviceAdapter)
	if !ok {
		log.Error("unexpected search engine type")
		return
	}

	if !adapter.snapshotLoaded {
		log.Info("Initialize worker pool")
		jobCh := make(chan models.Document)
		var wg sync.WaitGroup
		for range workerCount {
			select {
			case <-rootCtx.Done():
				log.Info("Received shutdown signal, shutting down...")
				return
			default:
				wg.Add(1)
				go func() {
					defer wg.Done()
					fmt.Println("Starting worker")
					storage.BatchDocument(ctx, jobCh)
				}()
			}
		}

		for i := range documents {
			select {
			case <-rootCtx.Done():
				log.Info("Received shutdown signal, shutting down...")
				return
			default:
				indexErr := ftsEngine.IndexDocument(ctx, documents[i].ID, documents[i].Abstract)
				if indexErr != nil {
					log.Error("could not index document:", "error", indexErr)
				}

				jobCh <- documents[i]
			}
		}

		close(jobCh)
		wg.Wait()

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

	appCUI := cui.New(ctx, log, ftsEngine, storage, 10)

	cuiErr := appCUI.Start()
	if cuiErr != nil {
		log.Error("Failed to start appCUI", "error", sl.Err(cuiErr))
		return
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
}

func (s *serviceAdapter) IndexDocument(ctx context.Context, docID string, content string) error {
	return s.service.IndexDocument(ctx, pkgfts.DocID(docID), content)
}

func (s *serviceAdapter) SearchDocuments(ctx context.Context, query string, maxResults int) (*models.SearchResult, error) {
	result, err := s.service.SearchDocuments(ctx, query, maxResults)
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
		Timings:           result.Timings,
	}, nil
}

func (s *serviceAdapter) AnalyzeStats() (pkgfts.Stats, bool) {
	return s.service.Analyze()
}

func buildService(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, pipeline textproc.Pipeline) (*pkgfts.Service, bool, error) {
	if cfg == nil {
		return nil, false, fmt.Errorf("nil config")
	}

	if cfg.Mode.Type == "prod" && cfg.FTS.Snapshot.Enabled && cfg.FTS.Snapshot.LoadOnStart {
		svc, ok, err := tryLoadSnapshot(log, cfg, keyGen, pipeline)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return svc, true, nil
		}
	}

	index, err := ftsbuiltin.BuildIndex(cfg.FTS.Index)
	if err != nil {
		return nil, false, err
	}

	searchFilter, err := selectFilter(cfg)
	if err != nil {
		return nil, false, err
	}

	svc := pkgfts.New(index, keyGen, pkgfts.WithPipeline(pipeline), pkgfts.WithFilter(searchFilter))
	return svc, false, nil
}

func tryLoadSnapshot(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, pipeline textproc.Pipeline) (*pkgfts.Service, bool, error) {
	if useSplitSnapshotFiles(cfg) {
		return tryLoadSplitSnapshot(log, cfg, keyGen, pipeline)
	}

	path := cfg.FTS.Snapshot.Path
	if path == "" {
		return nil, false, nil
	}

	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("check snapshot path: %w", err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, false, fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	loaded, err := ftsbuiltin.LoadSegmentSnapshot(f)
	if err != nil {
		return nil, false, fmt.Errorf("load snapshot: %w", err)
	}

	if loaded.IndexName != cfg.FTS.Index {
		log.Warn("Snapshot index type differs from config",
			"snapshot_index", loaded.IndexName,
			"config_index", cfg.FTS.Index,
			"path", path,
		)
	}

	configFilter := cfg.FTS.Filter
	if configFilter == "none" {
		configFilter = ""
	}
	if loaded.FilterName != configFilter {
		log.Warn("Snapshot filter type differs from config",
			"snapshot_filter", loaded.FilterName,
			"config_filter", cfg.FTS.Filter,
			"path", path,
		)
	}

	builtOpts := []pkgfts.Option{pkgfts.WithPipeline(pipeline)}
	if loaded.Filter != nil {
		builtOpts = append(builtOpts, pkgfts.WithFilter(loaded.Filter))
	}

	svc := pkgfts.New(loaded.Index, keyGen, builtOpts...)

	log.Info("Loaded FTS snapshot", "path", path)
	return svc, true, nil
}

func tryLoadSplitSnapshot(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, pipeline textproc.Pipeline) (*pkgfts.Service, bool, error) {
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

	loadedIndex, err := ftsbuiltin.LoadIndexSnapshot(indexFile)
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

	builtOpts := []pkgfts.Option{pkgfts.WithPipeline(pipeline)}

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

		loadedFilter, loadErr := ftsbuiltin.LoadFilterSnapshot(filterFile)
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

			loadedFilter, loadErr := ftsbuiltin.LoadFilterSnapshot(filterFile)
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

	if useSplitSnapshotFiles(cfg) {
		return saveSplitSnapshots(log, cfg, svc)
	}

	filterName := cfg.FTS.Filter
	if filterName == "none" {
		filterName = ""
	}

	opts := ftspersist.SaveOptions{
		BufferSize:     cfg.FTS.Snapshot.BufferSize,
		FlushThreshold: cfg.FTS.Snapshot.FlushThreshold,
		SyncFile:       cfg.FTS.Snapshot.SyncFile,
	}

	if err := ftspersist.SaveAtomicWithOptions(cfg.FTS.Snapshot.Path, opts, func(w io.Writer) error {
		return ftsbuiltin.SaveServiceSnapshot(w, svc, cfg.FTS.Index, filterName)
	}); err != nil {
		return err
	}

	log.Info("FTS snapshot persisted", "path", cfg.FTS.Snapshot.Path)
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

	opts := ftspersist.SaveOptions{
		BufferSize:     cfg.FTS.Snapshot.BufferSize,
		FlushThreshold: cfg.FTS.Snapshot.FlushThreshold,
		SyncFile:       cfg.FTS.Snapshot.SyncFile,
	}

	if err := ftspersist.SaveAtomicWithOptions(indexPath, opts, func(w io.Writer) error {
		return ftsbuiltin.SaveIndexSnapshot(w, indexName, index)
	}); err != nil {
		return err
	}

	if searchFilter != nil && filterName != "" {
		if err := ftspersist.SaveAtomicWithOptions(filterPath, opts, func(w io.Writer) error {
			return ftsbuiltin.SaveFilterSnapshot(w, filterName, searchFilter)
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

func useSplitSnapshotFiles(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}

	if cfg.FTS.Snapshot.SplitFiles {
		return true
	}

	return cfg.FTS.Snapshot.IndexPath != "" || cfg.FTS.Snapshot.FilterPath != ""
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
	case "trigram":
		return keygen.Trigram, nil
	default:
		return nil, fmt.Errorf("unknown keygen %q", kind)
	}
}

func selectFilter(cfg *config.Config) (pkgfts.Filter, error) {
	if cfg == nil {
		return nil, nil
	}

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
