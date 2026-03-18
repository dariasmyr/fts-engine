package main

import (
	"context"
	"fmt"
	"github.com/dariasmyr/fts-engine/internal/services/fts/kv"
	"github.com/dariasmyr/fts-engine/internal/utils"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
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
	pkgfilter "github.com/dariasmyr/fts-engine/pkg/filter"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/hamt"
	"github.com/dariasmyr/fts-engine/pkg/index/hamtpointered"
	"github.com/dariasmyr/fts-engine/pkg/index/radix"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/index/trigram"
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
		registerBuiltInIndexes()
		registerBuiltInFilters(cfg)

		index, err := pkgfts.NewIndex(cfg.FTS.Index)
		if err != nil {
			log.Error("Failed to create index", "error", sl.Err(err))
			return
		}

		keyGen, err := selectKeyGenerator(cfg.FTS.KeyGen)
		if err != nil {
			log.Error("Failed to select keygen", "error", sl.Err(err))
			return
		}

		searchFilter, err := selectFilter(cfg)
		if err != nil {
			log.Error("Failed to select filter", "error", sl.Err(err))
			return
		}

		pipeline := buildPipeline(cfg)
		svc := pkgfts.New(index, keyGen, pkgfts.WithPipeline(pipeline), pkgfts.WithFilter(searchFilter))
		ftsEngine = &serviceAdapter{service: svc}
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

			// log.Info("Document indexed, adding to job chan", "doc", i)

			jobCh <- documents[i]
		}
	}

	close(jobCh)
	wg.Wait()

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
	service *pkgfts.Service
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

func registerBuiltInIndexes() {
	register := func(name string, factory pkgfts.IndexFactory) {
		if pkgfts.IsIndexRegistered(name) {
			return
		}
		if err := pkgfts.RegisterIndex(name, factory); err != nil {
			panic(err)
		}
	}

	register("radix", func() (pkgfts.Index, error) { return radix.New(), nil })
	register("slicedradix", func() (pkgfts.Index, error) { return slicedradix.New(), nil })
	register("hamt", func() (pkgfts.Index, error) { return hamt.New(), nil })
	register("hamtpointered", func() (pkgfts.Index, error) { return hamtpointered.New(), nil })
	register("trigram", func() (pkgfts.Index, error) { return trigram.New(), nil })
}

func registerBuiltInFilters(cfg *config.Config) {
	if cfg == nil {
		return
	}

	register := func(name string, factory pkgfilter.Factory) {
		if pkgfilter.IsRegistered(name) {
			return
		}
		if err := pkgfilter.Register(name, factory); err != nil {
			panic(err)
		}
	}

	register("bloom", func() (pkgfilter.Filter, error) {
		return pkgfilter.NewBloomFilter(
			cfg.FTS.Bloom.ExpectedItems,
			cfg.FTS.Bloom.BitsPerItem,
			cfg.FTS.Bloom.K,
		), nil
	})
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
	if cfg == nil || cfg.FTS.Filter == "" || cfg.FTS.Filter == "none" {
		return nil, nil
	}

	return pkgfilter.New(cfg.FTS.Filter)
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

	if cfg.FTS.Pipeline.StemEN {
		filters = append(filters, textproc.EnglishStemFilter{})
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
