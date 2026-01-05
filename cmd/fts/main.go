package main

import (
	"context"
	"fmt"
	"fts-hw/internal/services/fts/kv"
	trigramtrie "fts-hw/internal/services/fts/trigram_trie"
	"fts-hw/internal/utils"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"fts-hw/config"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/lib/logger/sl"
	"fts-hw/internal/services/cui"
	ftsService "fts-hw/internal/services/fts"
	"fts-hw/internal/services/fts/loader"
	radixtrie "fts-hw/internal/services/fts/radix_trie"
	"fts-hw/internal/storage/leveldb"
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
	log.Info("fts", "engine-type", cfg.FTS.Trie.Type)
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
		switch cfg.FTS.Trie.Type {

		case "radix":
			trie := radixtrie.NewTrie()
			ftsEngine = ftsService.NewSearchService(
				trie,
				radixtrie.WordKeys,
			)

		case "trigram":
			trie := trigramtrie.NewTrie()
			ftsEngine = ftsService.NewSearchService(
				trie,
				trigramtrie.TrigramKeys,
			)
		}
	}

	log.Info("FTS engine initialised")

	dumpLoader := loader.NewLoader(log, cfg.DumpPath)
	log.Info("Loader initialised")

	startTime := time.Now()
	documents, err := dumpLoader.LoadDocuments(ctx)
	if err != nil {
		log.Error("Failed to load documents", "error", sl.Err(err))
		return
	}

	duration := time.Since(startTime)
	log.Info(fmt.Sprintf("Loaded %d documents in %v", len(documents), duration))

	if cfg.Mode.Type == "experiment" {
		memStats := utils.MeasureMemory(func() {
			for _, doc := range documents {
				_ = ftsEngine.IndexDocument(ctx, doc.ID, doc.Abstract)
			}
		})

		analyzeTrie(ftsEngine, memStats, log)
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
	engine cui.SearchEngine,
	memStats runtime.MemStats,
	log *slog.Logger,
) {
	svc, ok := engine.(*ftsService.SearchService)
	if !ok {
		log.Warn("analyzeTrie: engine does not support analysis")
		return
	}

	stats := svc.Analyse()

	log.Info("FTS analysis result",
		"engine", "radix",
		"nodes", stats.Nodes,
		"leafNodes", stats.LeafNodes,
		"maxDepth", stats.MaxDepth,
		"avgDepth", stats.AvgDepth,
		"totalDocs", stats.TotalDocs,
		"heapMB", memStats.HeapAlloc/1024/1024,
		"heapObjects", memStats.HeapObjects,
		"totalAllocMB", memStats.TotalAlloc/1024/1024,
	)

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
