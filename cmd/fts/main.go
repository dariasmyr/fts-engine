package main

import (
	"context"
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/services/loader"
	"sync"
	"time"

	//"fts-hw/internal/domain/models"
	"fts-hw/internal/lib/logger/sl"
	"fts-hw/internal/services/cui"
	ftsTrie "fts-hw/internal/services/fts_trie"
	"fts-hw/internal/storage/leveldb"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

const workerCount = 5

const (
	_readinessDrainDelay = 5 * time.Second
)

func main() {
	cfg := config.MustLoad()
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := setupLogger(cfg.Env)
	log.Info("fts", "env", cfg.Env)

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

	//keyValueFTS := ftsKV.New(log, storage, storage)
	// log.Info("Key Value FTS initialised")

	trieFTS := ftsTrie.NewNode()
	log.Info("Trie FTS initialised")

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
			trieFTS.IndexDocument(documents[i].ID, documents[i].Abstract)

			//_, err = keyValueFTS.ProcessDocument(ctx, &doc)

			log.Info("Document indexed, adding to job chan", "doc", i)
			jobCh <- documents[i]
		}
	}

	close(jobCh)
	wg.Wait()

	appCUI := cui.New(ctx, log, trieFTS, storage, 10)

	cuiErr := appCUI.Start()
	if cuiErr != nil {
		log.Error("Failed to start appCUI", "error", sl.Err(cuiErr))
		return
	}
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
