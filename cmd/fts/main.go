package main

import (
	"context"
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/lib/logger/sl"
	"fts-hw/internal/services/cui"
	ftsTrie "fts-hw/internal/services/fts_trie"
	"fts-hw/internal/services/loader"
	"time"

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

func main() {
	cfg := config.MustLoad()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := setupLogger(cfg.Env)
	log.Info("fts", "env", cfg.Env)

	storage, err := leveldb.NewStorage(log, cfg.StoragePath)
	if err != nil {
		panic(err)
	}
	log.Info("Storage initialised")

	//keyValueFTS := ftsKV.New(log, storage, storage)
	// log.Info("Key Value FTS initialised")

	trieFTS := ftsTrie.NewNode()
	log.Info("Trie FTS initialised")

	dumpLoader := loader.NewLoader(log, cfg.DumpPath)
	log.Info("Loader initialised")

	startTime := time.Now()
	documents, err := dumpLoader.LoadDocuments()
	if err != nil {
		log.Error("Failed to load documents", "error", sl.Err(err))
		return
	}

	duration := time.Since(startTime)
	log.Info(fmt.Sprintf("Loaded %d documents in %v", len(documents), duration))

	startTime = time.Now()
	duration = time.Since(startTime)
	log.Info(fmt.Sprintf("Split %d documents. in %v", len(documents), duration))

	for i, doc := range documents {
		trieFTS.IndexDocument(doc.ID, doc.Abstract)

		//_, err = keyValueFTS.ProcessDocument(ctx, &doc)

		log.Info("Document indexed, adding to batch", "doc", i)
		_, err := storage.BatchDocument(context.Background(), &doc)
		if err != nil {
			log.Error("Error processing document", "error", sl.Err(err))
			continue
		}
	}

	storage.StopWorkers()
	log.Info("All write workers stopped")

	fmt.Println("Indexing complete")

	appCUI := cui.New(&ctx, log, trieFTS, storage, 10)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stop
		log.Info("Gracefully stopped")
		if err := storage.Close(); err != nil {
			log.Error("Failed to close database", "error", sl.Err(err))
		}
		appCUI.Close()
		cancel()
	}()

	appCUI.Start()
}

func setupLogger(env string) *slog.Logger {
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
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
