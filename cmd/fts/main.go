package main

import (
	"context"
	"flag"
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/app"
	"fts-hw/internal/lib/logger/sl"
	"fts-hw/internal/services/loader"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func main() {
	cfg := config.MustLoad()

	ctx := context.Background()

	log := setupLogger(cfg.Env)

	log.Info("fts", "env", cfg.Env)

	application := app.New(log, cfg.StoragePath)

	log.Info("Database initialised")

	loader := loader.NewLoader(log, cfg.Loader.FilePath)

	log.Info("Loader initialised")

	var query string
	flag.StringVar(&query, "q", "Small wild cat", "search query")
	flag.Parse()

	fmt.Println("Starting simple fts")

	start := time.Now()
	docs, err := loader.LoadDocuments()
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d documents in %v\n", len(docs), time.Since(start))

	start = time.Now()
	fmt.Printf("Start indexing %d documents\n", len(docs))
	for _, doc := range docs {
		_, err := application.App.AddDocument(ctx, doc.Text)
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(1)
		}

	}
	fmt.Printf("Indexed %d documents in %v\n", len(docs), time.Since(start))

	start = time.Now()
	matchedDocs, err := application.App.Search(ctx, query)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	fmt.Printf("Search found %d documents in %v\n", len(matchedDocs), time.Since(start))

	for _, doc := range matchedDocs {
		fmt.Println(doc)
	}

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	// Waiting for SIGINT (pkill -2) or SIGTERM
	<-stop
	if err := application.StorageApp.Stop(); err != nil {
		log.Error("Failed to close database", "error", sl.Err(err))
	}

	// initiate graceful shutdown
	log.Info("Gracefully stopped")
}

func setupLogger(env string) *slog.Logger {
	var log *slog.Logger

	switch env {
	case envLocal:
		log = slog.New(
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envDev:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envProd:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	}

	return log
}
