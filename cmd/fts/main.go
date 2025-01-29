package main

import (
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/lib/logger/sl"
	"fts-hw/internal/storage/leveldb"
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

	log := setupLogger(cfg.Env)

	log.Info("fts", "env", cfg.Env)

	db, err := leveldb.New(cfg.StoragePath)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Error("Failed to close database", "error", sl.Err(err))
		}
	}()

	log.Info("Database initialised")

	id1, _ := db.AddDocument("hello world")
	id2, _ := db.AddDocument("hello go")
	fmt.Println("New document ", id2)

	results, _ := db.Search("hello")
	fmt.Println("Результаты поиска:", results)

	db.DeleteDocument(id1)
	results, _ = db.Search("hello")
	fmt.Println("После удаления:", results)

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	// Waiting for SIGINT (pkill -2) or SIGTERM
	<-stop

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
