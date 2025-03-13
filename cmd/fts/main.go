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
	log.Info("Key Value FTS initialised")

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

	for _, doc := range documents {
		trieFTS.IndexDocument(doc.ID, doc.Abstract)
		//articleID, err := storage.BatchDocument(context.Background(), &doc)
		if err != nil {
			log.Error("Error processing document", "error", sl.Err(err))
			continue
		}

	}

	//var wg sync.WaitGroup

	//pool := workers.New(runtime.NumCPU() * 2)
	//
	//wg.Add(1)
	//go func() {
	//	defer wg.Done()
	//	pool.Run(ctx)
	//}()

	// Uncomment this to process document with key value fts
	//for i, doc := range documents {
	//	log.Info("Starting job", "doc", i)
	//	job := workers.Job{
	//		Description: workers.JobDescriptor{
	//			ID:      workers.JobID(strconv.Itoa(i)),
	//			JobType: "fetch_and_store",
	//		},
	//		ExecFn: func(ctx context.Context, doc models.Document) (string, error) {
	//
	//			//Uncomment this if you want fetch Extract (extended article text) from Wikimedia API
	//			//doc, err = dumpLoader.FetchAndProcessDocument(ctx, doc)
	//			//if err != nil {
	//			//	return "", err
	//			//}
	//
	//			var articleID string
	//
	//			articleID, err = keyValueFTS.ProcessDocument(ctx, &doc)
	//
	//			if err != nil {
	//				log.Error("Error processing document", "error", sl.Err(err))
	//				return "", err
	//			}
	//
	//			return articleID, nil
	//		},
	//		Args: doc,
	//	}
	//	pool.AddJob(job)
	//}

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
