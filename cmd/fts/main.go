package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/app"
	"fts-hw/internal/cui"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/lib/logger/sl"
	"fts-hw/internal/services/loader"
	utils "fts-hw/internal/utils/clean"
	"fts-hw/internal/utils/metrics"
	"fts-hw/internal/workers"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

var searchQuery string

var totalEvents int
var totalFilteredEvents int
var eventsWithExtract int
var eventsWithoutExtract int

func main() {
	cfg := config.MustLoad()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	log := setupLogger(cfg.Env)
	log.Info("fts", "env", cfg.Env)

	application := app.New(log, cfg.StoragePath)
	log.Info("App initialised")

	dumpLoader := loader.NewLoader(log, cfg.Loader.FilePath)
	log.Info("Loader initialised")

	pool := workers.New(5)

	jobMetrics := metrics.New()

	startTime := time.Now()
	documents, err := dumpLoader.LoadDocuments()
	if err != nil {
		log.Error("Failed to load documents", "error", sl.Err(err))
		return
	}

	duration := time.Since(startTime)
	log.Info(fmt.Sprintf("Loaded %d documents in %v", len(documents), duration))

	startTime = time.Now()
	chunks := dumpLoader.ChunkDocuments(documents, cfg.ChunkSize)
	duration = time.Since(startTime)
	log.Info(fmt.Sprintf("Split %d documents in %d chunks in %v. Chunk size: %d", len(documents), len(chunks), duration, chunkSize))

	go func() {
		wg.Add(1)
		defer wg.Done()
		pool.Run(ctx)
	}()

	go func() {
		wg.Add(1)
		defer wg.Done()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-pool.Done:
				log.Info("Channel Done closed, exiting the loop.")
				return
			case <-ticker.C:
				log.Info("==============================================================")
				log.Info("Num CPUs", "count", runtime.NumCPU())
				log.Info("Num Goroutines", "count", runtime.NumGoroutine())
				log.Info("Memory usage", "bytes", pool.MemoryUsage())
				log.Info("Workers", "count", pool.ActiveWorkersCount())

				log.Info("Events Stats",
					"Events", totalEvents,
					"Filtered Events", totalFilteredEvents,
					"Events - Extract", eventsWithExtract,
					"Events - No Extract", eventsWithoutExtract)

				metricsStats := jobMetrics.PrintMetrics()
				log.Info("Job Metrics",
					"Total Jobs", metricsStats.TotalJobs,
					"Successful Jobs", metricsStats.SuccessfulJobs,
					"Failed Jobs", metricsStats.FailedJobs,
					"Avg Exec Time", metricsStats.AvgExecTime)
			}
		}
	}()

	for _, chunk := range chunks {
		job := workers.Job{
			Description: workers.JobDescriptor{
				ID:      workers.JobID(chunk[0].ID + "-" + chunk[len(chunk)-1].ID),
				JobType: "fetch_and_store",
			},
			ExecFn: func(ctx context.Context, chunk []models.Document) ([]string, error) {
				articleIDs := []string{}
				for _, doc := range chunk {
					startTime := time.Now()

					host, title, err := parseUrl(doc)
					if err != nil {
						jobMetrics.RecordFailure(time.Since(startTime))
						log.Error("Error parsing url", "error", sl.Err(err))
						return []string{""}, err
					}

					apiURL := fmt.Sprintf("%s/w/api.php?action=query&prop=extracts&explaintext=true&format=json&titles=%s",
						host, title)

					resp, err := http.Get(apiURL)
					if err != nil {
						jobMetrics.RecordFailure(time.Since(startTime))
						log.Error("Error getting url", "error", sl.Err(err))
						return []string{""}, err
					}
					defer resp.Body.Close()

					body, err := io.ReadAll(resp.Body)
					if err != nil {
						log.Error("Error reading body", "error", sl.Err(err))
						jobMetrics.RecordFailure(time.Since(startTime))
						return []string{""}, err
					}

					var apiResponse models.ArticleResponse
					if err := json.Unmarshal(body, &apiResponse); err != nil {
						log.Error("Error unmarshalling body", "error", sl.Err(err))
						jobMetrics.RecordFailure(time.Since(startTime))
						return []string{""}, err
					}

					for _, page := range apiResponse.Query.Pages {
						if page.Extract == "" {
							eventsWithoutExtract++
							jobMetrics.RecordFailure(time.Since(startTime))
							log.Error("Empty extract")
							return []string{""}, errors.New("empty extract in response")
						}

						eventsWithExtract++

						cleanExtract := utils.Clean(page.Extract)

						doc.Extract = cleanExtract

						articleID, err := application.App.ProcessDocument(ctx, doc, nil)

						articleIDs = append(articleIDs, articleID)

						if err != nil {
							log.Error("Error processing document", "error", sl.Err(err))
							jobMetrics.RecordFailure(time.Since(startTime))
							return []string{}, err
						}
						jobMetrics.RecordSuccess(time.Since(startTime))
					}
				}

				return articleIDs, nil
			},
			Args: &chunk,
		}
		pool.AddJob(&job)

	}

	fmt.Println("Indexing complete")

	cui := cui.New(&ctx, log, application, pool, 10)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		log.Info("Gracefully stopped")
		if err := application.StorageApp.Stop(); err != nil {
			log.Error("Failed to close database", "error", sl.Err(err))
		}
		cui.Close()
		cancel()
	}()

	cui.Start()
}

func parseUrl(doc models.Document) (host string, title string, err error) {
	parsedURL, err := url.Parse(doc.URL)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse URL: %v", err)
	}

	var hostBuilder strings.Builder

	hostBuilder.WriteString(parsedURL.Scheme)
	hostBuilder.WriteString("://")
	hostBuilder.WriteString(parsedURL.Host)

	host = hostBuilder.String()

	title = strings.TrimPrefix(parsedURL.Path, "/wiki/")

	return host, title, nil
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
