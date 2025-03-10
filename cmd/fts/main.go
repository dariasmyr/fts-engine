package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/app"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/lib/logger/sl"
	"fts-hw/internal/services/fts"
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jroimartin/gocui"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

var searchQuery string
var maxResults = 10

var totalEvents int
var totalFilteredEvents int
var eventsWithExtract int
var eventsWithoutExtract int

const chunkSize = 1000

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

	pool := workers.New()

	jobMetrics := metrics.New()

	go func() {
		wg.Add(1)
		defer wg.Done()
		pool.Run(ctx)
	}()

	retry := 0

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

	startTime := time.Now()
	documents, err := dumpLoader.LoadDocuments()
	if err != nil {
		log.Error("Failed to load documents", "error", sl.Err(err))
		return
	}

	duration := time.Since(startTime)
	log.Info(fmt.Sprintf("Loaded %d documents in %v", len(documents), duration))

	startTime = time.Now()
	chunks := dumpLoader.ChunkDocuments(documents, chunkSize)
	duration = time.Since(startTime)
	log.Info(fmt.Sprintf("Split %d documents in %d chunks in %v. Chunk size: %d", len(documents), len(chunks), duration, chunkSize))

	for i, chunk := range chunks {
		startTime := time.Now()

		for _, doc := range chunk {
			job := workers.Job{
				Description: workers.JobDescriptor{
					ID:      workers.JobID(doc.ID),
					JobType: "fetch_and_store",
				},
				ExecFn: func(ctx context.Context, args models.Event) (string, error) {
					startTime := time.Now()

					host, title, err := parseUrl(doc)
					if err != nil {
						jobMetrics.RecordFailure(time.Since(startTime))
						return "", err
					}

					apiURL := fmt.Sprintf("%s/w/api.php?action=query&prop=extracts&explaintext=true&format=json&titles=%s",
						host, title)

					resp, err := http.Get(apiURL)
					if err != nil {
						jobMetrics.RecordFailure(time.Since(startTime))
						return "", err
					}
					defer resp.Body.Close()

					body, err := io.ReadAll(resp.Body)
					if err != nil {
						jobMetrics.RecordFailure(time.Since(startTime))
						return "", err
					}

					var apiResponse models.ArticleResponse
					if err := json.Unmarshal(body, &apiResponse); err != nil {
						jobMetrics.RecordFailure(time.Since(startTime))
						return "", err
					}

					for _, page := range apiResponse.Query.Pages {
						if page.Extract == "" {
							eventsWithoutExtract++
							jobMetrics.RecordFailure(time.Since(startTime))
							return "", errors.New("empty extract in response")
						}

						eventsWithExtract++

						cleanExtract := utils.Clean(page.Extract)

						doc.Extract = cleanExtract

						fmt.Printf("Saving doc ID: %s, Document Length: %d bytes\n", doc.ID, len(cleanExtract))

						result, err := application.App.ProcessDocument(ctx, doc, nil)

						if err != nil {
							jobMetrics.RecordFailure(time.Since(startTime))
							return "", err
						}
						jobMetrics.RecordSuccess(time.Since(startTime))
						return result, nil
					}

					jobMetrics.RecordFailure(time.Since(startTime))
					return "", errors.New("no valid pages found in response")
				},
				Args: &doc,
			}
			pool.AddJob(&job)
		}

		wg.Wait()

		err := pool.CloseLogFile()
		if err != nil {
			log.Error("Failed to close log file", "error", sl.Err(err))
		}

		duration = time.Since(startTime)
		log.Info(fmt.Sprintf("Processed chunk %d of %d in %v", i+1, len(chunks), duration))
	}

	fmt.Println("Indexing complete")

	//g, err := gocui.NewGui(gocui.OutputNormal)
	//if err != nil {
	//	log.Error("Failed to create GUI:", "error", sl.Err(err))
	//	os.Exit(1)
	//}
	//defer g.Close()
	//
	//g.Cursor = true
	//g.SetManagerFunc(layout)
	//
	//go func() {
	//	for {
	//		if err := updateDBInfo(g, application, &pool); err != nil {
	//			log.Error("Failed to update DB info", "error", sl.Err(err))
	//		}
	//		time.Sleep(2 * time.Second)
	//	}
	//}()
	//
	//if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
	//	log.Error("Failed to set keybinding:", "error", sl.Err(err))
	//}
	//if err := g.SetKeybinding("input", gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
	//	return search(g, v, ctx, application)
	//}); err != nil {
	//	log.Error("Failed to set keybinding:", "error", sl.Err(err))
	//}
	//
	//if err := g.SetKeybinding("output", gocui.KeyArrowDown, gocui.ModNone, scrollDown); err != nil {
	//	log.Error("Failed to set keybinding:", "error", sl.Err(err))
	//}
	//if err := g.SetKeybinding("output", gocui.KeyArrowUp, gocui.ModNone, scrollUp); err != nil {
	//	log.Error("Failed to set keybinding:", "error", sl.Err(err))
	//}
	//if err := g.SetKeybinding("maxResults", gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
	//	return setMaxResults(g, v, ctx, application)
	//}); err != nil {
	//	log.Error("Failed to set keybinding:", "error", sl.Err(err))
	//}
	//
	//if err := g.SetKeybinding("", gocui.KeyTab, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
	//	currentView := g.CurrentView().Name()
	//	if currentView == "input" {
	//		_, _ = g.SetCurrentView("maxResults")
	//	} else if currentView == "maxResults" {
	//		_, _ = g.SetCurrentView("output")
	//	} else {
	//		_, _ = g.SetCurrentView("input")
	//	}
	//	return nil
	//}); err != nil {
	//	log.Error("Failed to set keybinding:", "error", sl.Err(err))
	//}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		log.Info("Gracefully stopped")
		if err := application.StorageApp.Stop(); err != nil {
			log.Error("Failed to close database", "error", sl.Err(err))
		}
		cancel()
		//g.Close()
	}()

	//if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
	//	log.Error("Failed to run GUI:", "error", sl.Err(err))
	//}

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

func setMaxResults(g *gocui.Gui, v *gocui.View, ctx context.Context, application *app.App) error {
	maxResultsStr := strings.TrimSpace(v.Buffer())
	if maxResultsInt, err := strconv.Atoi(maxResultsStr); err == nil {
		maxResults = maxResultsInt
	}
	return nil
}

func scrollDown(g *gocui.Gui, v *gocui.View) error {
	_, oy := v.Origin()
	_, sy := v.Size()

	lines := len(v.BufferLines())

	if oy+sy < lines {
		v.SetOrigin(0, oy+1)
	}
	return nil
}

func scrollUp(g *gocui.Gui, v *gocui.View) error {
	_, oy := v.Origin()
	if oy > 0 {
		v.SetOrigin(0, oy-1)
	}
	return nil
}

func layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()

	if maxX < 10 || maxY < 6 {
		return fmt.Errorf("terminal window is too small")
	}

	if v, err := g.SetView("time", 0, 0, maxX/4, maxY-2); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "Time Measurements"
		v.Wrap = true
		v.Frame = true
	}

	if v, err := g.SetView("dbInfo", 0, 2, maxX/4, 2); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "DB Info"
		v.Wrap = false
		v.Frame = true
	}

	if v, err := g.SetView("input", maxX/4+1, 2, maxX-2, 4); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Search"
		v.Wrap = true
		_, _ = g.SetCurrentView("input")
	}

	if v, err := g.SetView("maxResults", maxX/4+1, 5, maxX/2, 7); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Max Results"
		v.Wrap = true

		fmt.Fprintf(v, "%d", maxResults)
	}

	if v, err := g.SetView("output", 0, 5, maxX-1, maxY-1); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "Results"
		v.Wrap = true
		v.Clear()
	}

	return nil
}

func getDatabaseStats(ctx context.Context, application *app.App) (string, error) {
	size, err := application.StorageApp.Storage().GetDatabaseStats(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Database stats: %s", size), nil
}

func updateDBInfo(g *gocui.Gui, application *app.App, workerPool *workers.WorkerPool) error {
	dbInfoView, err := g.View("dbInfo")
	if err != nil {
		return err
	}

	dbStats, err := getDatabaseStats(context.Background(), application)
	if err != nil {
		return err
	}

	dbInfoView.Clear()
	fmt.Fprintf(dbInfoView, "\033[33mDatabase Stats:\033[0m %s\n", dbStats)

	return nil
}

func search(g *gocui.Gui, v *gocui.View, ctx context.Context, application *app.App) error {
	searchQuery = strings.TrimSpace(v.Buffer())

	results, elapsedTime, totalResultsCount, err := performSearch(searchQuery, ctx, application)

	timeView, err := g.View("time")
	if err != nil {
		return err
	}
	timeView.Clear()

	fmt.Fprintln(timeView, "\033[33mSearch Time:\033[0m")

	for phase, duration := range elapsedTime {
		formattedDuration := formatDuration(duration)
		fmt.Fprintf(timeView, "\033[32m%s: %s\033[0m\n", phase, formattedDuration)
	}

	outputView, err := g.View("output")
	if err != nil {
		return err
	}
	outputView.Clear()

	fmt.Fprintf(outputView, "\033[33mTotal Results Count: %d\033[0m\n", totalResultsCount)

	for i, result := range results {
		if i >= maxResults {
			break
		}

		highlightedHeader := fmt.Sprintf("\033[32mDoc ID: %d | Unique Matches: %d | Total Matches: %d\033[0m\n",
			result.DocID, result.UniqueMatches, result.TotalMatches)
		fmt.Fprintf(outputView, "%s\n", highlightedHeader)

		highlightedResult := highlightQueryInResult(result.Doc, searchQuery)
		fmt.Fprintf(outputView, "%s\n\n", highlightedResult)
	}

	_, _ = g.SetCurrentView("input")
	return nil
}

// Format the duration into a human-readable string
func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%.3fns", float64(d)/float64(time.Nanosecond))
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.3fµs", float64(d)/float64(time.Microsecond))
	} else if d < time.Second {
		return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.3fs", float64(d)/float64(time.Second))
}

func highlightQueryInResult(result, query string) string {
	words := strings.Fields(query)
	for _, word := range words {
		result = strings.ReplaceAll(result, word, "\033[31m"+word+"\033[0m")
	}
	return result
}

func performSearch(query string, ctx context.Context, application *app.App) ([]fts.ResultDoc, map[string]time.Duration, int, error) {
	searchResult, err := application.App.Search(ctx, query, maxResults)
	if err != nil {
		return nil, nil, 0, err
	}
	return searchResult.ResultDocs, searchResult.Timings, searchResult.TotalResultsCount, nil
}

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
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
