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
	"fts-hw/internal/workers"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/r3labs/sse/v2"

	"github.com/jroimartin/gocui"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

var searchQuery string
var maxResults = 10

func main() {
	cfg := config.MustLoad()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := setupLogger(cfg.Env)
	log.Info("fts", "env", cfg.Env)

	application := app.New(log, cfg.StoragePath)
	log.Info("App initialised")

	client := sse.NewClient("https://stream.wikimedia.org/v2/stream/recentchange")
	jobs := make(chan *workers.Job, 100)
	pool := workers.New(100)

	allowedServers := map[string]struct{}{
		"https://www.mediawiki.org":     {},
		"https://meta.wikimedia.org":    {},
		"https://en.wikipedia.org":      {},
		"https://nl.wikipedia.org":      {},
		"https://commons.wikimedia.org": {},
		"https://test.wikipedia.org":    {},
	}

	go func() {
		err := client.SubscribeRaw(func(msg *sse.Event) {

			log.Debug("MESSAGE:", "message", msg.Data)

			var event models.Event
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				log.Error("Failed to unmarshal event", "error", sl.Err(err))
				return
			}

			// Ignore events from the "canary" domain
			if event.Meta.Domain == "canary" {
				return
			}

			if _, ok := allowedServers[event.ServerURL]; !ok {
				// log.Info("Ignored event from non-allowed server", "serverURL", event.ServerURL)
				return
			}

			log.Debug("Filtered event:", "event", event)

			job := workers.Job{
				Description: workers.JobDescriptor{
					ID:      workers.JobID(event.Meta.ID),
					JobType: "fetch_and_store",
				},
				ExecFn: func(ctx context.Context, args models.Event) (string, error) {
					apiURL := fmt.Sprintf("%s/w/api.php?action=query&prop=extracts&explaintext=true&format=json&titles=%s", args.ServerURL, args.Title)

					resp, err := http.Get(apiURL)
					if err != nil {
						return "", err
					}
					defer resp.Body.Close()

					body, err := io.ReadAll(resp.Body)
					if err != nil {
						return "", err
					}

					var doc models.Article
					if err := json.Unmarshal(body, &doc); err != nil {
						return "", err
					}

					log.Debug("Article extract", "exctart", doc.Extract)

					return application.App.AddDocument(ctx, doc.Extract, body, nil)
				},
				Args: &event,
			}
			jobs <- &job
		})
		if err != nil {
			log.Error("Failed to subscribe to events", "error", sl.Err(err))
		}
	}()

	go func() {
		pool.Run(ctx)
	}()

	fmt.Println("Starting simple fts")

	g, err := gocui.NewGui(gocui.OutputNormal)
	if err != nil {
		log.Error("Failed to create GUI:", "error", sl.Err(err))
		os.Exit(1)
	}
	defer g.Close()

	g.Cursor = true
	g.SetManagerFunc(layout)

	go func() {
		for {
			if err := updateDBInfo(g, application, &pool); err != nil {
				log.Error("Failed to update DB info", "error", sl.Err(err))
			}
			time.Sleep(2 * time.Second)
		}
	}()

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}
	if err := g.SetKeybinding("input", gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		return search(g, v, ctx, application)
	}); err != nil {
		log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}

	if err := g.SetKeybinding("output", gocui.KeyArrowDown, gocui.ModNone, scrollDown); err != nil {
		log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}
	if err := g.SetKeybinding("output", gocui.KeyArrowUp, gocui.ModNone, scrollUp); err != nil {
		log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}
	if err := g.SetKeybinding("maxResults", gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		return setMaxResults(g, v, ctx, application)
	}); err != nil {
		log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}

	if err := g.SetKeybinding("", gocui.KeyTab, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		currentView := g.CurrentView().Name()
		if currentView == "input" {
			_, _ = g.SetCurrentView("maxResults")
		} else if currentView == "maxResults" {
			_, _ = g.SetCurrentView("output")
		} else {
			_, _ = g.SetCurrentView("input")
		}
		return nil
	}); err != nil {
		log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		log.Info("Gracefully stopped")
		if err := application.StorageApp.Stop(); err != nil {
			log.Error("Failed to close database", "error", sl.Err(err))
		}
		cancel()
		g.Close()
	}()

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Error("Failed to run GUI:", "error", sl.Err(err))
	}

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

	if v, err := g.SetView("time", 0, 0, maxX/2, maxY/4); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "Time Measurements"
		v.Wrap = true
		v.Frame = true
	}

	if v, err := g.SetView("input", maxX/2+1, 0, maxX-2, maxY/4); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Search"
		v.Wrap = true
		_, _ = g.SetCurrentView("input")
	}

	if v, err := g.SetView("maxResults", 0, maxY/4+1, maxX/2, maxY/4+2); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Max Results"
		v.Wrap = true
		fmt.Fprintf(v, "%d", maxResults)
	}

	if v, err := g.SetView("output", maxX/2+1, maxY/4+1, maxX-2, maxY/2); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "Results"
		v.Wrap = true
		v.Clear()
	}

	if v, err := g.SetView("dbInfo", 0, maxY/2+1, maxX-2, maxY-1); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "DB Info"
		v.Wrap = false
		v.Frame = true
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

	processedDocsCount := workerPool.ProcessedTasksCount
	if err != nil {
		return err
	}

	dbInfoView.Clear()
	fmt.Fprintf(dbInfoView, "\033[33mDatabase Stats:\033[0m %s\n", dbStats)
	fmt.Fprintf(dbInfoView, "\033[33mProcessed Documents:\033[0m %d\n", processedDocsCount)

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
