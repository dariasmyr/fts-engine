package main

import (
	"context"
	"errors"
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/app"
	"fts-hw/internal/lib/logger/sl"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jroimartin/gocui"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

var searchQuery string

const resultsPerPage = 10

var currentPage int

func main() {
	cfg := config.MustLoad()

	ctx := context.Background()

	log := setupLogger(cfg.Env)

	log.Info("fts", "env", cfg.Env)

	application := app.New(log, cfg.StoragePath)

	log.Info("Database initialised")

	log.Info("Loader initialised")

	fmt.Println("Starting simple fts")

	g, err := gocui.NewGui(gocui.OutputNormal)
	if err != nil {
		log.Error("Failed to create GUI:", "error", sl.Err(err))
	}
	defer g.Close()

	g.Cursor = true

	g.SetManagerFunc(layout)

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}
	if err := g.SetKeybinding("input", gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		return search(g, v, ctx, application)
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
		g.Close()
	}()

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Error("Failed to run GUI:", "error", sl.Err(err))
	}
}

func layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()

	if maxX < 10 || maxY < 6 {
		return fmt.Errorf("terminal window is too small")
	}

	if v, err := g.SetView("sidebar", 0, 0, 20, maxY-1); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "Databases"
		v.Highlight = true
		v.SelFgColor = gocui.ColorGreen
	}

	if v, err := g.SetView("input", 22, 2, maxX-2, 4); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Search"
		v.Wrap = true
		_, _ = g.SetCurrentView("input")
	}

	if v, err := g.SetView("output", 22, 5, maxX-2, maxY-2); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = " Results "
		v.Autoscroll = true
		v.Wrap = true
	}

	return nil
}

func search(g *gocui.Gui, v *gocui.View, ctx context.Context, application *app.App) error {
	searchQuery = strings.TrimSpace(v.Buffer())
	results := performSearch(searchQuery, ctx, application)

	outputView, err := g.View("output")
	if err != nil {
		return err
	}
	outputView.Clear()

	startIdx := currentPage * resultsPerPage
	endIdx := startIdx + resultsPerPage
	if endIdx > len(results) {
		endIdx = len(results)
	}

	for _, result := range results[startIdx:endIdx] {
		highlightedResult := highlightQueryInResult(result, searchQuery)

		source := getResultDatabaseSource(result)

		fmt.Fprintf(outputView, "%s\nFrom: %s\n\n", highlightedResult, source)
	}

	fmt.Fprintf(outputView, "\nPage %d of %d\n", currentPage+1, (len(results)/resultsPerPage)+1)

	return nil
}

func highlightQueryInResult(result, query string) string {
	words := strings.Fields(query)

	for _, word := range words {
		result = strings.ReplaceAll(result, word, "\033[31m"+word+"\033[0m")
	}

	return result
}

func getResultDatabaseSource(result string) string {
	if strings.Contains(result, "local") {
		return "Local DB"
	} else if strings.Contains(result, "dev") {
		return "Development DB"
	} else {
		return "Production DB"
	}
}

func performSearch(query string, ctx context.Context, application *app.App) []string {
	matchedDocs, err := application.App.Search(ctx, query)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}

	return matchedDocs
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
