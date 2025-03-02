package main

import (
	"context"
	"errors"
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/app"
	"fts-hw/internal/lib/logger/sl"
	"fts-hw/internal/services/fts"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
		os.Exit(1)
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

	if v, err := g.SetView("input", 2, 2, maxX-2, 4); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Search"
		v.Wrap = true
		_, _ = g.SetCurrentView("input")
	}

	if v, err := g.SetView("maxResults", 2, 5, maxX/4, 7); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Max Results"
		v.Wrap = true
	}

	if v, err := g.SetView("output", 2, 8, maxX-2, maxY-2); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = " Results "
		v.Wrap = true
		v.Clear()
	}

	return nil
}

func search(g *gocui.Gui, v *gocui.View, ctx context.Context, application *app.App) error {
	searchQuery = strings.TrimSpace(v.Buffer())

	results, elapsedTime := performSearch(searchQuery, ctx, application)

	outputView, err := g.View("output")
	if err != nil {
		return err
	}
	outputView.Clear()

	fmt.Fprintf(outputView, "\033[33mSearch Time: %s\033[0m\n\n", elapsedTime)
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

func highlightQueryInResult(result, query string) string {
	words := strings.Fields(query)
	for _, word := range words {
		result = strings.ReplaceAll(result, word, "\033[31m"+word+"\033[0m")
	}
	return result
}

func performSearch(query string, ctx context.Context, application *app.App) ([]fts.ResultDoc, time.Duration) {
	matchedDocs, elapsedTime, err := application.App.Search(ctx, query, maxResults)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	return matchedDocs, elapsedTime
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
