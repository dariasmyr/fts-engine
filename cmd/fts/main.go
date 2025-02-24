package main

import (
	"context"
	"flag"
	"fmt"
	"fts-hw/config"
	"fts-hw/internal/app"
	"fts-hw/internal/lib/logger/sl"
	"github.com/jroimartin/gocui"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

var searchQuery string
var results []string

func main() {
	cfg := config.MustLoad()

	ctx := context.Background()

	log := setupLogger(cfg.Env)

	log.Info("fts", "env", cfg.Env)

	application := app.New(log, cfg.StoragePath)

	log.Info("Database initialised")

	log.Info("Loader initialised")

	var query string
	flag.StringVar(&query, "q", "Small wild cat", "search query")
	flag.Parse()

	fmt.Println("Starting simple fts")

	g, err := gocui.NewGui(gocui.OutputNormal)
	if err != nil {
		log.Error("Failed to create GUI:", err)
	}
	defer g.Close()

	g.SetManagerFunc(layout)

	if err := g.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		log.Error("Failed to set keybinding:", err)
	}
	if err := g.SetKeybinding("input", gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		return search(g, v, ctx, application)
	}); err != nil {
		log.Error("Failed to set keybinding:", err)
	}

	if err := g.MainLoop(); err != nil && err != gocui.ErrQuit {
		log.Error("Failed to run GUI:", err)
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

func layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()

	if maxX < 10 || maxY < 6 {
		return fmt.Errorf("terminal window is too small")
	}

	if v, err := g.SetView("input", 2, 2, maxX-2, 4); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Editable = true
		v.Title = "Search"
		_, _ = g.SetCurrentView("input")
	}

	if v, err := g.SetView("output", 2, 5, maxX-2, maxY-2); err != nil {
		if err != gocui.ErrUnknownView {
			return err
		}
		v.Title = "Results"
	}

	return nil
}

func search(g *gocui.Gui, v *gocui.View, ctx context.Context, application *app.App) error {
	searchQuery = strings.TrimSpace(v.Buffer())
	results = performSearch(searchQuery, ctx, application)

	outputView, err := g.View("output")
	if err != nil {
		return err
	}
	outputView.Clear()

	fmt.Fprintln(outputView, "Search Results:")
	for i, result := range results {
		docID := fmt.Sprintf("Doc %d", i+1)
		words := strings.Count(result, " ") + 1
		totalResults := len(results)

		truncatedResult := truncateText(result, 100)
		fmt.Fprintf(outputView, "%s (Words: %d, Total Results: %d) | %s\n", docID, words, totalResults, truncatedResult)
	}
	return nil
}

// Function to truncate result text if it's too long
func truncateText(text string, maxLength int) string {
	if len(text) > maxLength {
		return text[:maxLength] + "..."
	}
	return text
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
