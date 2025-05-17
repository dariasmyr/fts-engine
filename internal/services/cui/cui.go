package cui

import (
	"context"
	"errors"
	"fmt"
	"fts-hw/internal/domain/models"
	"fts-hw/internal/lib/logger/sl"
	fts "fts-hw/internal/services/fts_trie"
	"fts-hw/internal/storage/leveldb"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/jroimartin/gocui"
)

type CUI struct {
	ctx        context.Context
	cui        *gocui.Gui
	ftsSerivce *fts.Node
	storage    *leveldb.Storage
	log        *slog.Logger
	maxResults int
}

func New(ctx context.Context, log *slog.Logger, ftsSerivce *fts.Node, storage *leveldb.Storage, maxResults int) *CUI {
	g, err := gocui.NewGui(gocui.OutputNormal)
	if err != nil {
		log.Error("Failed to create GUI:", "error", sl.Err(err))
		os.Exit(1)
	}
	return &CUI{
		ctx:        ctx,
		cui:        g,
		ftsSerivce: ftsSerivce,
		storage:    storage,
		log:        log,
		maxResults: maxResults,
	}
}

func (c *CUI) Close() {
	c.cui.Close()
}

func (c *CUI) Start() error {
	c.cui.Cursor = true
	c.cui.SetManagerFunc(c.layout)
	defer c.cui.Close()

	if err := c.cui.SetKeybinding("", gocui.KeyCtrlC, gocui.ModNone, quit); err != nil {
		c.log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}
	if err := c.cui.SetKeybinding("input", gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		searchQuery := strings.TrimSpace(v.Buffer())
		return c.search(g, v, c.ctx, searchQuery)
	}); err != nil {
		c.log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}

	if err := c.cui.SetKeybinding("output", gocui.KeyArrowDown, gocui.ModNone, scrollDown); err != nil {
		c.log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}
	if err := c.cui.SetKeybinding("output", gocui.KeyArrowUp, gocui.ModNone, scrollUp); err != nil {
		c.log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}
	if err := c.cui.SetKeybinding("maxResults", gocui.KeyEnter, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
		return c.setMaxResults(g, v)
	}); err != nil {
		c.log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}

	if err := c.cui.SetKeybinding("", gocui.KeyTab, gocui.ModNone, func(g *gocui.Gui, v *gocui.View) error {
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
		c.log.Error("Failed to set keybinding:", "error", sl.Err(err))
	}

	if err := c.cui.MainLoop(); err != nil && err != gocui.ErrQuit {
		c.log.Error("Failed to run GUI:", "error", sl.Err(err))
	}

	return nil
}

func (c *CUI) setMaxResults(g *gocui.Gui, v *gocui.View) error {
	maxResultsStr := strings.TrimSpace(v.Buffer())
	if maxResultsInt, err := strconv.Atoi(maxResultsStr); err == nil {
		c.maxResults = maxResultsInt
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

func (c *CUI) layout(g *gocui.Gui) error {
	maxX, maxY := g.Size()

	if maxX < 10 || maxY < 6 {
		return fmt.Errorf("terminal window is too small")
	}

	// Left Sidebar for Time Measurement
	if v, err := g.SetView("time", 0, 0, maxX/4, maxY-2); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "Time Measurements"
		v.Wrap = true
		v.Frame = true
	}

	// Search Input - Right side, top
	if v, err := g.SetView("input", maxX/4+1, 2, maxX-2, 4); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Search"
		v.Wrap = true
		_, _ = g.SetCurrentView("input")
	}

	// Max Results Input - Right side, below search input
	if v, err := g.SetView("maxResults", maxX/4+1, 5, maxX/2, 7); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Editable = true
		v.Title = "Max Results"
		v.Wrap = true

		fmt.Fprintf(v, "%d", c.maxResults)
	}

	// Output View - Right side, below max results
	if v, err := g.SetView("output", maxX/4+1, 8, maxX-2, maxY-2); err != nil {
		if !errors.Is(err, gocui.ErrUnknownView) {
			return err
		}
		v.Title = "Results"
		v.Wrap = true
		v.Clear()
	}

	return nil
}

func (c *CUI) search(g *gocui.Gui, v *gocui.View, ctx context.Context, searchQuery string) error {
	searchQuery = strings.TrimSpace(v.Buffer())

	results, elapsedTime, totalResultsCount, err := c.performSearch(searchQuery, ctx)

	timeView, err := g.View("time")
	if err != nil {
		return err
	}
	timeView.Clear()

	fmt.Fprintln(timeView, "\033[33mSearch Time:\033[0m")

	for phase, duration := range elapsedTime {
		fmt.Fprintf(timeView, "\033[32m%s: %s\033[0m\n", phase, duration)
	}

	outputView, err := g.View("output")
	if err != nil {
		return err
	}
	outputView.Clear()

	fmt.Fprintf(outputView, "\033[33mTotal Results Count: %d\033[0m\n", totalResultsCount)

	for i, result := range results {
		if i >= c.maxResults {
			break
		}

		highlightedHeader := fmt.Sprintf("\033[32mDoc ID: %s | Unique Matches: %d | Total Matches: %d\033[0m\n",
			result.ID, result.UniqueMatches, result.TotalMatches)
		fmt.Fprintf(outputView, "%s\n", highlightedHeader)

		highlightQueryInResult(&result.Document, searchQuery)
		fmt.Fprintf(outputView, "%s\n%s\n\n", result.Document.URL, result.Document.Abstract)
	}

	_, _ = g.SetCurrentView("input")
	return nil
}

func highlightQueryInResult(document *models.Document, query string) {
	words := strings.Fields(query)
	for _, word := range words {
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(word) + `\b`)
		document.Abstract = re.ReplaceAllString(document.Abstract, "\033[31m$0\033[0m")
	}
}

func (c *CUI) performSearch(query string, ctx context.Context) ([]models.ResultData, map[string]string, int, error) {
	searchResult, err := c.ftsSerivce.SearchDocuments(ctx, query, c.maxResults)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to search documents: %v", err)
	}

	for i, result := range searchResult.ResultData {
		doc, err := c.storage.GetDocument(ctx, result.ID)
		if err != nil {
			c.log.Error("Failed to get document from storage:", "error", sl.Err(err))
			continue
		}
		searchResult.ResultData[i].Document = *doc
	}

	if err != nil {
		return nil, nil, 0, err
	}
	return searchResult.ResultData, searchResult.Timings, searchResult.TotalResultsCount, nil
}

func quit(g *gocui.Gui, v *gocui.View) error {
	return gocui.ErrQuit
}
