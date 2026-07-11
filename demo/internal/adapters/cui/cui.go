package cui

import (
	"context"
	"fmt"
	"log/slog"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
)

type SearchEngine interface {
	Fields() []string
	HighlightPlainText(query string, text string) string
	HighlightQueryString(query string, text string) string
	SearchPlainText(
		ctx context.Context,
		query string,
		maxResults int,
	) (*models.SearchResult, error)
	SearchQueryString(
		ctx context.Context,
		query string,
		maxResults int,
	) (*models.SearchResult, error)
}

type Info struct {
	Engine  string
	Index   string
	Filter  string
	Version string
}

type searchMode string

const (
	searchModePlain  searchMode = "plain"
	searchModeSyntax searchMode = "syntax"
)

func (m searchMode) toggle() searchMode {
	if m == searchModeSyntax {
		return searchModePlain
	}
	return searchModeSyntax
}

func (m searchMode) inputTitle() string {
	return fmt.Sprintf("Search [%s | Ctrl+T toggle]", m)
}

type CUI struct {
	ctx        context.Context
	ftsService SearchEngine
	documents  map[string]models.Document
	log        *slog.Logger
	maxResults int
	mode       searchMode
	info       Info

	program *tea.Program
}

func New(ctx context.Context, log *slog.Logger, ftsService SearchEngine, documents map[string]models.Document, maxResults int, info Info) *CUI {
	return &CUI{
		ctx:        ctx,
		ftsService: ftsService,
		documents:  documents,
		log:        log,
		maxResults: maxResults,
		mode:       searchModeSyntax,
		info:       info,
	}
}

func (c *CUI) Close() {
	if c.program != nil {
		c.program.Quit()
	}
}

func (c *CUI) Start() error {
	m := newModel(c.ctx, c.ftsService, c.documents, c.maxResults, c.mode, c.info)
	c.program = tea.NewProgram(m, tea.WithAltScreen())
	_, err := c.program.Run()
	return err
}
