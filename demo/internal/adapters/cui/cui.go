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
	SemanticEnabled() bool
	HighlightPlainText(query string, text string) string
	HighlightQueryString(query string, text string) string
	SearchTerm(
		ctx context.Context,
		query string,
		maxResults int,
	) (*models.SearchResult, error)
	SearchText(
		ctx context.Context,
		query string,
		maxResults int,
	) (*models.SearchResult, error)
	SearchSemantic(
		ctx context.Context,
		query string,
		maxResults int,
	) (*models.SearchResult, error)
}

type Info struct {
	Engine  string
	Index   string
	Scorer  string
	Profile string
	Filter  string
	Version string
}

type searchMode string

const (
	searchModePlain    searchMode = "plain"
	searchModeSyntax   searchMode = "syntax"
	searchModeSemantic searchMode = "semantic"
)

func (m searchMode) toggle(semanticEnabled bool) searchMode {
	switch m {
	case searchModeSyntax:
		return searchModePlain
	case searchModePlain:
		if semanticEnabled {
			return searchModeSemantic
		}
		return searchModeSyntax
	default:
		return searchModeSyntax
	}
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

func (c *CUI) Start() error {
	m := newModel(c.ctx, c.ftsService, c.documents, c.maxResults, c.mode, c.info)
	c.program = tea.NewProgram(m, tea.WithAltScreen())
	_, err := c.program.Run()
	return err
}
