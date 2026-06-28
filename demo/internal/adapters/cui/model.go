package cui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
)

type focusArea int

const (
	focusSearch focusArea = iota
	focusLimit
	focusResults
)

type searchDoneMsg struct {
	query       string
	mode        searchMode
	results     []models.ResultData
	diagnostics *models.SearchDiagnostics
	total       int
	elapsed     time.Duration
	err         error
}

type keyMap struct {
	Tab         key.Binding
	Search      key.Binding
	ToggleMode  key.Binding
	ScrollUp    key.Binding
	ScrollDown  key.Binding
	Quit        key.Binding
}

var keys = keyMap{
	Tab:        key.NewBinding(key.WithKeys("tab"), key.WithHelp("tab", "switch focus")),
	Search:     key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "search")),
	ToggleMode: key.NewBinding(key.WithKeys("ctrl+t"), key.WithHelp("ctrl+t", "toggle mode")),
	ScrollUp:   key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("up/k", "scroll up")),
	ScrollDown: key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("down/j", "scroll down")),
	Quit:       key.NewBinding(key.WithKeys("ctrl+c"), key.WithHelp("ctrl+c", "quit")),
}

type model struct {
	ctx       context.Context
	engine    SearchEngine
	documents map[string]models.Document
	info      Info

	searchInput textinput.Model
	limitInput  textinput.Model
	resultsView viewport.Model
	spinner     spinner.Model
	keys        keyMap

	focus      focusArea
	width      int
	height     int
	ready      bool
	searching  bool
	maxResults int
	mode       searchMode
	startTime  time.Time
	now        time.Time
	docCount   int

	lastQuery       string
	lastResults     []models.ResultData
	diagnostics     *models.SearchDiagnostics
	totalResults    int
	lastElapsed     time.Duration
	lastError       error
	resultContent   string
	statusMessage   string
	searchInputHint string
}

func newModel(ctx context.Context, engine SearchEngine, documents map[string]models.Document, maxResults int, mode searchMode, info Info) model {
	searchInput := textinput.New()
	searchInput.Placeholder = "Type a query and press Enter"
	searchInput.Prompt = "fts> "
	searchInput.CharLimit = 256
	searchInput.PromptStyle = promptStyle
	searchInput.TextStyle = inputTextStyle
	searchInput.PlaceholderStyle = mutedStyle
	searchInput.Cursor.Style = cursorStyle
	searchInput.Focus()

	limitInput := textinput.New()
	limitInput.SetValue(strconv.Itoa(maxResults))
	limitInput.CharLimit = 6
	limitInput.Prompt = ""
	limitInput.TextStyle = inputTextStyle
	limitInput.Cursor.Style = cursorStyle

	resultsView := viewport.New(0, 0)

	spin := spinner.New()
	spin.Spinner = spinner.Dot
	spin.Style = accentStyle

	now := time.Now()

	m := model{
		ctx:       ctx,
		engine:    engine,
		documents: documents,
		info:      info,

		searchInput: searchInput,
		limitInput:  limitInput,
		resultsView: resultsView,
		spinner:     spin,
		keys:        keys,

		focus:      focusSearch,
		maxResults: maxResults,
		mode:       mode,
		startTime:  now,
		now:        now,
		docCount:   len(documents),
	}
	m.updateStatus("Ready")
	return m
}

func (m model) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, tickCmd())
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.layout()
		m.ready = true
		m.refreshResultsView()
		return m, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keys.Quit):
			return m, tea.Quit
		case key.Matches(msg, m.keys.ToggleMode):
			m.mode = m.mode.toggle()
			m.updateStatus(fmt.Sprintf("Search mode: %s", m.mode))
			m.refreshResultsView()
			return m, nil
		case key.Matches(msg, m.keys.Tab):
			m.cycleFocus()
			return m, textinput.Blink
		case key.Matches(msg, m.keys.Search):
			if m.focus == focusSearch || m.focus == focusLimit {
				m.applyLimit()
				query := strings.TrimSpace(m.searchInput.Value())
				if query == "" {
					m.lastError = nil
					m.updateStatus("Enter a query to search")
					m.refreshResultsView()
					return m, nil
				}
				if m.searching {
					return m, nil
				}
				m.searching = true
				m.lastError = nil
				m.updateStatus(fmt.Sprintf("Searching in %s mode", m.mode))
				return m, tea.Batch(m.spinner.Tick, m.runSearch(query))
			}
		case key.Matches(msg, m.keys.ScrollUp):
			if m.focus == focusResults {
				m.resultsView.ScrollUp(1)
				return m, nil
			}
		case key.Matches(msg, m.keys.ScrollDown):
			if m.focus == focusResults {
				m.resultsView.ScrollDown(1)
				return m, nil
			}
		}

	case searchDoneMsg:
		m.searching = false
		m.lastQuery = msg.query
		m.mode = msg.mode
		m.lastElapsed = msg.elapsed
		m.lastError = msg.err
		m.diagnostics = msg.diagnostics
		if msg.err != nil {
			m.lastResults = nil
			m.totalResults = 0
			m.updateStatus("Search failed")
			m.refreshResultsView()
			return m, nil
		}

		m.lastResults = msg.results
		m.totalResults = msg.total
		m.updateStatus(fmt.Sprintf("Found %d result(s)", msg.total))
		m.refreshResultsView()
		m.resultsView.GotoTop()
		return m, nil

	case spinner.TickMsg:
		if m.searching {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil

	case tickMsg:
		m.now = time.Time(msg)
		return m, tickCmd()
	}

	var cmd tea.Cmd
	switch m.focus {
	case focusSearch:
		m.searchInput, cmd = m.searchInput.Update(msg)
	case focusLimit:
		m.limitInput, cmd = m.limitInput.Update(msg)
	case focusResults:
		m.resultsView, cmd = m.resultsView.Update(msg)
	}

	return m, cmd
}

func (m *model) cycleFocus() {
	m.searchInput.Blur()
	m.limitInput.Blur()

	switch m.focus {
	case focusSearch:
		m.focus = focusLimit
		m.limitInput.Focus()
	case focusLimit:
		m.focus = focusResults
	default:
		m.focus = focusSearch
		m.searchInput.Focus()
	}
	m.updateStatus(fmt.Sprintf("Focus: %s", m.focusLabel()))
}

func (m *model) focusLabel() string {
	switch m.focus {
	case focusSearch:
		return "search"
	case focusLimit:
		return "limit"
	default:
		return "results"
	}
}

func (m *model) applyLimit() {
	limit := strings.TrimSpace(m.limitInput.Value())
	n, err := strconv.Atoi(limit)
	if err != nil || n <= 0 {
		m.limitInput.SetValue(strconv.Itoa(m.maxResults))
		m.updateStatus("Max results must be a positive integer")
		return
	}
	if n != m.maxResults {
		m.maxResults = n
		m.updateStatus(fmt.Sprintf("Max results: %d", m.maxResults))
		m.refreshResultsView()
	}
}

func (m model) runSearch(query string) tea.Cmd {
	ctx := m.ctx
	engine := m.engine
	documents := m.documents
	mode := m.mode
	maxResults := m.maxResults

	return func() tea.Msg {
		startedAt := time.Now()

		var (
			searchResult *models.SearchResult
			err          error
		)
		if mode == searchModeSyntax {
			searchResult, err = engine.SearchQueryString(ctx, query, maxResults)
		} else {
			searchResult, err = engine.SearchPlainText(ctx, query, maxResults)
		}

		if err != nil {
			return searchDoneMsg{
				query:   query,
				mode:    mode,
				elapsed: time.Since(startedAt),
				err:     err,
			}
		}

		results := make([]models.ResultData, len(searchResult.ResultData))
		copy(results, searchResult.ResultData)
		for i := range results {
			if doc, ok := documents[results[i].ID]; ok {
				results[i].Document = doc
			}
		}

		return searchDoneMsg{
			query:       query,
			mode:        mode,
			results:     results,
			diagnostics: searchResult.Diagnostics,
			total:       searchResult.TotalResultsCount,
			elapsed:     time.Since(startedAt),
		}
	}
}

func (m *model) updateStatus(status string) {
	m.statusMessage = status
}

func (m *model) refreshResultsView() {
	m.resultContent = m.renderResults()
	m.resultsView.SetContent(m.resultContent)
}

func (m model) highlightQuery(query string, text string) string {
	if m.mode == searchModeSyntax {
		return m.engine.HighlightQueryString(query, text)
	}
	return m.engine.HighlightPlainText(query, text)
}
