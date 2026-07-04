package cui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

const (
	minWidth        = 56
	minHeight       = 14
	minSidebarW     = 30
	preferredSide   = 38
	helpBoxW        = 42
	dualHelpLayoutW = 118
	tripleLayoutW   = 154
)

func (m *model) layout() {
	availableWidth := max(m.width-4, 32)

	limitWidth := 12
	searchWidth := max(availableWidth-limitWidth-3, 16)
	m.searchInput.Width = max(searchWidth-4, 12)
	m.limitInput.Width = 6

	resultWidth := availableWidth
	sidebarWidth := m.sidebarWidth()
	helpWidth := m.helpPanelWidth()
	switch {
	case m.width >= tripleLayoutW:
		resultWidth = max(availableWidth-sidebarWidth-helpWidth-2, 28)
	case m.width >= dualHelpLayoutW:
		resultWidth = max(availableWidth-helpWidth-1, 28)
	}
	m.resultsView.Width = max(resultWidth-4, 20)

	resultHeight := max(m.height-11, 5)
	switch {
	case m.width >= tripleLayoutW:
		resultHeight = max(m.height-11, 5)
	case m.width >= dualHelpLayoutW:
		resultHeight = max(m.height-23, 5)
	default:
		resultHeight = max(m.height-33, 4)
	}
	m.resultsView.Height = resultHeight
}

func (m model) View() string {
	if !m.ready {
		return "\n  initializing interface..."
	}
	if m.width < minWidth || m.height < minHeight {
		return appStyle.Render(m.renderTinyView())
	}

	searchPanel := panelStyle(m.focus == focusSearch).
		Width(m.searchInput.Width + 4).
		Render(panelTitle(m.mode.inputTitle(), m.focus == focusSearch) + "\n" + m.searchInput.View())

	limitPanel := panelStyle(m.focus == focusLimit).
		Width(12).
		Render(panelTitle("Max Results", m.focus == focusLimit) + "\n" + m.limitInput.View())

	top := m.renderTop(searchPanel, limitPanel)
	body := m.renderBody()
	footer := m.renderStatusBar()

	return appStyle.Render(lipgloss.JoinVertical(lipgloss.Left, top, "", body, "", footer))
}

func (m model) renderTop(searchPanel, limitPanel string) string {
	return lipgloss.JoinHorizontal(lipgloss.Top, searchPanel, " ", limitPanel)
}

func (m model) renderBody() string {
	helpPanel := m.renderHelpPanel()
	if m.width >= tripleLayoutW {
		return lipgloss.JoinHorizontal(lipgloss.Top, m.renderSidebar(), " ", m.renderResultsPanel(), " ", helpPanel)
	}
	if m.width >= dualHelpLayoutW {
		main := lipgloss.JoinHorizontal(lipgloss.Top, m.renderResultsPanel(), " ", helpPanel)
		return lipgloss.JoinVertical(lipgloss.Left, main, "", m.renderSidebar())
	}
	return lipgloss.JoinVertical(lipgloss.Left, m.renderResultsPanel(), "", m.renderSidebar(), "", helpPanel)
}

func (m model) renderSidebar() string {
	width := m.sidebarWidth()
	body := lipgloss.JoinVertical(
		lipgloss.Left,
		m.sectionSystem(width),
		divider(width),
		m.sectionStats(width),
		divider(width),
		m.sectionDiagnostics(width),
		divider(width),
		m.sectionShortcuts(),
	)
	return panelStyle(false).Width(width).Render(body)
}

func (m model) renderHelpPanel() string {
	width := m.helpPanelWidth()
	body := lipgloss.JoinVertical(
		lipgloss.Left,
		m.renderHelpKeys(width-4),
		"",
		divider(width),
		m.renderModeOverview(width-4),
		"",
		divider(width),
		m.renderSyntaxHelp(width-4),
	)
	return panelStyle(false).Width(width).Render(panelTitle("Help", false) + "\n" + body)
}

func (m model) renderHelpKeys(width int) string {
	rows := []string{
		sectionTitle.Render("Shortcuts"),
		kv("enter", "search"),
		kv("tab", "switch focus"),
		kv("ctrl+t", "toggle plain/syntax"),
		kv("ctrl+c", "quit"),
	}
	if m.focus == focusResults {
		rows = append(rows, kv("up/down", "scroll results"), kv("j/k", "scroll results"))
	} else {
		rows = append(rows, kv("up/down", "use in results focus"), kv("j/k", "use in results focus"))
	}
	return strings.Join(rows, "\n")
}

func (m model) renderModeOverview(width int) string {
	rows := []string{sectionTitle.Render("Modes")}
	if m.mode == searchModeSyntax {
		rows = append(rows,
			kv("active", "syntax"),
			mutedStyle.Width(width).Render("Syntax mode parses operators, fields, phrases, prefix queries, and grouped boolean expressions."),
		)
	} else {
		rows = append(rows,
			kv("active", "plain"),
			mutedStyle.Width(width).Render("Plain mode treats the input as regular text and does not parse query operators. Good for quick bag-of-words search."),
		)
	}
	rows = append(rows, mutedStyle.Width(width).Render("Press Ctrl+T to switch modes at any time."))
	return strings.Join(rows, "\n")
}

func (m model) renderSyntaxHelp(width int) string {
	if width < 20 {
		return ""
	}

	if m.mode != searchModeSyntax {
		return strings.Join([]string{
			sectionTitle.Render("Plain Mode Tips"),
			kv("example", "french hotel"),
			kv("example", "canal barge biography"),
			mutedStyle.Width(width).Render("Use plain mode when you want the engine to interpret the full input as text, not as syntax."),
		}, "\n")
	}

	rows := []string{
		sectionTitle.Render("Syntax Mode"),
		mutedStyle.Width(width).Render("Query-string syntax examples:"),
		kv("term", "hotel"),
		kv("phrase", "\"hotel barge\""),
		kv("must/not", "+hotel -market"),
		kv("field", "title:hotel"),
		kv("prefix", "bar*"),
		kv("group", "+(title:hotel title:french) -market"),
		mutedStyle.Width(width).Render("Use syntax mode when you need explicit operators or field-aware queries."),
	}
	return strings.Join(rows, "\n")
}

func (m model) renderResultsPanel() string {
	title := panelTitle("Results", m.focus == focusResults)
	if m.lastQuery != "" {
		title += mutedStyle.Render("  \"" + truncateSingleLine(m.lastQuery, 24) + "\"")
	}

	content := m.resultsView.View()
	if m.lastError != nil {
		content = m.centeredCard("error", errorStyle.Render("search failed: "+m.lastError.Error()))
	} else if m.searching {
		content = m.centeredCard("search", accentStyle.Render(m.spinner.View()+" searching the corpus"))
	} else if m.lastQuery == "" {
		if m.resultsView.Width >= 52 && m.resultsView.Height >= 14 {
			content = splash(m.resultsView.Width, "idle")
		} else {
			content = m.centeredCard("idle", emptyStyle.Render("type a query and press enter"))
		}
	} else if len(m.lastResults) == 0 {
		content = m.centeredCard("sleep", emptyStyle.Render("nothing matched \""+truncateSingleLine(m.lastQuery, 28)+"\""))
	} else if content == "" {
		content = mutedStyle.Render("Run a search to see matching documents.")
	}

	scroll := scrollStyle.Render(fmt.Sprintf("%3.0f%%", m.resultsView.ScrollPercent()*100))
	headerPad := max(m.resultsView.Width-lipgloss.Width(title)-lipgloss.Width(scroll), 1)
	header := title + strings.Repeat(" ", headerPad) + scroll
	return panelStyle(m.focus == focusResults).
		Width(m.resultsView.Width + 4).
		Height(m.resultsView.Height + 2).
		Render(header + "\n" + content)
}

func (m model) renderResults() string {
	if m.lastError != nil || m.searching || m.lastQuery == "" || len(m.lastResults) == 0 {
		return ""
	}

	var builder strings.Builder
	builder.WriteString(resultHeaderStyle.Render(fmt.Sprintf("Total Results Count: %d", m.totalResults)))
	builder.WriteString("\n\n")

	for i, result := range m.lastResults {
		if i >= m.maxResults {
			break
		}

		header := lipgloss.JoinHorizontal(
			lipgloss.Center,
			accentBar.Render("▌ "),
			docIDStyle.Render("doc "+result.ID),
			"  ",
			badgeUnique.Render(fmt.Sprintf("◆ %d unique", result.UniqueMatches)),
			" ",
			badgeTotal.Render(fmt.Sprintf("Σ %d total", result.TotalMatches)),
		)
		builder.WriteString(header)
		builder.WriteString("\n")
		if result.Document.URL != "" {
			builder.WriteString(accentBar.Render("  "))
			builder.WriteString(urlStyle.Render(truncateSingleLine(result.Document.URL, max(m.resultsView.Width-2, 24))))
			builder.WriteString("\n")
		}

		abstract := strings.TrimSpace(result.Document.Abstract)
		if abstract == "" {
			abstract = emptyStyle.Render("(no abstract)")
		} else {
			abstract = m.highlightQuery(m.lastQuery, abstract)
			abstract = abstractStyle.Width(max(m.resultsView.Width-2, 20)).Render(abstract)
		}
		builder.WriteString(lipgloss.NewStyle().PaddingLeft(2).Render(abstract))
		builder.WriteString("\n\n")

		if i < len(m.lastResults)-1 && i < m.maxResults-1 {
			builder.WriteString(scrollStyle.Render(strings.Repeat("┄", max(m.resultsView.Width-2, 1))))
			builder.WriteString("\n")
		}
	}

	return builder.String()
}

func (m model) renderStatusBar() string {
	state := statusVal.Render("ready")
	dot := statusDot.Render("●")
	switch {
	case m.searching:
		state = statusVal.Render("searching")
		dot = statusKey.Render("◐")
	case m.lastError != nil:
		state = errorStyle.Render("error")
		dot = errorStyle.Render("●")
	}

	parts := []string{dot + " " + state}
	if m.lastQuery != "" && m.lastError == nil {
		parts = append(parts,
			statusKey.Render("hits ")+statusVal.Render(fmt.Sprintf("%d", m.totalResults)),
			statusKey.Render("mode ")+statusVal.Render(string(m.mode)),
			statusKey.Render("wall ")+statusVal.Render(formatDuration(m.lastElapsed)),
		)
	} else {
		parts = append(parts, statusKey.Render("mode ")+statusVal.Render(string(m.mode)))
	}
	parts = append(parts, statusKey.Render("focus ")+statusVal.Render(m.focusLabel()))
	parts = append(parts, statusKey.Render("quit ")+statusVal.Render("Ctrl+C"))
	return strings.Join(parts, "  "+statusSep.Render("│")+"  ")
}

func (m model) renderTinyView() string {
	lines := []string{
		errorStyle.Render(fmt.Sprintf("terminal too small for full UI (%dx%d minimum)", minWidth, minHeight)),
		"",
		sectionTitle.Render("Current State"),
		kv("mode", string(m.mode)),
		kv("status", m.statusMessage),
		kv("query", truncateSingleLine(m.searchInput.Value(), max(m.width-10, 12))),
		kv("quit", "Ctrl+C"),
		"",
		mutedStyle.Render("Resize the terminal to use the full Bubble Tea interface."),
	}
	return strings.Join(lines, "\n")
}

func (m model) centeredCard(mood, msg string) string {
	body := msg
	if m.resultsView.Width >= 38 && m.resultsView.Height >= 10 {
		body = lipgloss.JoinVertical(lipgloss.Center, mascot(mood), "", msg)
	}
	return lipgloss.Place(m.resultsView.Width, m.resultsView.Height, lipgloss.Center, lipgloss.Center, body)
}

func (m model) sidebarWidth() int {
	availableWidth := max(m.width-4, 32)
	if m.width >= tripleLayoutW {
		return min(preferredSide, max(availableWidth/3, minSidebarW))
	}
	return max(availableWidth-2, 26)
}

func (m model) sectionSystem(width int) string {
	return strings.Join([]string{
		sectionTitle.Render("⚙ system"),
		kvVal("version", statStyle.Render(orDash(m.info.Version))),
		kv("engine", orDash(m.info.Engine)),
		kv("index", orDash(m.info.Index)),
		kv("filter", orDash(m.info.Filter)),
		kvVal("docs", statStyle.Render(fmtInt(m.docCount))),
		kvVal("limit", statStyle.Render(fmt.Sprintf("%d", m.maxResults))),
		kv("uptime", fmtDuration(m.now.Sub(m.startTime))),
	}, "\n")
}

func (m model) sectionStats(width int) string {
	rows := []string{sectionTitle.Render("✸ search")}
	if m.lastQuery == "" {
		rows = append(rows, mutedStyle.Render("no search yet"))
		return strings.Join(rows, "\n")
	}

	rows = append(rows,
		kv("query", truncateSingleLine(m.lastQuery, max(width-10, 18))),
		kvVal("hits", statStyle.Render(fmt.Sprintf("%d", m.totalResults))),
		kvVal("shown", statStyle.Render(fmt.Sprintf("%d/%d", min(len(m.lastResults), m.maxResults), m.totalResults))),
		kvVal("wall", timingValStyle.Render(formatDuration(m.lastElapsed))),
		kv("mode", string(m.mode)),
	)
	if m.searching {
		rows = append(rows, kvVal("state", timingValStyle.Render(m.spinner.View()+" searching")))
	}
	return strings.Join(rows, "\n")
}

func (m model) sectionDiagnostics(width int) string {
	rows := []string{sectionTitle.Render("◎ diagnostics")}
	if m.diagnostics == nil {
		rows = append(rows, mutedStyle.Render("run a search to populate diagnostics"))
		return strings.Join(rows, "\n")
	}

	rows = append(rows,
		kv("query_type", orDash(m.diagnostics.LogicalQueryType)),
		kv("strategy", orDash(m.diagnostics.ExecutionStrategy)),
	)
	if m.diagnostics.StrategySkipReason != "" {
		rows = append(rows, kv("skip_reason", orDash(m.diagnostics.StrategySkipReason)))
	}

	timingKeys := make([]string, 0, len(m.diagnostics.Timings))
	for key := range m.diagnostics.Timings {
		timingKeys = append(timingKeys, key)
	}
	sort.Strings(timingKeys)
	for _, key := range timingKeys {
		rows = append(rows, kvVal(key, timingValStyle.Render(m.diagnostics.Timings[key])))
	}

	rows = append(rows,
		kvVal("tokens", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.ProcessedTokens))),
		kvVal("fields", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.FieldsVisited))),
		kvVal("keys", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.GeneratedKeys))),
		kvVal("searches", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.IndexSearches))),
		kvVal("checks", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.FilterChecks))),
		kvVal("rejects", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.FilterRejects))),
		kvVal("postings", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.PostingEntriesRead))),
		kvVal("candidates", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.CandidateDocs))),
		kvVal("matched", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.MatchedDocs))),
		kvVal("returned", statStyle.Render(fmt.Sprintf("%d", m.diagnostics.ReturnedDocs))),
	)
	return strings.Join(rows, "\n")
}

func (m model) sectionShortcuts() string {
	return strings.Join([]string{
		sectionTitle.Render("⌨ shortcuts"),
		kv("tab", "focus"),
		kv("enter", "search"),
		kv("ctrl+t", "toggle mode"),
		kv("↑/↓", "scroll"),
		kv("ctrl+c", "quit"),
	}, "\n")
}

func (m model) helpPanelWidth() int {
	if m.width >= dualHelpLayoutW {
		return helpBoxW
	}
	return max(m.width-4, 36)
}

func kv(key, value string) string {
	return sidebarKey.Render(key+": ") + sidebarVal.Render(value)
}

func kvVal(key, value string) string {
	return sidebarKey.Render(key+": ") + value
}

func divider(width int) string {
	return dividerDot.Render(strings.Repeat("┄", max(width-4, 8)))
}

func fmtDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return d.Round(100 * time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%dus", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Round(time.Millisecond).String()
}

func fmtInt(n int) string {
	return fmt.Sprintf("%d", n)
}

func orDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func truncateSingleLine(value string, width int) string {
	if width <= 3 {
		return value
	}
	value = strings.ReplaceAll(value, "\n", " ")
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	return string(runes[:width-3]) + "..."
}
