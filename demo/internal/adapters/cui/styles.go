package cui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
	colorful "github.com/lucasb-eyer/go-colorful"
)

var (
	colMint   = lipgloss.Color("#6FCF8E")
	colSage   = lipgloss.Color("#5B9279")
	colFern   = lipgloss.Color("#52B788")
	colHoney  = lipgloss.Color("#E0A45E")
	colCoral  = lipgloss.Color("#D1654E")
	colBorder = lipgloss.Color("#2E4A36")
	colMuted  = lipgloss.Color("#7E9079")
	colText   = lipgloss.Color("#DCE6D5")
	colInk    = lipgloss.Color("#0E1A13")
)

const (
	gradFrom = "#B6DC7E"
	gradTo   = "#2E8B6F"
)

var (
	appStyle = lipgloss.NewStyle().Padding(0, 1)

	subtitleStyle = lipgloss.NewStyle().
		Foreground(colMuted).
		Italic(true)

	mutedStyle = lipgloss.NewStyle().Foreground(colMuted)
	errorStyle = lipgloss.NewStyle().Foreground(colCoral).Bold(true)
	accentStyle = lipgloss.NewStyle().Foreground(colMint).Bold(true)
	valueStyle = lipgloss.NewStyle().Foreground(colText)
	promptStyle = lipgloss.NewStyle().Foreground(colFern).Bold(true)
	inputTextStyle = lipgloss.NewStyle().Foreground(colText)
	cursorStyle = lipgloss.NewStyle().Foreground(colMint)
	resultHeaderStyle = lipgloss.NewStyle().Foreground(colMint).Bold(true)
	urlStyle = lipgloss.NewStyle().Foreground(colSage).Underline(true)
	sectionTitle = lipgloss.NewStyle().Foreground(colSage).Bold(true)
	sidebarKey = lipgloss.NewStyle().Foreground(colMuted)
	sidebarVal = lipgloss.NewStyle().Foreground(colText)
	statusKey = lipgloss.NewStyle().Foreground(colMuted)
	statusVal = lipgloss.NewStyle().Foreground(colText)
	statusDot = lipgloss.NewStyle().Foreground(colFern)
	statusSep = lipgloss.NewStyle().Foreground(colBorder)
	dividerDot = lipgloss.NewStyle().Foreground(colBorder)
	badgeUnique = lipgloss.NewStyle().Foreground(colInk).Background(colFern).Bold(true).Padding(0, 1)
	badgeTotal = lipgloss.NewStyle().Foreground(colInk).Background(colHoney).Bold(true).Padding(0, 1)
	docIDStyle = lipgloss.NewStyle().Foreground(colMint).Bold(true)
	highlightStyle = lipgloss.NewStyle().Foreground(colInk).Background(colMint).Bold(true)
	abstractStyle = lipgloss.NewStyle().Foreground(colText)
	accentBar = lipgloss.NewStyle().Foreground(colMint)
	timingValStyle = lipgloss.NewStyle().Foreground(colFern).Bold(true)
	statStyle = lipgloss.NewStyle().Foreground(colHoney).Bold(true)
	emptyStyle = lipgloss.NewStyle().Foreground(colMuted).Italic(true).Align(lipgloss.Center)
	scrollStyle = lipgloss.NewStyle().Foreground(colBorder)
)

func panelStyle(focused bool) lipgloss.Style {
	border := colBorder
	if focused {
		border = colMint
	}
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(border).
		Padding(0, 1)
}

func panelTitle(title string, focused bool) string {
	style := lipgloss.NewStyle().Foreground(colSage).Bold(true)
	if focused {
		style = style.Foreground(colMint)
	}
	return style.Render(title)
}

func gradientText(s, from, to string) string {
	runes := []rune(s)
	if len(runes) == 0 {
		return s
	}
	a, err1 := colorful.Hex(from)
	b, err2 := colorful.Hex(to)
	if err1 != nil || err2 != nil {
		return s
	}

	var sb strings.Builder
	n := len(runes)
	for i, r := range runes {
		t := 0.0
		if n > 1 {
			t = float64(i) / float64(n-1)
		}
		c := a.BlendLuv(b, t).Hex()
		sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color(c)).Bold(true).Render(string(r)))
	}
	return sb.String()
}
