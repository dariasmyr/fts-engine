package cui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var ftsBanner = []string{
	`███████╗████████╗███████╗`,
	`██╔════╝╚══██╔══╝██╔════╝`,
	`█████╗     ██║   ███████╗`,
	`██╔══╝     ██║   ╚════██║`,
	`██║        ██║   ███████║`,
	`╚═╝        ╚═╝   ╚══════╝`,
}

func gradientLines(lines []string, from, to string) string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = gradientText(l, from, to)
	}
	return strings.Join(out, "\n")
}

func splash(width int, mood string) string {
	banner := gradientLines(ftsBanner, gradFrom, gradTo)
	face := mascot(mood)

	tagline := lipgloss.NewStyle().
		Foreground(colMint).
		Bold(true).
		Render("Fast · Turtle · Search")

	subtitle := subtitleStyle.Render("a tiny full-text engine with a big heart")

	hints := lipgloss.NewStyle().Foreground(colMuted).Render(
		"type a query above  ·  press " +
			lipgloss.NewStyle().Foreground(colFern).Render("enter") +
			"  ·  " +
			lipgloss.NewStyle().Foreground(colSage).Render("tab") + " to move around  ·  " +
			lipgloss.NewStyle().Foreground(colFern).Render("Ctrl+C") + " to quit",
	)

	card := lipgloss.JoinVertical(
		lipgloss.Center,
		banner,
		"",
		face,
		"",
		tagline,
		subtitle,
		"",
		hints,
	)

	return lipgloss.Place(width, lipgloss.Height(card), lipgloss.Center, lipgloss.Center, card)
}
