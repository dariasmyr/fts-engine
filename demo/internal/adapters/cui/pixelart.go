package cui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var turtlePalette = map[rune]lipgloss.Color{
	'o': lipgloss.Color("#244A2E"),
	's': lipgloss.Color("#3C7A45"),
	'S': lipgloss.Color("#6BB45C"),
	'H': lipgloss.Color("#9AD47E"),
	'u': lipgloss.Color("#2B5331"),
	'g': lipgloss.Color("#8FC457"),
	'G': lipgloss.Color("#B6DC7E"),
	'e': lipgloss.Color("#1C2A1B"),
	'm': lipgloss.Color("#244A2E"),
	'b': lipgloss.Color("#E0A45E"),
	'B': lipgloss.Color("#C07C3A"),
	'z': lipgloss.Color("#8FB9C7"),
	'*': lipgloss.Color("#F2C14E"),
}

func renderSprite(rows []string, pal map[rune]lipgloss.Color) string {
	width := 0
	for _, r := range rows {
		if len(r) > width {
			width = len(r)
		}
	}

	var b strings.Builder
	for y := 0; y < len(rows); y += 2 {
		top := rows[y]
		bot := ""
		if y+1 < len(rows) {
			bot = rows[y+1]
		}
		for x := 0; x < width; x++ {
			tc, tok := pal[pixelAt(top, x)]
			bc, bok := pal[pixelAt(bot, x)]
			switch {
			case tok && bok:
				b.WriteString(lipgloss.NewStyle().Foreground(tc).Background(bc).Render("▀"))
			case tok:
				b.WriteString(lipgloss.NewStyle().Foreground(tc).Render("▀"))
			case bok:
				b.WriteString(lipgloss.NewStyle().Foreground(bc).Render("▄"))
			default:
				b.WriteString(" ")
			}
		}
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func pixelAt(row string, x int) rune {
	if x < 0 || x >= len(row) {
		return ' '
	}
	return rune(row[x])
}

var turtleIdle = []string{
	"            oooooooo            ",
	"         oooSSSSSSSSooo         ",
	"       ooSSSHHHHHHHHSSSoo       ",
	"      oSSSHSSSooooSSSHSSSo      ",
	"     oSSSHSSoSSSSSSoSSHSSSo  ggg    ",
	"    oSSHSSSoSSSSSSSSoSSSHSSo gGGGg  ",
	"    oSSHSSoSSSSSSSSSSoSSHSSogGGGGGg ",
	"   oSSHSSSoSSSSSSSSSSoSSSHSSoGGeGGGg",
	"   oSSSSSSoSSSSSSSSSSoSSSSSSoGGGGGGg",
	"   oSSSSSSSoSSSSSSSSoSSSSSSSoGGGGGGg",
	"   ouSSSSSSSooooooooSSSSSSSuoGGGGGGg",
	"   oouuSSSSSSSSSSSSSSSSSSuuoo gGGGg ",
	"  obBuuuuuuuuuuuuuuuuuuuuBboo  ggg  ",
	"  oBBBBBBBBBBBBBBBBBBBBBBBBo        ",
	"    ggg    ggg    ggg    ggg        ",
	"    ggg    ggg    ggg    ggg        ",
	"    GGG    GGG    GGG    GGG        ",
}

func turtleFor(mood string) []string {
	rows := make([]string, len(turtleIdle))
	copy(rows, turtleIdle)

	switch mood {
	case "sleep":
		rows[7] = strings.Replace(rows[7], "GGeGG", "GmmmG", 1)
		rows = append([]string{
			"                                 z   ",
			"                               z     ",
			"                                 z   ",
		}, rows...)
	case "error":
		rows[9] = strings.Replace(rows[9], "GGGGGGg", "GGmmGGg", 1)
		rows = append([]string{
			"                              *      ",
			"                              *      ",
		}, rows...)
	case "search":
		rows = append([]string{
			"                                 *   ",
			"                               *   * ",
		}, rows...)
	}
	return rows
}

func mascot(mood string) string {
	return renderSprite(turtleFor(mood), turtlePalette)
}
