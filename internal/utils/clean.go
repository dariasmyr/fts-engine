package utils

import (
	"regexp"
	"strings"
)

func Clean(text string) string {
	text = regexp.MustCompile(`\n+`).ReplaceAllString(text, " ")

	text = regexp.MustCompile(`[^\p{L}\p{N}\p{P}\p{Z}]`).ReplaceAllString(text, "")

	text = strings.TrimSpace(text)

	return text
}
