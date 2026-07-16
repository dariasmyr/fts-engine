package textproc

import (
	"strings"
	"unicode"
)

type Tokenizer interface {
	Tokenize(text string) []string
}

type AlnumTokenizer struct{}

func (AlnumTokenizer) Tokenize(text string) []string {
	if text == "" {
		return nil
	}

	tokens := make([]string, 0, 16)
	var b strings.Builder

	flush := func() {
		if b.Len() == 0 {
			return
		}
		tokens = append(tokens, b.String())
		b.Reset()
	}

	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			continue
		}
		flush()
	}
	flush()

	return tokens
}

// ObservabilityTokenizer emits exact technical atoms together with their
// alphanumeric components. For example, "10.0.0.1" produces the complete IP
// plus 10/0/0/1, while ordinary words are emitted once.
type ObservabilityTokenizer struct{}

func (ObservabilityTokenizer) Tokenize(text string) []string {
	if text == "" {
		return nil
	}
	fields := strings.Fields(text)
	tokens := make([]string, 0, len(fields)*2)
	alnum := AlnumTokenizer{}
	for _, field := range fields {
		atom := strings.TrimFunc(field, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r)
		})
		if atom == "" {
			continue
		}
		parts := alnum.Tokenize(atom)
		if hasTechnicalSeparator(atom) {
			tokens = append(tokens, atom)
		}
		tokens = append(tokens, parts...)
	}
	return tokens
}

func hasTechnicalSeparator(token string) bool {
	for _, r := range token {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return true
		}
	}
	return false
}
