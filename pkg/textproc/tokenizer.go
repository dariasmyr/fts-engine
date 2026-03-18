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
