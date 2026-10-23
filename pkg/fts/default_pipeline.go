package fts

import (
	"strings"
	"unicode"
)

type defaultPipeline struct{}

func (defaultPipeline) Descriptor() AnalyzerDescriptor {
	descriptor, err := NewAnalyzerDescriptor("default", 1)
	if err != nil {
		panic(err)
	}
	return descriptor
}

func (defaultPipeline) Process(text string) []string {
	if text == "" {
		return nil
	}

	tokens := make([]string, 0, 16)
	var b strings.Builder

	flush := func() {
		if b.Len() == 0 {
			return
		}
		tokens = append(tokens, strings.ToLower(b.String()))
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
