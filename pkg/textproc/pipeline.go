// Package textproc provides tokenizers, filters, and analyzer pipelines.
package textproc

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	snowballeng "github.com/kljensen/snowball/english"
	snowballrus "github.com/kljensen/snowball/russian"
)

type Filter interface {
	Apply(tokens []string) []string
	// Descriptor must cover every filter setting that can affect output.
	Descriptor() ComponentDescriptor
}

type Pipeline struct {
	name      string
	tokenizer Tokenizer
	filters   []Filter
}

type Descriptor = fts.AnalyzerDescriptor

// NewNamedPipeline creates a validated pipeline whose fingerprint covers its
// tokenizer and ordered filters. Name is diagnostic metadata only. Custom
// components must remain consistent with their descriptors and update component
// versions when an implementation change is not represented by parameters.
func NewNamedPipeline(name string, tokenizer Tokenizer, filters ...Filter) (Pipeline, error) {
	if tokenizer == nil {
		tokenizer = AlnumTokenizer{}
	}
	filters = append([]Filter(nil), filters...)
	descriptor, err := pipelineDescriptor(name, tokenizer, filters)
	if err != nil {
		return Pipeline{}, err
	}

	return Pipeline{
		name:      descriptor.Name,
		tokenizer: tokenizer,
		filters:   filters,
	}, nil
}

func (p Pipeline) Process(text string) []string {
	tokens := p.tokenizer.Tokenize(text)
	for _, filter := range p.filters {
		if filter == nil {
			continue
		}
		tokens = filter.Apply(tokens)
	}
	return tokens
}

func (p Pipeline) Descriptor() Descriptor {
	descriptor, err := pipelineDescriptor(p.name, p.tokenizer, p.filters)
	if err != nil {
		return Descriptor{Name: p.name}
	}
	return descriptor
}

func DefaultEnglishPipeline() Pipeline {
	return mustNewNamedPipeline("english",
		AlnumTokenizer{},
		LowercaseFilter{},
		MinLengthOrNumericFilter{MinLength: 3},
		EnglishStopwordFilter{},
		EnglishStemFilter{},
	)
}

func DefaultRussianPipeline() Pipeline {
	return mustNewNamedPipeline("russian",
		AlnumTokenizer{},
		LowercaseFilter{},
		MinLengthOrNumericFilter{MinLength: 3},
		RussianStopwordFilter{},
		RussianStemFilter{},
	)
}

func DefaultMultilingualPipeline() Pipeline {
	return mustNewNamedPipeline("multilingual",
		AlnumTokenizer{},
		LowercaseFilter{},
		MinLengthOrNumericFilter{MinLength: 3},
		MultilingualStopwordFilter{},
		MultilingualStemFilter{},
	)
}

// ObservabilityPipeline preserves technical identifiers and short diagnostic
// terms. It deliberately does not remove stopwords or stem tokens.
func ObservabilityPipeline() Pipeline {
	return mustNewNamedPipeline("observability",
		ObservabilityTokenizer{},
		LowercaseFilter{},
		MinLengthOrNumericFilter{MinLength: 2},
		UniqueFilter{},
	)
}

func mustNewNamedPipeline(name string, tokenizer Tokenizer, filters ...Filter) Pipeline {
	pipeline, err := NewNamedPipeline(name, tokenizer, filters...)
	if err != nil {
		panic(err)
	}
	return pipeline
}

type LowercaseFilter struct{}

func (LowercaseFilter) Descriptor() ComponentDescriptor {
	return component("lowercase", unicodeParams())
}

func (LowercaseFilter) Apply(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}

	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		out = append(out, strings.ToLower(token))
	}
	return out
}

type MinLengthOrNumericFilter struct {
	MinLength int
}

func (f MinLengthOrNumericFilter) Descriptor() ComponentDescriptor {
	return component("min_length_or_numeric", minLengthParams(f.MinLength))
}

func (f MinLengthOrNumericFilter) Apply(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}

	minLen := f.MinLength
	if minLen <= 0 {
		minLen = 1
	}

	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if isNumericToken(token) || utf8.RuneCountInString(token) >= minLen {
			out = append(out, token)
		}
	}
	return out
}

// UniqueFilter removes duplicate tokens while preserving first-seen order.
type UniqueFilter struct{}

func (UniqueFilter) Descriptor() ComponentDescriptor {
	return component("unique", nil)
}

func (UniqueFilter) Apply(tokens []string) []string {
	if len(tokens) < 2 {
		return tokens
	}
	seen := make(map[string]struct{}, len(tokens))
	out := tokens[:0]
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		out = append(out, token)
	}
	return out
}

type EnglishStopwordFilter struct{}

func (EnglishStopwordFilter) Descriptor() ComponentDescriptor {
	return component("english_stopwords", map[string]string{
		"dictionary": snowballVersion + "/english-stopwords",
	})
}

func (EnglishStopwordFilter) Apply(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}

	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if isNumericToken(token) {
			out = append(out, token)
			continue
		}
		if snowballeng.IsStopWord(token) {
			continue
		}
		out = append(out, token)
	}
	return out
}

type EnglishStemFilter struct{}

func (EnglishStemFilter) Descriptor() ComponentDescriptor {
	return component("english_stem", map[string]string{
		"implementation":  snowballVersion + "/english",
		"stem_stop_words": "false",
	})
}

func (EnglishStemFilter) Apply(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}

	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if isNumericToken(token) {
			out = append(out, token)
			continue
		}
		out = append(out, snowballeng.Stem(token, false))
	}
	return out
}

type RussianStopwordFilter struct{}

func (RussianStopwordFilter) Descriptor() ComponentDescriptor {
	return component("russian_stopwords", map[string]string{
		"dictionary": snowballVersion + "/russian-stopwords",
	})
}

func (RussianStopwordFilter) Apply(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}

	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if isNumericToken(token) {
			out = append(out, token)
			continue
		}
		if snowballrus.IsStopWord(token) {
			continue
		}
		out = append(out, token)
	}
	return out
}

type RussianStemFilter struct{}

func (RussianStemFilter) Descriptor() ComponentDescriptor {
	return component("russian_stem", map[string]string{
		"implementation":  snowballVersion + "/russian",
		"stem_stop_words": "false",
	})
}

func (RussianStemFilter) Apply(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}

	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if isNumericToken(token) {
			out = append(out, token)
			continue
		}
		out = append(out, snowballrus.Stem(token, false))
	}
	return out
}

type MultilingualStopwordFilter struct{}

func (MultilingualStopwordFilter) Descriptor() ComponentDescriptor {
	return component("multilingual_stopwords", map[string]string{
		"english_dictionary": snowballVersion + "/english-stopwords",
		"russian_dictionary": snowballVersion + "/russian-stopwords",
		"unicode_version":    unicode.Version,
	})
}

func (MultilingualStopwordFilter) Apply(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}

	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if isNumericToken(token) {
			out = append(out, token)
			continue
		}

		switch tokenScript(token) {
		case scriptLatin:
			if snowballeng.IsStopWord(token) {
				continue
			}
		case scriptCyrillic:
			if snowballrus.IsStopWord(token) {
				continue
			}
		}

		out = append(out, token)
	}
	return out
}

type MultilingualStemFilter struct{}

func (MultilingualStemFilter) Descriptor() ComponentDescriptor {
	return component("multilingual_stem", map[string]string{
		"english_implementation": snowballVersion + "/english",
		"russian_implementation": snowballVersion + "/russian",
		"stem_stop_words":        "false",
		"unicode_version":        unicode.Version,
	})
}

func (MultilingualStemFilter) Apply(tokens []string) []string {
	if len(tokens) == 0 {
		return tokens
	}

	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		if isNumericToken(token) {
			out = append(out, token)
			continue
		}

		switch tokenScript(token) {
		case scriptLatin:
			out = append(out, snowballeng.Stem(token, false))
		case scriptCyrillic:
			out = append(out, snowballrus.Stem(token, false))
		default:
			out = append(out, token)
		}
	}
	return out
}

type scriptKind uint8

const (
	scriptUnknown scriptKind = iota
	scriptLatin
	scriptCyrillic
	scriptMixed
)

func tokenScript(token string) scriptKind {
	var hasLatin bool
	var hasCyrillic bool

	for _, r := range token {
		if unicode.In(r, unicode.Latin) {
			hasLatin = true
		}
		if unicode.In(r, unicode.Cyrillic) {
			hasCyrillic = true
		}
		if hasLatin && hasCyrillic {
			return scriptMixed
		}
	}

	if hasLatin {
		return scriptLatin
	}
	if hasCyrillic {
		return scriptCyrillic
	}
	return scriptUnknown
}

func isNumericToken(token string) bool {
	if token == "" {
		return false
	}
	_, err := strconv.ParseUint(token, 10, 64)
	return err == nil
}
