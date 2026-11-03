// Package textproc provides tokenizers, filters, and text-processing pipelines.
package textproc

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	snowballeng "github.com/kljensen/snowball/english"
	snowballrus "github.com/kljensen/snowball/russian"
)

type Filter interface {
	// Apply may mutate tokens and return the same backing storage.
	Apply(tokens []string) []string
	// Fingerprint must cover every filter setting that can affect output.
	Fingerprint() string
}

type Pipeline struct {
	name      string
	tokenizer Tokenizer
	filters   []Filter
}

// NewNamedPipeline creates a validated pipeline whose fingerprint covers its
// tokenizer and ordered filters. Name is diagnostic metadata only. Custom
// components must return a new fingerprint whenever their behavior changes.
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

func (p Pipeline) Descriptor() fts.PipelineDescriptor {
	descriptor, err := pipelineDescriptor(p.name, p.tokenizer, p.filters)
	if err != nil {
		return fts.PipelineDescriptor{Name: p.name}
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

func (LowercaseFilter) Fingerprint() string {
	return unicodeFingerprint("lowercase-filter/v1")
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

func (f MinLengthOrNumericFilter) Fingerprint() string {
	return minLengthFingerprint(f.MinLength)
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

func (UniqueFilter) Fingerprint() string {
	return componentFingerprint("unique-filter/v1")
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

func (EnglishStopwordFilter) Fingerprint() string {
	return componentFingerprint(
		"english-stopword-filter/v1",
		"dictionary="+snowballSource+"/english-stopwords",
	)
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

func (EnglishStemFilter) Fingerprint() string {
	return componentFingerprint(
		"english-stem-filter/v1",
		"implementation="+snowballSource+"/english",
		"stem-stop-words=false",
	)
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

func (RussianStopwordFilter) Fingerprint() string {
	return componentFingerprint(
		"russian-stopword-filter/v1",
		"dictionary="+snowballSource+"/russian-stopwords",
	)
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

func (RussianStemFilter) Fingerprint() string {
	return componentFingerprint(
		"russian-stem-filter/v1",
		"implementation="+snowballSource+"/russian",
		"stem-stop-words=false",
	)
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

func (MultilingualStopwordFilter) Fingerprint() string {
	return componentFingerprint(
		"multilingual-stopword-filter/v1",
		"english-dictionary="+snowballSource+"/english-stopwords",
		"russian-dictionary="+snowballSource+"/russian-stopwords",
		"unicode="+unicode.Version,
	)
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

func (MultilingualStemFilter) Fingerprint() string {
	return componentFingerprint(
		"multilingual-stem-filter/v1",
		"english-implementation="+snowballSource+"/english",
		"russian-implementation="+snowballSource+"/russian",
		"stem-stop-words=false",
		"unicode="+unicode.Version,
	)
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
