package textproc

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	snowballeng "github.com/kljensen/snowball/english"
	snowballrus "github.com/kljensen/snowball/russian"
)

type Filter interface {
	Apply(tokens []string) []string
}

type Pipeline struct {
	tokenizer  Tokenizer
	filters    []Filter
	descriptor Descriptor
}

// Descriptor is the stable identity of an analysis contract. Persisted indexes
// should store Fingerprint and reject or rebuild on mismatch.
type Descriptor struct {
	Name        string
	Version     uint32
	Fingerprint string
}

func NewDescriptor(name string, version uint32) Descriptor {
	spec := fmt.Sprintf("%s@%d", name, version)
	sum := sha256.Sum256([]byte(spec))
	return Descriptor{Name: name, Version: version, Fingerprint: hex.EncodeToString(sum[:])}
}

func NewPipeline(tokenizer Tokenizer, filters ...Filter) Pipeline {
	return NewNamedPipeline("custom", 0, tokenizer, filters...)
}

// NewNamedPipeline creates a pipeline with a caller-owned compatibility ID.
// Increment version whenever token output can change for the same input.
func NewNamedPipeline(name string, version uint32, tokenizer Tokenizer, filters ...Filter) Pipeline {
	if tokenizer == nil {
		tokenizer = AlnumTokenizer{}
	}
	if name == "" {
		name = "custom"
	}

	return Pipeline{
		tokenizer:  tokenizer,
		filters:    filters,
		descriptor: NewDescriptor(name, version),
	}
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

func (p Pipeline) Descriptor() Descriptor { return p.descriptor }

func (p Pipeline) AnalyzerName() string { return p.descriptor.Name }

func (p Pipeline) AnalyzerVersion() uint32 { return p.descriptor.Version }

func (p Pipeline) AnalyzerFingerprint() string { return p.descriptor.Fingerprint }

func DefaultEnglishPipeline() Pipeline {
	return NewNamedPipeline("english", 1,
		AlnumTokenizer{},
		LowercaseFilter{},
		MinLengthOrNumericFilter{MinLength: 3},
		EnglishStopwordFilter{},
		EnglishStemFilter{},
	)
}

func DefaultRussianPipeline() Pipeline {
	return NewNamedPipeline("russian", 1,
		AlnumTokenizer{},
		LowercaseFilter{},
		MinLengthOrNumericFilter{MinLength: 3},
		RussianStopwordFilter{},
		RussianStemFilter{},
	)
}

func DefaultMultilingualPipeline() Pipeline {
	return NewNamedPipeline("multilingual", 1,
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
	return NewNamedPipeline("observability", 1,
		ObservabilityTokenizer{},
		LowercaseFilter{},
		MinLengthOrNumericFilter{MinLength: 2},
		UniqueFilter{},
	)
}

type LowercaseFilter struct{}

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
