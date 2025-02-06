package fts

import (
	"context"
	"errors"
	snowballeng "github.com/kljensen/snowball/english"
	"log/slog"
	"strings"
	"unicode"
)

type FTS struct {
	log              *slog.Logger
	documentSaver    DocumentSaver
	documentProvider DocumentProvider
}

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type DocumentSaver interface {
	AddDocument(ctx context.Context, content string, words []string) (int, error)
	DeleteDocument(ctx context.Context, docId int) error
}

type DocumentProvider interface {
	Search(ctx context.Context, word string) ([]string, error)
}

func New(
	log *slog.Logger,
	documentSaver DocumentSaver,
	documentProvider DocumentProvider,
) *FTS {
	return &FTS{
		log:              log,
		documentSaver:    documentSaver,
		documentProvider: documentProvider,
	}
}

var stopWords = map[string]struct{}{
	"a":       {},
	"an":      {},
	"and":     {},
	"are":     {},
	"as":      {},
	"at":      {},
	"be":      {},
	"but":     {},
	"by":      {},
	"for":     {},
	"if":      {},
	"in":      {},
	"into":    {},
	"is":      {},
	"it":      {},
	"no":      {},
	"not":     {},
	"of":      {},
	"on":      {},
	"or":      {},
	"such":    {},
	"that":    {},
	"the":     {},
	"their":   {},
	"then":    {},
	"there":   {},
	"these":   {},
	"they":    {},
	"this":    {},
	"to":      {},
	"was":     {},
	"were":    {},
	"will":    {},
	"with":    {},
	"i":       {},
	"me":      {},
	"my":      {},
	"mine":    {},
	"we":      {},
	"us":      {},
	"our":     {},
	"ours":    {},
	"you":     {},
	"your":    {},
	"yours":   {},
	"he":      {},
	"him":     {},
	"his":     {},
	"she":     {},
	"her":     {},
	"hers":    {},
	"himself": {},
	"herself": {},
}

func (fts *FTS) PreprocessText(content string) []string {
	tokens := fts.tokenize(content)
	tokens = fts.toLowercase(tokens)
	tokens = fts.filterStopWords(tokens)
	tokens = fts.stemWords(tokens)
	return tokens
}

func (fts *FTS) tokenize(content string) []string {
	return strings.FieldsFunc(content, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
}

func (fts *FTS) toLowercase(tokens []string) []string {
	lowercaseTokens := make([]string, len(tokens))

	for i, token := range tokens {
		lowercaseTokens[i] = strings.ToLower(token)
	}

	return lowercaseTokens
}

func (fts *FTS) filterStopWords(tokens []string) []string {
	filteredWords := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if _, ok := stopWords[token]; !ok {
			filteredWords = append(filteredWords, token)
		}
	}

	return filteredWords
}

func (fts *FTS) stemWords(tokens []string) []string {
	stemmedWords := make([]string, len(tokens))

	for i, token := range tokens {
		stemmedWords[i] = snowballeng.Stem(token, false)
	}

	return stemmedWords
}

func (fts *FTS) AddDocument(ctx context.Context, content string) (int, error) {
	words := fts.PreprocessText(content)
	return fts.documentSaver.AddDocument(ctx, content, words)
}
