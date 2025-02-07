package fts

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"unicode"

	snowballeng "github.com/kljensen/snowball/english"
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
	SearchWord(ctx context.Context, word string) ([]string, error)
	SearchDocument(ctx context.Context, docID int) (string, error)
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

func (fts *FTS) preprocessText(content string) []string {
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
	words := fts.preprocessText(content)
	return fts.documentSaver.AddDocument(ctx, content, words)
}

func (fts *FTS) Search(ctx context.Context, content string) ([]string, error) {
	// Split content by tokens
	tokens := fts.preprocessText(content)

	docFrequency := make(map[int]int)

	// Find docIDs for every token
	for _, token := range tokens {
		docEntries, err := fts.documentProvider.SearchWord(ctx, token)
		if err != nil {
			fts.log.Debug("No doc entries found for word, continue", "word", token)
			continue
		}

		for _, docEntry := range docEntries {
			// Split entries by comma and parse each "docID:count" pair
			pairs := strings.Split(string(docEntry), ",")

			// Parse the stored index data (word = docID:count pairs)
			for _, pair := range pairs {
				parts := strings.Split(pair, ":")
				if len(parts) != 2 {
					continue // Skip invalid entries
				}
				docID, _ := strconv.Atoi(parts[0])
				count, _ := strconv.Atoi(parts[1])

				//Increase docFrequency by word match count for doc
				docFrequency[docID] += count
			}
		}
	}

	var docMatches []struct {
		docID   int
		matches int
	}

	// Collect all docs from docFrequency to slice
	for docID, matched := range docFrequency {
		docMatches = append(docMatches, struct {
			docID   int
			matches int
		}{docID, matched})
	}

	// Sort my matches count
	sort.Slice(docMatches, func(i, j int) bool {
		return docMatches[i].matches > docMatches[j].matches
	})

	maxResultCount := 20
	resultDocs := make([]string, 0, maxResultCount)

	for i := 0; i < len(docMatches) && i < maxResultCount; i++ {
		docData, err := fts.documentProvider.SearchDocument(ctx, docMatches[i].docID)
		if err == nil {
			resultDocs = append(resultDocs, fmt.Sprintf("Doc %d (x%d): %s", docMatches[i].docID, docMatches[i].matches, docData))
		}
	}

	return resultDocs, nil
}
