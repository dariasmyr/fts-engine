package fts

import (
	"context"
	"errors"
	"fmt"
	snowballeng "github.com/kljensen/snowball/english"
	"iter"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
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

func Tokenize(content string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, token := range strings.FieldsFunc(content, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsNumber(r)
		}) {
			if !yield(token) {
				return
			}
		}
	}
}

func ToLower(seq iter.Seq[string]) iter.Seq[string] {
	return func(yield func(string) bool) {
		for token := range seq {
			if !yield(strings.ToLower(token)) {
				return
			}
		}
	}
}

func FilterStopWords(seq iter.Seq[string]) iter.Seq[string] {
	return func(yield func(string) bool) {
		for token := range seq {
			if _, ok := stopWords[token]; !ok {
				if !yield(token) {
					return
				}
			}
		}
	}
}

func Stem(seq iter.Seq[string]) iter.Seq[string] {
	return func(yield func(string) bool) {
		for token := range seq {
			if !yield(snowballeng.Stem(token, false)) {
				return
			}
		}
	}
}

func GenerateNGrams(seq iter.Seq[string]) iter.Seq[string] {
	return func(yield func(string) bool) {
		for token := range seq {
			for _, ngram := range generateNGrams(token, 3) {
				if !yield(ngram) {
					return
				}
			}
		}
	}
}

func (fts *FTS) preprocessText(content string) []string {
	tokens := Tokenize(content)
	tokens = ToLower(tokens)
	tokens = FilterStopWords(tokens)
	tokens = Stem(tokens)

	var words []string
	for token := range tokens {
		words = append(words, token)
	}
	return words
}

func generateNGrams(token string, n int) []string {
	var ngrams []string

	if len(token) < n {
		return []string{token}
	}

	for i := 0; i <= len(token)-n; i++ {
		ngrams = append(ngrams, token[i:i+n])
	}

	return ngrams
}

//func (fts *FTS) preprocessText(content string) []string {
//	var processedTokens []string
//
//	tokens := strings.FieldsFunc(content, func(r rune) bool {
//		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
//	})
//
//	for _, token := range tokens {
//		token = strings.ToLower(token)
//		if _, ok := stopWords[token]; !ok {
//			processedTokens = append(processedTokens, snowballeng.Stem(token, false))
//		}
//	}
//
//	return processedTokens
//}

func (fts *FTS) AddDocument(ctx context.Context, content string) (int, error) {
	words := fts.preprocessText(content)

	return fts.documentSaver.AddDocument(ctx, content, words)
}

func (fts *FTS) Search(ctx context.Context, content string) ([]string, error) {
	// Split content by tokens
	tokens := fts.preprocessText(content)
	//fts.log.Debug("Tokens", "tokens", tokens)
	var mu sync.Mutex
	var wg sync.WaitGroup

	docFrequency := make(map[int]int)
	wordMatchCount := make(map[int]int)

	// Find docIDs for every token
	for _, token := range tokens {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			docEntries, err := fts.documentProvider.SearchWord(ctx, token)
			//fts.log.Debug("Doc entries", "docEntries count", len(docEntries), "token", token)
			if err != nil {
				//fts.log.Debug("No doc entries found for word, continue", "word", token)
				return
			}

			localMap := make(map[int]int)
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
					localMap[docID] += count
					//Increase wordMatchCount for doc (how many unique words in doc)
					mu.Lock()
					wordMatchCount[docID]++
					mu.Unlock()
				}
			}

			mu.Lock()
			for docID, count := range localMap {
				docFrequency[docID] += count
			}
			mu.Unlock()
		}(token)
	}

	wg.Wait()

	var docMatches []struct {
		docID         int
		uniqueMatches int
		totalMatches  int
	}

	// Collect all docs from docFrequency to slice
	for docID := range docFrequency {
		docMatches = append(docMatches, struct {
			docID         int
			uniqueMatches int
			totalMatches  int
		}{docID, wordMatchCount[docID], docFrequency[docID]})
	}

	// Sort by unique matches and (if equal) total matches
	sort.Slice(docMatches, func(i, j int) bool {
		if docMatches[i].uniqueMatches == docMatches[j].uniqueMatches {
			return docMatches[i].totalMatches > docMatches[j].totalMatches
		}
		return docMatches[i].uniqueMatches > docMatches[j].uniqueMatches
	})

	maxResultCount := 20
	resultDocs := make([]string, 0, maxResultCount)

	for i := 0; i < len(docMatches) && i < maxResultCount; i++ {
		docData, err := fts.documentProvider.SearchDocument(ctx, docMatches[i].docID)
		if err == nil {
			resultDocs = append(resultDocs, fmt.Sprintf(
				"Doc %d (words:%d, total:%d): %s",
				docMatches[i].docID,
				docMatches[i].uniqueMatches,
				docMatches[i].totalMatches,
				docData,
			))
		}
	}

	return resultDocs, nil
}
