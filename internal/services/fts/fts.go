package fts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"fts-hw/internal/domain/models"
	"iter"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
	SaveDocumentWithIndexing(ctx context.Context, content []byte, words []string, docID string) (string, error)
	SaveDocument(ctx context.Context, content []byte, docID string) (string, error)
	DeleteDocument(ctx context.Context, docId int) error
}

type DocumentProvider interface {
	GetWord(ctx context.Context, word string) ([]string, error)
	GetDocument(ctx context.Context, docID int) (string, error)
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

type ResultDoc struct {
	DocID         int
	UniqueMatches int
	TotalMatches  int
	Doc           string
}

type SearchResult struct {
	ResultDocs        []ResultDoc
	TotalResultsCount int
	Timings           map[string]time.Duration
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
		lastSplit := -1 // Index of last split

		for i, char := range content {
			if !(unicode.IsLetter(char) || unicode.IsNumber(char)) {
				if lastSplit != -1 { // Prevent empty start
					if !yield(content[lastSplit:i]) {
						return
					}
				}
				lastSplit = -1 // Reset lastSplit if a delimiter is found
			} else if lastSplit == -1 { // New word start
				lastSplit = i
			}
		}

		if lastSplit != -1 {
			yield(content[lastSplit:])
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

func (fts *FTS) ProcessDocument(ctx context.Context, document models.Document) (string, error) {
	fmt.Printf("Document Abstract: %s\n", document.Abstract)
	words := fts.preprocessText(document.Abstract)
	fmt.Printf("Words: %v\n", words)

	documentBytes, err := json.Marshal(document)
	if err != nil {
		return "", fmt.Errorf("failed to marshal document to bytes")
	}

	return fts.documentSaver.SaveDocumentWithIndexing(ctx, documentBytes, words, document.ID)
}

func (fts *FTS) Search(ctx context.Context, content string, maxResults int) (SearchResult, error) {
	startTime := time.Now()
	timings := make(map[string]time.Duration)

	preprocessStart := time.Now()
	tokens := fts.preprocessText(content)
	timings["preprocess"] = time.Since(preprocessStart)

	var mu sync.Mutex
	var wg sync.WaitGroup

	docFrequency := make(map[int]int)
	wordMatchCount := make(map[int]int)

	searchStart := time.Now()
	for _, token := range tokens {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			docEntries, err := fts.documentProvider.GetWord(ctx, token)
			if err != nil {
				return
			}

			localMap := make(map[int]int)
			for _, docEntry := range docEntries {
				// Split entries by comma and parse each "docID:count" pair
				pairs := strings.Split(docEntry, ",")

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
	timings["search_words"] = time.Since(searchStart)

	sortStart := time.Now()
	var docMatches []struct {
		docID         int
		uniqueMatches int
		totalMatches  int
	}

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
	timings["sort_results"] = time.Since(sortStart)

	fetchStart := time.Now()
	totalResultsCount := len(docMatches)
	resultDocs := make([]ResultDoc, 0, maxResults)

	for i := 0; i < len(docMatches) && i < maxResults; i++ {
		docData, err := fts.documentProvider.GetDocument(ctx, docMatches[i].docID)
		if err == nil {
			resultDocs = append(resultDocs, ResultDoc{
				DocID:         docMatches[i].docID,
				UniqueMatches: docMatches[i].uniqueMatches,
				TotalMatches:  docMatches[i].totalMatches,
				Doc:           docData,
			})
		}
	}
	timings["fetch_documents"] = time.Since(fetchStart)

	timings["total"] = time.Since(startTime)

	return SearchResult{
		ResultDocs:        resultDocs,
		Timings:           timings,
		TotalResultsCount: totalResultsCount,
	}, nil
}
