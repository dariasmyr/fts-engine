package fts_kv

import (
	"context"
	"errors"
	"fts-hw/internal/domain/models"
	utils "fts-hw/internal/utils/format"
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

type KeyValueFTS struct {
	log              *slog.Logger
	documentSaver    DocumentSaver
	documentProvider DocumentProvider
}

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
)

type DocumentSaver interface {
	SaveDocumentWithIndexing(ctx context.Context, doc *models.Document, words []string) (string, error)
	SaveDocument(ctx context.Context, doc *models.Document) (string, error)
	DeleteDocument(ctx context.Context, docId string) error
}

type DocumentProvider interface {
	GetWord(ctx context.Context, word string) ([]string, error)
	GetDocument(ctx context.Context, docID string) (*models.Document, error)
}

func New(
	log *slog.Logger,
	documentSaver DocumentSaver,
	documentProvider DocumentProvider,
) *KeyValueFTS {
	return &KeyValueFTS{
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
		lastSplit := 0

		for i, char := range content {
			if !unicode.IsLetter(char) && !unicode.IsNumber(char) {
				if i-lastSplit != 0 && !yield(content[lastSplit:i]) {
					return
				}
				lastSplit = i + 1
			}
		}

		if len(content)-lastSplit != 0 {
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

func (fts *KeyValueFTS) preprocessText(content string) []string {
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

func (fts *KeyValueFTS) ProcessDocument(ctx context.Context, document *models.Document) (string, error) {
	words := fts.preprocessText(document.Abstract)

	return fts.documentSaver.SaveDocumentWithIndexing(ctx, document, words)
}

func (fts *KeyValueFTS) SearchDocuments(ctx context.Context, query string, maxResults int) (*models.SearchResult, error) {
	startTime := time.Now()
	timings := make(map[string]string)

	preprocessStart := time.Now()
	tokens := fts.preprocessText(query)
	timings["preprocess"] = utils.FormatDuration(time.Since(preprocessStart))

	var mu sync.Mutex
	var wg sync.WaitGroup

	docFrequency := make(map[string]int)
	wordMatchCount := make(map[string]int)

	searchStart := time.Now()
	for _, token := range tokens {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			docEntries, err := fts.documentProvider.GetWord(ctx, token)
			if err != nil {
				return
			}

			localMap := make(map[string]int)
			for _, docEntry := range docEntries {
				// Split entries by comma and parse each "docID:count" pair
				pairs := strings.Split(docEntry, ",")

				// Parse the stored index data (word = docID:count pairs)
				for _, pair := range pairs {
					parts := strings.Split(pair, ":")
					if len(parts) != 2 {
						continue // Skip invalid entries
					}
					docID := parts[0]
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
		docID         string
		uniqueMatches int
		totalMatches  int
	}

	for docID := range docFrequency {
		docMatches = append(docMatches, struct {
			docID         string
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
	timings["search_tokens"] = utils.FormatDuration(time.Since(searchStart))

	totalResultsCount := len(docMatches)
	resultDocs := make([]models.ResultData, 0, maxResults)
	for i := 0; i < len(docMatches) && i < maxResults; i++ {
		resultDocs = append(resultDocs, models.ResultData{
			ID:            docMatches[i].docID,
			UniqueMatches: docMatches[i].uniqueMatches,
			TotalMatches:  docMatches[i].totalMatches,
			Document:      models.Document{},
		})
	}
	timings["total"] = utils.FormatDuration(time.Since(startTime))

	for i := 0; i < len(resultDocs); i++ {
		doc, err := fts.documentProvider.GetDocument(ctx, resultDocs[i].ID)
		if err != nil {
			return nil, err
		}
		resultDocs[i].Document = *doc
	}

	return &models.SearchResult{
		ResultData:        resultDocs,
		Timings:           timings,
		TotalResultsCount: totalResultsCount,
	}, nil
}
