package fts

import (
	"context"
	"fmt"
	"fts-hw/internal/domain/models"
	utils "fts-hw/internal/utils/format"
	snowballeng "github.com/kljensen/snowball/english"
	"sort"
	"time"
	"unicode/utf8"
)

type Index interface {
	Search(key string) (map[string]int, error)
	Insert(key string, docID string) error
}

type KeyGenerator func(token string) ([]string, error)

type SearchService struct {
	index  Index
	keyGen KeyGenerator
}

func NewSearchService(index Index, keyGen KeyGenerator) *SearchService {
	return &SearchService{
		index:  index,
		keyGen: keyGen,
	}
}

func Tokenize(content string) []string {
	lastSplit := 0
	tokens := make([]string, 0)
	for i, char := range content {
		if char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' {
			continue
		}

		if i-lastSplit != 0 {
			tokens = append(tokens, content[lastSplit:i])
		}

		charBytes := utf8.RuneLen(char)
		// Update lastSplit considering the byte length of the character
		// We don't use `i + 1` because characters can occupy more than one byte in UTF-8.
		lastSplit = i + charBytes // account for the character's byte length
	}

	if len(content) > lastSplit {
		tokens = append(tokens, content[lastSplit:])
	}

	return tokens
}

func (s *SearchService) IndexDocument(
	ctx context.Context,
	docID string,
	content string,
) error {
	tokens := Tokenize(content)
	for _, token := range tokens {
		// skip stop words
		if snowballeng.IsStopWord(token) {
			continue
		}
		//lowercase and stemmimg (eng only)
		token = snowballeng.Stem(token, false)
		keys, err := s.keyGen(token)
		if err != nil {
			return fmt.Errorf("trie: index document: %w", err)
		}

		for _, trigram := range keys {
			insertErr := s.index.Insert(trigram, docID)
			if insertErr != nil {
				return fmt.Errorf("trie: insert document while indexing: %w", insertErr)
			}
		}
	}

	return nil
}

func (s *SearchService) SearchDocuments(
	ctx context.Context,
	query string,
	maxResults int,
) (*models.SearchResult, error) {
	startTime := time.Now()
	timings := make(map[string]string)

	preprocessStart := time.Now()
	tokens := Tokenize(query)
	timings["preprocess"] = utils.FormatDuration(time.Since(preprocessStart))

	searchStart := time.Now()

	docUniqueMatches := make(map[string]int)
	docTotalMatches := make(map[string]int)

	for _, token := range tokens {
		// skip stop words
		if snowballeng.IsStopWord(token) {
			continue
		}
		//lowercase and stemmimg (eng only)
		token = snowballeng.Stem(token, false)

		keys, err := s.keyGen(token)
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			docEntries, searchErr := s.index.Search(key)
			if searchErr != nil {
				return nil, searchErr
			}
			if docEntries == nil {
				continue
			}
			for docID, count := range docEntries {
				docUniqueMatches[docID]++
				docTotalMatches[docID] += count
			}
		}

	}

	if len(docUniqueMatches) < maxResults {
		maxResults = len(docUniqueMatches)
	}

	results := make([]models.ResultData, 0, len(docUniqueMatches))
	for docID, uniqueMatches := range docUniqueMatches {
		results = append(results, models.ResultData{
			ID:            docID,
			UniqueMatches: uniqueMatches,
			TotalMatches:  docTotalMatches[docID],
			Document:      models.Document{},
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].UniqueMatches == results[j].UniqueMatches {
			return results[i].TotalMatches > results[j].TotalMatches
		}
		return results[i].UniqueMatches > results[j].UniqueMatches
	})

	timings["search_tokens"] = utils.FormatDuration(time.Since(searchStart))

	timings["total"] = utils.FormatDuration(time.Since(startTime))

	var lastIndex int
	lastIndex = maxResults

	if len(docUniqueMatches) > maxResults {
		lastIndex = len(docUniqueMatches)
	}

	return &models.SearchResult{
		ResultData:        results[:lastIndex],
		Timings:           timings,
		TotalResultsCount: len(docUniqueMatches),
	}, nil
}
