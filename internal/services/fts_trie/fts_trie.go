package fts_trie

import (
	"context"
	"errors"
	"fmt"
	"fts-hw/internal/domain/models"
	utils "fts-hw/internal/utils/format"
	"sort"
	"time"
	"unicode"
	"unicode/utf8"

	snowballeng "github.com/kljensen/snowball/english"
)

type Node struct {
	Docs          map[string]int
	Continuations [26]*Node
}

type ResultDocIDs struct {
	DocID         string
	UniqueMatches int
	TotalMatches  int
}

var ErrInvalidCharacter = errors.New("invalid character in trigram")
var ErrInvalidTrigramSize = errors.New("trigram must have exactly 3 characters")

func NewNode() *Node {
	return &Node{
		Docs: make(map[string]int),
	}
}

func (n *Node) Insert(trigram string, docID string) error {
	if len(trigram) != 3 {
		return ErrInvalidTrigramSize
	}

	node := n
	for i := 0; i < 3; i++ {
		index := trigram[i] - 'a'
		if index < 0 || index >= 26 {
			return ErrInvalidCharacter
		}
		if node.Continuations[index] == nil {
			node.Continuations[index] = NewNode()
		}
		node = node.Continuations[index]
	}
	// Increase doc entry count
	node.Docs[docID]++
	return nil
}

func (n *Node) Search(trigram string) (map[string]int, error) {
	if len(trigram) != 3 {
		return nil, ErrInvalidTrigramSize
	}

	node := n
	for i := 0; i < 3; i++ {
		index := trigram[i] - 'a'
		if index < 0 || index >= 26 {
			return nil, ErrInvalidCharacter
		}
		if node.Continuations[index] == nil {
			fmt.Println("Trigram not found")
			return nil, nil
		}
		node = node.Continuations[index]
	}
	// Return trigram doc entries
	return node.Docs, nil
}

func getTrigrams(token string) []string {
	if len(token) < 3 {
		return nil
	}
	trigrams := make([]string, 0, 3)
	for i := 0; i < len(token)-2; i++ {
		trigrams = append(trigrams, token[i:i+3])
	}
	return trigrams
}

func tokenize(content string) []string {
	lastSplit := 0
	tokens := make([]string, 0)
	for i, char := range content {
		if unicode.IsLetter(char) {
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

func (n *Node) IndexDocument(docID string, content string) {
	tokens := tokenize(content)
	for _, token := range tokens {
		// skip stop words
		if snowballeng.IsStopWord(token) {
			continue
		}
		//lowercase and stemmimg (eng only)
		token = snowballeng.Stem(token, false)
		trigrams := getTrigrams(token)
		for _, trigram := range trigrams {
			n.Insert(trigram, docID)
		}
	}
}

func (n *Node) SearchDocuments(ctx context.Context, query string, maxResults int) (*models.SearchResult, error) {
	results := make([]models.ResultData, 0, maxResults)
	currentIndex := 0

	startTime := time.Now()
	timings := make(map[string]string)

	preprocessStart := time.Now()
	tokens := tokenize(query)
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
		trigrams := getTrigrams(token)
		if len(trigrams) == 0 {
			return nil, ErrInvalidTrigramSize
		}

		for _, trigram := range trigrams {
			docEntries, err := n.Search(trigram)
			if err != nil {
				return nil, err
			}
			if docEntries == nil {
				continue
			}
			for docID, count := range docEntries {
				docUniqueMatches[docID]++
				docTotalMatches[docID] += count
			}
		}

		for docID, uniqueMatches := range docUniqueMatches {
			if currentIndex >= maxResults {
				break
			}
			results = append(results, models.ResultData{
				ID:            docID,
				UniqueMatches: uniqueMatches,
				TotalMatches:  docTotalMatches[docID],
				Document:      models.Document{},
			})
			currentIndex++
		}

		sort.Slice(results[:currentIndex], func(i, j int) bool {
			if results[i].UniqueMatches == results[j].UniqueMatches {
				return results[i].TotalMatches > results[j].TotalMatches
			}
			return results[i].UniqueMatches > results[j].UniqueMatches
		})
	}
	timings["search_tokens"] = utils.FormatDuration(time.Since(searchStart))

	totalResultsCount := len(results[:currentIndex])

	timings["total"] = utils.FormatDuration(time.Since(startTime))

	return &models.SearchResult{
		ResultData:        results[:currentIndex],
		Timings:           timings,
		TotalResultsCount: totalResultsCount,
	}, nil
}
