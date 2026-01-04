package trigramtrie

import (
	"context"
	"errors"
	"fmt"
	"fts-hw/internal/domain/models"
	utils "fts-hw/internal/utils/format"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	snowballeng "github.com/kljensen/snowball/english"
)

type Node struct {
	docs          map[string]int
	continuations [26]*Node
}

func newNode() *Node {
	return &Node{
		docs: make(map[string]int),
	}
}

type Trie struct {
	root *Node
	mu   sync.RWMutex
}

func NewTrie() *Trie {
	return &Trie{
		root: newNode(),
	}
}

var ErrInvalidTrigramSize = errors.New("trigram must have exactly 3 characters")

func (t *Trie) Insert(trigram string, docID string) error {
	if len(trigram) != 3 {
		return ErrInvalidTrigramSize
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	node := t.root
	for i := 0; i < 3; i++ {
		index := trigram[i] - 'a'
		if index < 0 || index >= 26 {
			return fmt.Errorf("invalid character in trigram %v", trigram)
		}
		if node.continuations[index] == nil {
			node.continuations[index] = newNode()
		}
		node = node.continuations[index]
	}
	// Increase doc entry count
	node.docs[docID]++
	return nil
}

func (t *Trie) Search(trigram string) (map[string]int, error) {
	if len(trigram) != 3 {
		return nil, ErrInvalidTrigramSize
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	node := t.root
	for i := 0; i < 3; i++ {
		index := trigram[i] - 'a'
		if index < 0 || index >= 26 {
			return nil, fmt.Errorf("invalid character in trigram %v", trigram)
		}
		if node.continuations[index] == nil {
			fmt.Println("Trigram not found")
			return nil, nil
		}
		node = node.continuations[index]
	}
	// Return trigram doc entries
	return node.docs, nil
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

func (t *Trie) IndexDocument(docID string, content string) {
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
			err := t.Insert(trigram, docID)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}
}

func (t *Trie) SearchDocuments(ctx context.Context, query string, maxResults int) (*models.SearchResult, error) {
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
			docEntries, err := t.Search(trigram)
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
