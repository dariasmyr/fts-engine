package fts_trie

import (
	"errors"
	"fmt"
	"sort"
	"unicode"

	snowballeng "github.com/kljensen/snowball/english"
)

type Node struct {
	Docs          map[string]int
	Continuations [26]*Node
}

type ResultDoc struct {
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
			fmt.Printf("Trigram not found")
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
	fmt.Printf("Trigrams: %v \n", trigrams)
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

		lastSplit = i + 1
	}

	if len(content) > lastSplit {
		tokens = append(tokens, content[lastSplit:])
	}

	return tokens
}

func IndexDocument(node *Node, docID string, content string) {
	fmt.Printf("Content to index: %s \n", content)
	tokens := tokenize(content)
	fmt.Printf("Tokens to index: %v \n", tokens)
	for _, token := range tokens {
		// skip stop words
		if snowballeng.IsStopWord(token) {
			continue
		}
		//lowercase and stemmimg (eng only)
		token = snowballeng.Stem(token, false)
		fmt.Printf("Processed word: %s\n", token)
		trigrams := getTrigrams(token)
		for _, trigram := range trigrams {
			node.Insert(trigram, docID)
		}
	}
}

func (n *Node) SearchDocuments(query string, maxResults int) ([]ResultDoc, error) {
	results := make([]ResultDoc, 0, maxResults)
	currentIndex := 0

	tokens := tokenize(query)
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

		docUniqueMatches := make(map[string]int)
		docTotalMatches := make(map[string]int)

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
			results[currentIndex] = ResultDoc{
				DocID:         docID,
				UniqueMatches: uniqueMatches,
				TotalMatches:  docTotalMatches[docID],
			}
			currentIndex++
		}

		sort.Slice(results[:currentIndex], func(i, j int) bool {
			if results[i].UniqueMatches == results[j].UniqueMatches {
				return results[i].TotalMatches > results[j].TotalMatches
			}
			return results[i].UniqueMatches > results[j].UniqueMatches
		})
	}

	return results[:currentIndex], nil
}
