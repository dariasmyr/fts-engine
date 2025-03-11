package fts_trie

import (
	"sort"
	"unicode"
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

func NewNode() *Node {
	return &Node{
		Docs: make(map[string]int),
	}
}

func (n *Node) Insert(trigram string, docID string) {
	if len(trigram) != 3 {
		return
	}

	node := n
	for i := 0; i < 3; i++ {
		index := trigram[i] - 'a'
		if node.Continuations[index] == nil {
			node.Continuations[index] = NewNode()
		}
		node = node.Continuations[index]
	}
	// Increase doc entry count
	node.Docs[docID]++
}

func (n *Node) Search(trigram string) map[string]int {
	if len(trigram) != 3 {
		return nil
	}

	node := n
	for i := 0; i < 3; i++ {
		index := trigram[i] - 'a'
		if node.Continuations[index] == nil {
			return nil
		}
		node = node.Continuations[index]
	}
	// Return trigram doc entries
	return node.Docs
}

func getTrigrams(token string) []string {
	if len(token) < 3 {
		return nil
	}
	trigrams := make([]string, 3)
	for i := 0; i < len(token)-3; i++ {
		trigrams = append(trigrams, token[i:i+3])
	}
	return trigrams
}

func tokenize(content string) []string {
	lastIndex := 0
	tokens := make([]string, 0)
	for i, char := range content {
		if unicode.IsLetter(char) || unicode.IsNumber(char) {
			if i-lastIndex != 0 {
				tokens = append(tokens, content[lastIndex:i])
			}
			lastIndex = i + 1
		}
	}
	if lastIndex < len(content) {
		tokens = append(tokens, content[lastIndex:])
	}
	return tokens
}

func IndexDocument(node *Node, docID string, content string) {
	tokens := tokenize(content)
	for _, token := range tokens {
		trigrams := getTrigrams(token)
		for _, trigram := range trigrams {
			node.Insert(trigram, docID)
		}
	}
}

func (n *Node) SearchDocuments(query string, maxResults int) []ResultDoc {
	results := make([]ResultDoc, 0, maxResults)
	currentIndex := 0

	tokens := tokenize(query)
	for _, token := range tokens {
		trigrams := getTrigrams(token)
		if len(trigrams) == 0 {
			return nil
		}

		docUniqueMatches := make(map[string]int)
		docTotalMatches := make(map[string]int)

		for _, trigram := range trigrams {
			docEntries := n.Search(trigram)
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

		sort.Slice(results, func(i, j int) bool {
			if results[i].UniqueMatches == results[j].UniqueMatches {
				return results[i].TotalMatches > results[j].TotalMatches
			}
			return results[i].UniqueMatches > results[j].UniqueMatches
		})
	}

	return results
}
