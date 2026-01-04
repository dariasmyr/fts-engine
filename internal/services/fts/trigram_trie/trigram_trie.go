package trigramtrie

import (
	"errors"
	"fmt"
	"sync"
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

func (t *Trie) Search(word string) (map[string]int, error) {
	if len(word) != 3 {
		return nil, ErrInvalidTrigramSize
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	node := t.root
	for i := 0; i < 3; i++ {
		index := word[i] - 'a'
		if index < 0 || index >= 26 {
			return nil, fmt.Errorf("invalid character in trigram %v", word)
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

func TrigramKeys(token string) ([]string, error) {
	trigrams := getTrigrams(token)
	if len(trigrams) == 0 {
		return nil, ErrInvalidTrigramSize
	}
	return trigrams, nil
}
