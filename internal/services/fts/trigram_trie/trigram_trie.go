package trigramtrie

import (
	"errors"
	"fmt"
	"fts-hw/internal/utils"
	"sync"
)

type Node struct {
	docs     map[string]int
	children [26]*Node
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
		if node.children[index] == nil {
			node.children[index] = newNode()
		}
		node = node.children[index]
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
		if node.children[index] == nil {
			fmt.Println("Trigram not found")
			return nil, nil
		}
		node = node.children[index]
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

func (t *Trie) Analyze() utils.TrieStats {
	var s utils.TrieStats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var dfs func(n *Node, depth int)
	dfs = func(n *Node, depth int) {
		s.Nodes++
		totalDepth += depth
		if len(n.docs) > 0 {
			s.LeafNodes++
		}
		if depth > s.MaxDepth {
			s.MaxDepth = depth
		}
		s.TotalDocs += len(n.docs)

		filledChildren := 0
		for _, c := range n.children {
			if c != nil {
				filledChildren++
			}
		}
		levelChildrenSum[depth] += filledChildren
		levelNodeCount[depth]++
		s.TotalChildren += filledChildren

		for _, c := range n.children {
			if c != nil {
				dfs(c, depth+1)
			}
		}
	}

	dfs(t.root, 0)
	s.AvgDepth = float64(totalDepth) / float64(s.Nodes)

	// Average not nil children count per level (for first 3 levels)
	for depth := 0; depth <= 3; depth++ {
		if levelNodeCount[depth] > 0 {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel,
				float64(levelChildrenSum[depth])/float64(levelNodeCount[depth]))
		} else {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel, 0)
		}
	}

	return s
}
