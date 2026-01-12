package radixtrie

import (
	"fts-hw/internal/services/fts"
	"fts-hw/internal/utils"
	"sync"
)

type Node struct {
	terminal bool
	prefix   string
	children []*Node
	docs     map[string]int
}

func newNode(prefix string) *Node {
	return &Node{
		prefix: prefix,
		docs:   make(map[string]int),
	}
}

type Trie struct {
	root *Node
	mu   sync.RWMutex
}

func New() *Trie {
	return &Trie{
		root: newNode(""),
	}
}

// longest common prefix
func lcp(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func (t *Trie) Insert(word string, docID string) error {

	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.root
	rest := word

	var node *Node

	for {
		for i, child := range current.children {
			p := lcp(rest, child.prefix)

			if p == 0 {
				continue
			}

			// prefix fully matched with child - go deeper
			if p == len(child.prefix) {
				current = child
				rest = rest[p:]

				if rest == "" {
					current.terminal = true
					current.docs[docID]++
					return nil
				}

				goto NEXT
			}

			// split
			common := child.prefix[:p]
			childSuffix := child.prefix[p:]
			newSuffix := rest[p:]

			middle := newNode(common)

			// shorten old child prefix
			child.prefix = childSuffix

			// relink old node
			middle.children = append(middle.children, child)

			// replace child with middle node (with common suffix)
			current.children[i] = middle

			// if rest is not empty, create new node and mark it as end for new word
			if newSuffix != "" {
				node = newNode(newSuffix)
				node.terminal = true
				node.docs[docID]++
				middle.children = append(middle.children, node)
				return nil
			}

			// rest is empty, mark middle common node as end for new word
			middle.terminal = true
			middle.docs[docID]++
			return nil
		}

		//if no child fitted new word by prefix - just add new node
		node = newNode(rest)
		node.terminal = true
		node.docs[docID]++
		current.children = append(current.children, node)
		return nil

	NEXT:
	}
}

func (t *Trie) Search(word string) ([]fts.Document, error) {

	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.root
	rest := word

	for {
		nextNode, nextRest, matched, exact := t.next(current, rest)

		if !matched {
			return nil, nil
		}

		if exact {
			return collectDocs(nextNode.docs), nil
		}

		current = nextNode
		rest = nextRest
	}
}

func collectDocs(docs map[string]int) []fts.Document {
	s := make([]fts.Document, 0, len(docs))
	for id, count := range docs {
		s = append(s, fts.Document{
			ID:    id,
			Count: count,
		})
	}

	return s
}

// next tries to advance from current node using rest of the word.
// Returns:
//
//	nextNode  - child node to continue from
//	nextRest  - remaining part of the word after consuming prefix
//	matched   - whether ANY progress was made
//	exact     - whether the word fully matched on this node boundary
func (t *Trie) next(current *Node, rest string) (*Node, string, bool, bool) {
	for _, child := range current.children {
		p := lcp(rest, child.prefix)

		// case 0:
		// no common prefix at all - try next child
		if p == 0 {
			continue
		}

		// case 1:
		// rest fully consumed
		if p == len(rest) {
			// exact match only if node is terminal
			if child.terminal {
				return child, "", true, true
			}
			// query word matched only a prefix of a longer word in a tree - so it's not found
			return nil, "", false, false
		}

		// case 2:
		// child prefix fully matched, go deeper
		if p == len(child.prefix) {
			return child, rest[p:], true, false
		}

		// case 3:
		// partial overlap:
		// - the word does not exist in the trie
		return nil, "", false, false
	}

	return nil, "", false, false
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

		if n.terminal {
			s.Leaves++
		}
		if depth > s.MaxDepth {
			s.MaxDepth = depth
		}
		s.TotalDocs += len(n.docs)

		numChildren := len(n.children)
		s.TotalChildren += numChildren
		levelChildrenSum[depth] += numChildren
		levelNodeCount[depth]++

		for _, c := range n.children {
			if c != nil {
				dfs(c, depth+1)
			}
		}
	}

	dfs(t.root, 0)

	if s.Nodes > 0 {
		s.AvgDepth = float64(totalDepth) / float64(s.Nodes)
	}

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
