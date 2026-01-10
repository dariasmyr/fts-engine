package radixhashtrie

import (
	"fts-hw/internal/utils"
	"sync"
)

type DocEntry struct {
	docID string
	count uint16
}

type Node struct {
	prefix   string
	terminal bool
	docs     []DocEntry
	children []int
}

func (t *Trie) newNode(prefix string) int {
	t.nodes = append(t.nodes, Node{
		prefix:   prefix,
		docs:     make([]DocEntry, 0),
		children: make([]int, 0),
	})

	return len(t.nodes) - 1
}

type Trie struct {
	root  int
	nodes []Node
	mu    sync.RWMutex
}

func NewTrie() *Trie {
	var t Trie
	t.nodes = make([]Node, 0)
	t.root = t.newNode("")
	return &t
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

	var newNodeIdx int

	for {
		for i, child := range t.nodes[current].children {
			p := lcp(rest, t.nodes[child].prefix)

			if p == 0 {
				continue
			}

			// prefix fully matched with child - go deeper
			if p == len(t.nodes[child].prefix) {
				current = child
				rest = rest[p:]

				if rest == "" {
					t.nodes[current].terminal = true
					t.addDoc(current, docID)
					return nil
				}

				goto NEXT
			}

			// split
			common := t.nodes[child].prefix[:p]
			childSuffix := t.nodes[child].prefix[p:]
			newSuffix := rest[p:]

			middle := t.newNode(common)

			// shorten old child prefix
			t.nodes[child].prefix = childSuffix

			// relink old node
			t.nodes[middle].children = append(t.nodes[middle].children, child)

			// replace child with middle node (with common suffix)
			t.nodes[current].children[i] = middle

			// if rest is not empty, create new node and mark it as end for new word
			if newSuffix != "" {
				newNodeIdx = t.newNode(newSuffix)
				t.nodes[newNodeIdx].terminal = true
				t.addDoc(newNodeIdx, docID)
				t.nodes[middle].children = append(t.nodes[middle].children, newNodeIdx)
				return nil
			}

			// rest is empty, mark middle common node as end for new word
			t.nodes[middle].terminal = true
			t.addDoc(middle, docID)
			return nil
		}

		//if no child fitted new word by prefix - just add new node
		newNodeIdx = t.newNode(rest)
		t.nodes[newNodeIdx].terminal = true
		t.addDoc(newNodeIdx, docID)
		t.nodes[current].children = append(t.nodes[current].children, newNodeIdx)
		return nil

	NEXT:
	}
}

func (t *Trie) addDoc(nodeIdx int, docID string) {
	node := &t.nodes[nodeIdx]

	for i := range node.docs {
		if node.docs[i].docID == docID {
			node.docs[i].count++
			return
		}
	}

	node.docs = append(node.docs, DocEntry{
		docID: docID,
		count: 1,
	})
}

func (t *Trie) collectDocs(nodeIdx int) map[string]int {
	node := t.nodes[nodeIdx]
	result := make(map[string]int, len(node.docs))

	for _, d := range node.docs {
		result[d.docID] = int(d.count)
	}
	return result
}

func (t *Trie) Search(word string) (map[string]int, error) {

	t.mu.RLock()
	defer t.mu.RUnlock()

	currentIdx := t.root
	rest := word

	for {
		nextNodeIdx, nextRest, matched, exact := t.next(currentIdx, rest)

		if nextNodeIdx == 0 {
			return nil, nil
		}

		if !matched {
			return nil, nil
		}

		if exact {
			return t.collectDocs(nextNodeIdx), nil
		}

		currentIdx = nextNodeIdx
		rest = nextRest
	}
}

// next tries to advance from current node using rest of the word.
// Returns:
//
//	nextNode  - child node to continue from
//	nextRest  - remaining part of the word after consuming prefix
//	matched   - whether ANY progress was made
//	exact     - whether the word fully matched on this node boundary
func (t *Trie) next(current int, rest string) (int, string, bool, bool) {
	for _, child := range t.nodes[current].children {
		p := lcp(rest, t.nodes[child].prefix)

		// case 0:
		// no common prefix at all - try next child
		if p == 0 {
			continue
		}

		// case 1:
		// rest fully consumed
		if p == len(rest) {
			// exact match only if node is terminal
			if t.nodes[child].terminal {
				return child, "", true, true
			}
			// query word matched only a prefix of a longer word in a tree - so it's not found
			return 0, "", false, false
		}

		// case 2:
		// child prefix fully matched, go deeper
		if p == len(t.nodes[child].prefix) {
			return child, rest[p:], true, false
		}

		// case 3:
		// partial overlap:
		// - the word does not exist in the trie
		return 0, "", false, false
	}

	return 0, "", false, false
}

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}

func (t *Trie) Analyze() utils.TrieStats {
	var s utils.TrieStats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var dfs func(n int, depth int)
	dfs = func(n int, depth int) {
		s.Nodes++
		totalDepth += depth

		if t.nodes[n].terminal {
			s.LeafNodes++
		}
		if depth > s.MaxDepth {
			s.MaxDepth = depth
		}
		s.TotalDocs += len(t.nodes[n].docs)

		numChildren := len(t.nodes[n].children)
		s.TotalChildren += numChildren
		levelChildrenSum[depth] += numChildren
		levelNodeCount[depth]++

		for _, c := range t.nodes[n].children {
			dfs(c, depth+1)
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
