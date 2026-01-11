package hamtrie

import (
	"fts-hw/internal/utils"
	"hash/fnv"
	"math/bits"
	"sync"
)

const (
	hashBits  = 32
	shiftBits = 5
	maxLevel  = hashBits / shiftBits
)

type DocEntry struct {
	docID string
	count uint16
}

type Entry struct {
	key  string
	docs []DocEntry
}

type Node struct {
	bitmap   uint32
	children []any
}

type Leaf struct {
	hash uint32
	key  string
	docs []DocEntry
}

type Trie struct {
	root *Node
	mu   sync.RWMutex
}

func newNode() *Node {
	return &Node{
		bitmap:   0,
		children: make([]any, 0),
	}
}

func NewTrie() *Trie {
	return &Trie{
		root: &Node{
			bitmap:   0,
			children: make([]any, 0),
		},
	}
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func bitpos(hash uint32, level int) (idx int, mask uint32) {
	idx = int((hash >> (level * shiftBits)) & 0b11111)
	mask = 1 << idx
	return
}

func childIndex(bitmap uint32, mask uint32) int {
	return bits.OnesCount32(bitmap & (mask - 1))
}

func (t *Trie) Insert(word string, docID string) error {

	t.mu.Lock()
	defer t.mu.Unlock()

	hash := hashKey(word)
	t.root = insertNode(t.root, hash, word, docID, 0)
	return nil
}

func insertNode(n *Node, hash uint32, key, docID string, level int) *Node {
	_, mask := bitpos(hash, level)

	pos := childIndex(n.bitmap, mask)

	if n.bitmap&mask == 0 {
		leaf := &Leaf{
			hash: hash,
			key:  key,
			docs: []DocEntry{{
				docID: docID,
				count: 1,
			}},
		}

		n.bitmap |= mask
		n.children = append(n.children, nil)
		copy(n.children[pos+1:], n.children[pos:])
		n.children[pos] = leaf
		return n
	}

	child := n.children[pos]

	switch c := child.(type) {
	case *Leaf:
		if c.key == key {
			addDoc(&c.docs, docID)
			return n
		}

		n.children[pos] = mergeLeaves(c, hash, key, docID, level+1)
		return n
	case *Node:
		n.children[pos] = insertNode(c, hash, key, docID, level+1)
		return n
	}

	return n
}

func mergeLeaves(existing *Leaf, hash uint32, key, docID string, level int) *Node {
	node := newNode()

	if level >= maxLevel {
		node.children = []any{
			existing,
			&Leaf{
				hash: hash,
				key:  key,
				docs: []DocEntry{{docID, 1}},
			},
		}
		node.bitmap = 0b11
		return node
	}

	idx1, mask1 := bitpos(existing.hash, level)
	idx2, mask2 := bitpos(hash, level)

	if idx1 != idx2 {
		node.bitmap = mask1 | mask2
		if idx1 < idx2 {
			node.children = []any{
				existing,
				&Leaf{
					hash: hash,
					key:  key,
					docs: []DocEntry{
						{docID, 1},
					},
				}}
		} else {
			node.children = []any{
				&Leaf{
					hash: hash,
					key:  key,
					docs: []DocEntry{
						{docID, 1},
					},
				},
				existing,
			}
		}
		return node
	}

	node.bitmap = mask1
	child := mergeLeaves(existing, hash, key, docID, level+1)
	node.children = []any{child}
	return node
}

func addDoc(docs *[]DocEntry, docID string) {
	for i := range *docs {
		if (*docs)[i].docID == docID {
			(*docs)[i].count++
			return
		}
	}

	*docs = append(*docs, DocEntry{
		docID: docID,
		count: 1,
	})
}

func (t *Trie) Search(word string) (map[string]int, error) {

	t.mu.RLock()
	defer t.mu.RUnlock()

	hash := hashKey(word)
	node := t.root

	for level := 0; level <= maxLevel; level++ {
		_, mask := bitpos(hash, level)
		if node.bitmap&mask == 0 {
			return nil, nil
		}

		pos := childIndex(node.bitmap, mask)
		child := node.children[pos]

		switch c := child.(type) {
		case *Leaf:
			if c.key != word {
				return nil, nil
			}
			return collectDocs(c.docs), nil
		case *Node:
			node = c
		}
	}

	return nil, nil
}

func collectDocs(docs []DocEntry) map[string]int {
	result := make(map[string]int, len(docs))

	for _, d := range docs {
		result[d.docID] = int(d.count)
	}
	return result
}

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}

func (t *Trie) Analyze() utils.TrieStats {
	var s utils.TrieStats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var dfs func(n any, depth int)
	dfs = func(n any, depth int) {
		s.Nodes++
		totalDepth += depth

		if depth > s.MaxDepth {
			s.MaxDepth = depth
		}

		switch node := n.(type) {
		case *Node:
			s.TotalChildren += len(node.children)
			levelChildrenSum[depth] += len(node.children)
			levelNodeCount[depth]++

			for _, c := range node.children {
				dfs(c, depth+1)
			}
		case *Leaf:
			s.LeafNodes++
			s.TotalDocs += len(node.docs)
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
