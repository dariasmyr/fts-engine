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

// Node is an internal HAMT node that stores direction
type Node struct {
	bitmap   uint32 // bitmap to indicate occupied slots i.e. occupied slot 5 = 0b00000000000000000000000001000000
	children []any  // slice of *Node or *Leaf
}

// Leaf if a terminal node with actual key and documents
type Leaf struct {
	hash uint32
	key  string
	docs []DocEntry
}

// CollisionNode stores different keys with identical hash
type CollisionNode struct {
	hash   uint32
	leaves []*Leaf
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
		root: newNode(),
	}
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func bitpos(hash uint32, level int) (idx int, mask uint32) {
	// shift hash right by (level * shiftBits) bits
	// hash (32 bits):    00000001 01101100 10101010 01011010
	// hash >> (2 * 5) =  00000000 00010110 11001010 10100101
	// then mask the shifted 5 bits using & 0b11111 AND operation to get a value from 0 to 31
	// 0b01101 & 0b11111= 00000000 00000000 00000000 00001101
	// idx of 0b01101 = 13
	idx = int((hash >> (level * shiftBits)) & 0b11111)
	// convert index into a bitmask with a single 1 at position idx
	// mask = 0b00000000000000000010000000000000
	mask = 1 << idx
	return
}

// childIndex returns the index of the child in the compressed children slice
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

	// slot empty (free) - create a leaf to store a word
	if n.bitmap&mask == 0 {
		leaf := &Leaf{
			hash: hash,
			key:  key,
			docs: []DocEntry{{
				docID,
				1,
			}},
		}

		// mark the slot as used by AND operation
		// bitmap   = 00100000
		// mask     = 00001000
		// n.bitmap = 00101000
		n.bitmap |= mask
		// insert new leaf node inside children to a special position
		n.children = append(n.children[:pos], append([]any{leaf}, n.children[pos:]...)...)
		return n
	}

	child := n.children[pos]

	switch c := child.(type) {
	case *Leaf:
		// found terminal node - check key
		// same keys, increment doc count
		if c.key == key {
			addDoc(&c.docs, docID)
			return n
		}

		// same hash (collision): create collision node
		if c.hash == hash {
			cn := &CollisionNode{
				hash: hash,
				leaves: []*Leaf{
					c,
					{
						hash,
						key,
						[]DocEntry{{docID, 1}},
					},
				},
			}
			n.children[pos] = cn
			return n
		}

		// different hash - splitLeaf
		node := splitLeaf(c, hash, key, docID, level+1)
		n.children[pos] = node
		return n

	case *CollisionNode:
		// same hash quaranteed
		for _, l := range c.leaves {
			if l.key == key {
				addDoc(&l.docs, docID)
				return n
			}
		}

		c.leaves = append(c.leaves, &Leaf{
			hash: hash,
			key:  key,
			docs: []DocEntry{{docID, 1}},
		})
		return n

	case *Node:
		// go deeper into internal node to check
		n.children[pos] = insertNode(c, hash, key, docID, level+1)
		return n
	}

	return n
}

// splitLeaf creates subtree for two leaves with different hashes
func splitLeaf(existing *Leaf, hash uint32, key, docID string, level int) *Node {
	node := newNode()

	for {
		idx1, mask1 := bitpos(existing.hash, level)
		idx2, mask2 := bitpos(hash, level)

		// different slots
		if idx1 != idx2 || level >= maxLevel {
			node.bitmap = mask1 | mask2
			if idx1 < idx2 {
				node.children = []any{
					existing,
					&Leaf{hash, key, []DocEntry{
						{docID, 1},
					},
					}}
			} else {
				node.children = []any{
					&Leaf{hash, key, []DocEntry{
						{docID, 1},
					},
					},
					existing,
				}
			}
			return node
		}

		// two nodes with same slot - create intermediate node and go deeper
		node.bitmap = mask1
		child := newNode()
		node.children = []any{child}
		node = child
		level++
	}
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
