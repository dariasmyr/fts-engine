package hamtrie

import (
	"fts-hw/internal/services/fts"
	"fts-hw/internal/utils"
	"hash/fnv"
	"math/bits"
	"sync"
)

const (
	quant     = 5 // bits stored per node
	lowerbits = uint32(1<<quant) - 1
	depth     = 7 // 32 bits of hash / 5 bits per node = 6.2 => 7 nodes constant trie depth
)

// Node is an internal HAMT node that stores direction
type Node struct {
	bitmap   uint32 // bitmap to indicate occupied slots i.e. occupied slot 5 = 0b00000000000000000000000001000000
	children []any  // slice of *Node or *Leaf
}

// TerminalNode if a node on the last level with actual key and documents
type TerminalNode struct {
	hash    uint32
	entries []entry
}

type entry struct {
	key  string
	docs []fts.Document
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

func (n *Node) nextNode(hash uint32, level int) (child any, pos int, mask uint32) {
	// Example:
	// hash (32 bits):
	// 00000001 01101100 10101010 01011010
	//                                 ^^^^^ level 0
	//                           ^^^^^       level 1
	//                     ^^^^^             level 2
	//
	// For level = 2:
	// shift right by (2 * 5) = 10 bits:
	//
	// hash >> 10 = 00000000 00000001 01101100 10101010
	//
	// mask lower 5 bits:
	// 01010 (decimal 10)
	//
	// idx = 10
	idx := int((hash >> (level * quant)) & lowerbits)

	// convert index into a bitmask with a single 1 at position idx
	// mask = 0b00000000000000000010000000000000
	mask = 1 << idx

	// returns the index of the child in the compressed children slice
	pos = bits.OnesCount32(n.bitmap & (mask - 1))

	// slot empty (free)
	if n.bitmap&mask == 0 {
		return nil, pos, mask
	}

	return n.children[pos], pos, mask
}

func (n *Node) append(newChild any, mask uint32, pos int) {
	// mark the slot as used by AND operation
	// bitmap   = 00100000
	// mask     = 00001000
	// n.bitmap = 00101000
	n.bitmap |= mask
	// insert new leaf node inside children to a special position
	n.children = append(n.children[:pos], append([]any{newChild}, n.children[pos:]...)...)
}

func (n *Node) insertNode(hash uint32, key, docID string, level int) *Node {
	child, pos, mask := n.nextNode(hash, level)

	//last level
	if level == depth {
		// new word added - create new terminal node
		if child == nil {
			tn := &TerminalNode{
				hash: hash,
				entries: []entry{
					{
						key:  key,
						docs: []fts.Document{{docID, 1}},
					},
				},
			}

			n.append(tn, mask, pos)
			return n
		}

		// terminal node found - check entries
		t := child.(*TerminalNode)
		for i := range t.entries {
			// entry with same word found - increase doc count
			if key == t.entries[i].key {
				addDoc(&t.entries[i].docs, docID)
				return n
			}
		}

		// collision - same hash but words differ - create new entry
		t.entries = append(t.entries, entry{
			key:  key,
			docs: []fts.Document{{docID, 1}},
		})
		return n
	}

	// not last level and no child found - create new node
	if child == nil {
		newChild := newNode()
		n.append(newChild, mask, pos)
		child = newChild
	}

	// traverse to next levels
	child.(*Node).insertNode(hash, key, docID, level+1)
	return n
}

func addDoc(docs *[]fts.Document, docID string) {
	for i := range *docs {
		if (*docs)[i].ID == docID {
			(*docs)[i].Count++
			return
		}
	}

	*docs = append(*docs, fts.Document{
		ID:    docID,
		Count: 1,
	})
}

func (t *Trie) Insert(word string, docID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	hash := hashKey(word)
	t.root.insertNode(hash, word, docID, 0)
	return nil
}

func (t *Trie) Search(word string) ([]fts.Document, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	hash := hashKey(word)
	node := t.root

	for level := 0; level <= depth; level++ {

		child, _, _ := node.nextNode(hash, level)
		if child == nil {
			return nil, nil
		}

		if level == depth {
			t := child.(*TerminalNode)
			for i := range t.entries {
				if word == t.entries[i].key {
					return t.entries[i].docs, nil
				}
			}
			return nil, nil
		}

		node = child.(*Node)
	}

	return nil, nil
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
		case *TerminalNode:
			s.Leaves++
			for i := range node.entries {
				s.TotalDocs += len(node.entries[i].docs)
			}
		}

	}

	dfs(t.root, 0)

	if s.Nodes > 0 {
		s.AvgDepth = float64(totalDepth) / float64(s.Nodes)
	}

	// Average not nil children count per level (for first 3 levels)
	for d := 0; d <= depth; d++ {
		if levelNodeCount[d] > 0 {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel,
				float64(levelChildrenSum[d])/float64(levelNodeCount[d]))
		} else {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel, 0)
		}
	}

	return s
}
