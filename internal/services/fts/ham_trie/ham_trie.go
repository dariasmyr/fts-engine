package hamtrie

import (
	"fts-hw/internal/utils"
	"hash/fnv"
	"math/bits"
	"sync"
)

const (
	shiftBits = 5
	maxLevel  = 7 // 32-hash/5 = 6.2 => 7 nodes deep
)

type Document struct {
	docID string
	count uint16
}

// Terminal if a node with actual key and documents
type Terminal struct {
	bitmap  uint8   // bitmap to indicate occupied 2 last bits
	entries []Entry // array of documents
}

type Entry struct {
	key  string
	docs []Document
}

// Node is an internal HAMT node that stores direction
type Node struct {
	bitmap   uint32 // bitmap to indicate occupied slots i.e. occupied slot 5 = 0b00000000000000000000000001000000
	children []any  // slice of *Node or *Terminal
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

func nextNode(n *Node, hash uint32, level int) (child any, pos int, mask uint32) {
	// shift hash right by (level * shiftBits) bits
	// hash (32 bits):    00000001 01101100 10101010 01011010
	// hash >> (2 * 5) =  00000000 00010110 11001010 10100101
	// then mask the shifted 5 bits using & 0b11111 AND operation to get a value from 0 to 31
	// 0b01101 & 0b11111= 00000000 00000000 00000000 00001101
	// idx of 0b01101 = 13
	idx := int((hash >> (level * shiftBits)) & 0b11111)

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

func lookupTerminalEntry(n *Terminal, hash uint32) (entry *Entry, pos int, mask uint8) {
	// get first 2 bits
	idx := int(hash & 0b11)

	// convert index into a bitmask with a single 1 at position idx
	mask = uint8(1 << idx)

	pos = bits.OnesCount8(n.bitmap & (mask - 1))

	// slot empty (free)
	if n.bitmap&mask == 0 {
		return nil, pos, mask
	}

	return &n.entries[pos], pos, mask
}

func insertIntoTerminal(t *Terminal, hash uint32, key, docID string) {
	entry, pos, mask := lookupTerminalEntry(t, hash)

	// slot is free
	if entry == nil {
		t.bitmap |= mask
		t.entries = append(
			t.entries[:pos],
			append(
				[]Entry{{
					key:  key,
					docs: []Document{{docID, 1}}},
				}, t.entries[pos:]...)...,
		)
		return
	}

	//slot is occupied - check key
	if entry.key == key {
		addDoc(&entry.docs, docID)
	}

	// fallback: 2 same bits, keys differ
	for i := range t.entries {
		if t.entries[i].key == key {
			addDoc(&t.entries[i].docs, docID)
			return
		}
	}

	t.entries = append(t.entries, Entry{
		key:  key,
		docs: []Document{{docID, 1}},
	})
}

func (t *Trie) Insert(word string, docID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	hash := hashKey(word)
	t.root = insertNode(t.root, hash, word, docID, 0)
	return nil
}

func insertNode(n *Node, hash uint32, key, docID string, level int) *Node {
	child, pos, mask := nextNode(n, hash, level)

	if level == maxLevel {
		var term *Terminal

		if child == nil {
			term = &Terminal{}
			n.bitmap |= mask
			n.children = append(
				n.children[:pos],
				append([]any{term}, n.children[pos:]...)...,
			)
		} else {
			term = child.(*Terminal)
		}

		insertIntoTerminal(term, hash, key, docID)
		return n
	}

	// slot empty (free) - create a leaf to store a word
	if child == nil {
		newChild := newNode()
		// mark the slot as used by AND operation
		// bitmap   = 00100000
		// mask     = 00001000
		// n.bitmap = 00101000
		n.bitmap |= mask
		// insert new leaf node inside children to a special position
		n.children = append(
			n.children[:pos],
			append([]any{newChild}, n.children[pos:]...)...,
		)
		n.children[pos] = insertNode(newChild, hash, key, docID, level+1)
		return n
	}

	// already have Node, go deeper
	n.children[pos] = insertNode(child.(*Node), hash, key, docID, level+1)
	return n
}

func addDoc(docs *[]Document, docID string) {
	for i := range *docs {
		if (*docs)[i].docID == docID {
			(*docs)[i].count++
			return
		}
	}

	*docs = append(*docs, Document{
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

		child, _, _ := nextNode(node, hash, level)

		if child == nil {
			return nil, nil
		}

		if level == maxLevel {
			term := child.(*Terminal)
			return searchTerminal(term, hash, word), nil
		}

		node = child.(*Node)
	}

	return nil, nil
}

func searchTerminal(t *Terminal, hash uint32, key string) map[string]int {
	entry, _, _ := lookupTerminalEntry(t, hash)

	if entry != nil {
		if entry.key == key {
			return collectDocs(entry.docs)
		}
	}

	return nil
}

func collectDocs(docs []Document) map[string]int {
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
		case *Terminal:
			s.LeafNodes++
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
	for depth := 0; depth <= 8; depth++ {
		if levelNodeCount[depth] > 0 {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel,
				float64(levelChildrenSum[depth])/float64(levelNodeCount[depth]))
		} else {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel, 0)
		}
	}

	return s
}
