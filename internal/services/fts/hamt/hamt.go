package hamtrie

import (
	"fts-hw/internal/services/fts"
	"fts-hw/internal/utils"
	"hash/fnv"
	"math/bits"
	"slices"
	"sort"
	"sync"
)

const (
	quant     = 5 // bits stored per node
	lowerbits = uint32(1<<quant) - 1
	depth     = 7 // 32 bits of hash / 5 bits per node = 6.2 => 7 nodes constant trie depth
)

type Documents []fts.Document

// Add uses binary search across the docs and insert it in the right place
func (d Documents) Add(id string) Documents {
	i := sort.Search(len(d), func(i int) bool { return d[i].ID >= id })

	if i < len(d) && d[i].ID == id {
		d[i].Count++
		return d
	}

	newDoc := fts.Document{ID: id, Count: 1}
	d = append(d, fts.Document{})
	copy(d[i+1:], d[i:])
	d[i] = newDoc
	return d
}

// entry serves collision resolution. Its key is compared against the actual given key
type entry struct {
	key  string
	docs Documents
}

type nodeptr = uint32

// Terminal is the 7th trie level. Terminal nodes store actual values instead of children
type Terminal struct {
	entries []entry
}

// Append appends a new value to the array with binary search
func (t *Terminal) Append(word, id string) {
	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].key >= word
	})

	if i < len(t.entries) && t.entries[i].key == word {
		// word found — look for doc with same ID
		t.entries[i].docs = t.entries[i].docs.Add(id)
		return
	}

	// add new entry
	newEntry := entry{
		key:  word,
		docs: Documents{fts.Document{ID: id, Count: 1}},
	}

	// add and save order
	t.entries = append(t.entries, entry{})
	copy(t.entries[i+1:], t.entries[i:])
	t.entries[i] = newEntry
}

func (t *Terminal) Find(word string) Documents {
	i := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].key >= word
	})
	if i < len(t.entries) && t.entries[i].key == word {
		return t.entries[i].docs
	}
	return nil
}

// Node is the base HAMT node for the first 6 trie levels
type Node struct {
	bitmap   uint32    // bitmap to indicate occupied slots i.e. occupied slot 5 = 0b00000000000000000000000001000000
	children []nodeptr // slice of *Node or *Terminal
}

// Append appends the new element to the dense array preserving the correct order
func (n Node) Append(idx uint32, branch nodeptr) Node {
	mask := uint32(1) << idx
	n.bitmap |= mask
	index := bits.OnesCount32(n.bitmap & (mask - 1))
	n.children = slices.Insert(n.children, index, branch)
	return n
}

type Trie struct {
	mu    sync.RWMutex
	nodes []Node
	terms []Terminal
}

func New() *Trie {
	return &Trie{
		nodes: make([]Node, 1),
	}
}

func (t *Trie) Search(key string) ([]fts.Document, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	node := nodeptr(0)
	hash := strhash32(key)

	for range depth - 1 {
		var ok bool
		node, ok = t.nextNode(node, hash)
		if !ok {
			return nil, nil
		}

		hash >>= quant
	}

	term := t.terms[node]
	if term.entries == nil {
		return nil, nil
	}

	docs := term.Find(key)
	if docs == nil {
		return nil, nil
	}

	return docs, nil
}

func (t *Trie) Insert(word, id string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	hash := strhash32(word)
	node := nodeptr(0)

	for range depth - 2 {
		var ok bool
		node, ok = t.nextNode(node, hash)
		if !ok {
			// just create new nodes as we go
			newNode := t.newNode()
			t.nodes[node] = t.nodes[node].Append(hash&lowerbits, newNode)
			node = newNode
		}

		hash >>= quant
	}

	// the sixth node always points at a terminal node. Not having one isn't bad.
	termptr, ok := t.nextNode(node, hash)
	if !ok {
		termptr = t.newTerm()
		t.nodes[node] = t.nodes[node].Append(hash&lowerbits, termptr)
	}

	// hash contains 7 bits at this point. First 5 of them are already used. The last 2
	// are the object of interest, therefore erase the first 5 by shifting them away
	t.terms[termptr].Append(word, id)
	return nil
}

func (t *Trie) newNode() nodeptr {
	// spawn a new node by appending one. Its "array pointer" is the slice's length itself therefore.
	t.nodes = append(t.nodes, Node{})
	return nodeptr(len(t.nodes)) - 1
}

func (t *Trie) newTerm() nodeptr {
	// similar to newNode.
	t.terms = append(t.terms, Terminal{})
	return nodeptr(len(t.terms)) - 1
}

// nextNode returns an array pointer to the next node. If no child is found, the initial n is returned
// in order to not lose information (usually the node pointer is overwritten immediately at a callsite)
func (t *Trie) nextNode(n nodeptr, hash uint32) (nodeptr, bool) {
	// move 1 by 5 lower bits of hash (shift by 0-31 bits)
	mask := uint32(1) << (hash & lowerbits)
	node := t.nodes[n]
	// if at the desired position the bit is zero, such 5 bits of hash
	// aren't in the trie
	if node.bitmap&mask == 0 {
		return n, false
	}

	// as t.children is a dense array, we must count the number of non-nil members
	// in it first. Each non-nil member has its corresponding bit on, therefore count
	// all leading 1s (mask-1 produces 5 lower bits turned on)
	index := bits.OnesCount32(node.bitmap & (mask - 1))
	return node.children[index], true
}

func strhash32(str string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(str)) // to be replaced with unsafe S2B method
	return h.Sum32()
}

func (t *Trie) Analyze() utils.TrieStats {
	var s utils.TrieStats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var dfs func(ptr nodeptr, currentDepth int, isTerm bool)
	dfs = func(ptr nodeptr, currentDepth int, isTerm bool) {
		if isTerm {
			if int(ptr) >= len(t.terms) {
				return
			}
			term := t.terms[ptr]
			s.Leaves++
			// считаем все документы в терминале
			for _, e := range term.entries {
				s.TotalDocs += len(e.docs)
			}
			return
		}

		if int(ptr) >= len(t.nodes) {
			return
		}
		node := t.nodes[ptr]

		s.Nodes++
		totalDepth += currentDepth
		if currentDepth > s.MaxDepth {
			s.MaxDepth = currentDepth
		}

		childCount := len(node.children)
		s.TotalChildren += childCount
		levelChildrenSum[currentDepth] += childCount
		levelNodeCount[currentDepth]++

		for _, c := range node.children {
			if currentDepth == depth-2 {
				dfs(c, currentDepth+1, true)
			} else {
				dfs(c, currentDepth+1, false)
			}
		}
	}

	dfs(0, 0, false)

	if s.Nodes > 0 {
		s.AvgDepth = float64(totalDepth) / float64(s.Nodes)
	}

	for d := 1; d <= depth; d++ {
		if levelNodeCount[d] > 0 {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel,
				float64(levelChildrenSum[d])/float64(levelNodeCount[d]))
		} else {
			s.AvgChildrenPerLevel = append(s.AvgChildrenPerLevel, 0)
		}
	}

	return s
}
