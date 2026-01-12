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

func (d Documents) Add(id string) Documents {

	i := sort.Search(len(d), func(i int) bool { return d[i].ID >= id })

	for i < len(d) && d[i].ID == id {
		d[i].Count++
		return d
	}

	newDoc := fts.Document{
		ID:    id,
		Count: 1,
	}

	if cap(d) > len(d) {
		d = append(d, fts.Document{})
		copy(d[i+1:], d[i:])
		d[i] = newDoc
	} else {
		newCap := 2*len(d) + 1
		newSlice := make(Documents, len(d)+1, newCap)
		copy(newSlice[:i], d[:i])
		newSlice[i] = newDoc
		copy(newSlice[i+1:], d[i:])
		d = newSlice
	}

	return d
}

// entry serves collision resolution. Its key is compared against the actual given key
type entry struct {
	key  string
	docs Documents
}

type bucket []entry

func (b bucket) find(str string) Documents {
	for _, e := range b {
		if e.key == str {
			return e.docs
		}
	}

	return nil
}

func (b bucket) add(word, id string) bucket {
	for i, e := range b {
		if e.key == word {
			b[i].docs = e.docs.Add(id)
		}
	}

	return append(b, entry{
		key:  word,
		docs: Documents(nil).Add(id),
	})
}

type nodeptr = uint64

// Terminal is the 7th trie level. Terminal nodes store actual values instead of children
type Terminal struct {
	bitmap uint8    // bitmap to address 2 last bits (32%5=2) of hash
	bucks  []bucket // array of documents
}

// Append appends a new value to the dense array preserving the correct order
func (t Terminal) Append(idx uint32, word, id string) Terminal {
	mask := uint8(1) << idx
	index := bits.OnesCount8(t.bitmap & (mask - 1))

	if t.bitmap&mask == 0 {
		t.bitmap |= mask
		t.bucks = slices.Insert(t.bucks, index, make(bucket, 0, 1))
	}

	t.bucks[index] = t.bucks[index].add(word, id)
	return t
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
	if term.bitmap&(1<<hash) == 0 {
		return nil, nil
	}

	buck := term.bucks[bits.OnesCount8(term.bitmap&((1<<hash)-1))]
	return buck.find(key), nil
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
	t.terms[termptr] = t.terms[termptr].Append(hash>>quant, word, id)
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

func WordKeys(token string) ([]string, error) {
	return []string{token}, nil
}

func (t *Trie) Analyze() utils.TrieStats {
	var s utils.TrieStats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var traverse func(n nodeptr, level int)
	traverse = func(n nodeptr, level int) {
		if level >= 7 {
			// terminal node
			for _, buck := range t.terms[n].bucks {
				s.TotalDocs += len(buck)
			}

			return
		}

		node := t.nodes[n]
		s.TotalChildren += len(node.children)
		levelChildrenSum[level] += len(node.children)
		levelNodeCount[level]++

		for _, child := range node.children {
			traverse(child, level+1)
		}
	}

	s.Nodes = len(t.nodes) + len(t.terms)
	s.Leaves = len(t.terms)
	s.MaxDepth = depth
	traverse(0, 0)

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
