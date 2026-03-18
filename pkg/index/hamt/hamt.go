package hamt

import (
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"hash/fnv"
	"math/bits"
	"slices"
	"sort"
	"sync"
)

const (
	quant     = 5
	lowerbits = uint32(1<<quant) - 1
	depth     = 7
)

type documents []fts.DocRef

func (d documents) Add(id fts.DocID) documents {
	i := sort.Search(len(d), func(i int) bool { return d[i].ID >= id })
	if i < len(d) && d[i].ID == id {
		d[i].Count++
		return d
	}
	d = append(d, fts.DocRef{})
	copy(d[i+1:], d[i:])
	d[i] = fts.DocRef{ID: id, Count: 1}
	return d
}

type entry struct {
	key  string
	docs documents
}

type nodeptr = uint32

type terminal struct {
	entries []entry
}

func (t *terminal) Append(word string, id fts.DocID) {
	i := sort.Search(len(t.entries), func(i int) bool { return t.entries[i].key >= word })
	if i < len(t.entries) && t.entries[i].key == word {
		t.entries[i].docs = t.entries[i].docs.Add(id)
		return
	}
	t.entries = append(t.entries, entry{})
	copy(t.entries[i+1:], t.entries[i:])
	t.entries[i] = entry{key: word, docs: documents{{ID: id, Count: 1}}}
}

func (t *terminal) Find(word string) documents {
	i := sort.Search(len(t.entries), func(i int) bool { return t.entries[i].key >= word })
	if i < len(t.entries) && t.entries[i].key == word {
		return t.entries[i].docs
	}
	return nil
}

type node struct {
	bitmap   uint32
	children []nodeptr
}

func (n node) Append(idx uint32, branch nodeptr) node {
	mask := uint32(1) << idx
	n.bitmap |= mask
	index := bits.OnesCount32(n.bitmap & (mask - 1))
	n.children = slices.Insert(n.children, index, branch)
	return n
}

type Index struct {
	mu    sync.RWMutex
	nodes []node
	terms []terminal
}

func New() *Index {
	return &Index{nodes: make([]node, 1)}
}

func (t *Index) Search(key string) ([]fts.DocRef, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	n := nodeptr(0)
	hash := strhash32(key)
	for range depth - 1 {
		var ok bool
		n, ok = t.nextNode(n, hash)
		if !ok {
			return nil, nil
		}
		hash >>= quant
	}

	term := t.terms[n]
	if term.entries == nil {
		return nil, nil
	}

	docs := term.Find(key)
	if docs == nil {
		return nil, nil
	}

	return docs, nil
}

func (t *Index) Insert(word string, id fts.DocID) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	hash := strhash32(word)
	n := nodeptr(0)
	for range depth - 2 {
		var ok bool
		n, ok = t.nextNode(n, hash)
		if !ok {
			newNode := t.newNode()
			t.nodes[n] = t.nodes[n].Append(hash&lowerbits, newNode)
			n = newNode
		}
		hash >>= quant
	}

	termPtr, ok := t.nextNode(n, hash)
	if !ok {
		termPtr = t.newTerm()
		t.nodes[n] = t.nodes[n].Append(hash&lowerbits, termPtr)
	}

	t.terms[termPtr].Append(word, id)
	return nil
}

func (t *Index) newNode() nodeptr {
	t.nodes = append(t.nodes, node{})
	return nodeptr(len(t.nodes) - 1)
}

func (t *Index) newTerm() nodeptr {
	t.terms = append(t.terms, terminal{})
	return nodeptr(len(t.terms) - 1)
}

func (t *Index) nextNode(n nodeptr, hash uint32) (nodeptr, bool) {
	mask := uint32(1) << (hash & lowerbits)
	node := t.nodes[n]
	if node.bitmap&mask == 0 {
		return n, false
	}
	index := bits.OnesCount32(node.bitmap & (mask - 1))
	return node.children[index], true
}

func strhash32(str string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(str))
	return h.Sum32()
}

func (t *Index) Analyze() fts.Stats {
	var s fts.Stats
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
			for _, e := range term.entries {
				s.TotalDocs += len(e.docs)
			}
			return
		}

		if int(ptr) >= len(t.nodes) {
			return
		}
		n := t.nodes[ptr]
		s.Nodes++
		totalDepth += currentDepth
		if currentDepth > s.MaxDepth {
			s.MaxDepth = currentDepth
		}

		childCount := len(n.children)
		s.TotalChildren += childCount
		levelChildrenSum[currentDepth] += childCount
		levelNodeCount[currentDepth]++

		for _, c := range n.children {
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

var _ fts.Index = (*Index)(nil)
