package slicedradix

import (
	"encoding/gob"
	"fmt"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"io"
	"sync"
)

type node struct {
	prefix    string
	children  []int
	docs      []fts.DocRef
	positions [][]uint32
}

type Index struct {
	root     int
	nodes    []node
	docToOrd map[fts.DocID]uint32
	mu       sync.RWMutex
}

type snapshotNode struct {
	Prefix    string
	Children  []int
	Docs      []fts.DocRef
	Positions [][]uint32
}

type snapshotIndex struct {
	Root  int
	Nodes []snapshotNode
}

func New() *Index {
	var t Index
	t.docToOrd = make(map[fts.DocID]uint32)
	t.root = t.newNode("")
	return &t
}

func (t *Index) Serialize(w io.Writer) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	snap := snapshotIndex{
		Root:  t.root,
		Nodes: make([]snapshotNode, 0, len(t.nodes)),
	}

	for i := range t.nodes {
		n := t.nodes[i]
		var positions [][]uint32
		if len(n.positions) > 0 {
			positions = make([][]uint32, len(n.positions))
			for j, p := range n.positions {
				positions[j] = append([]uint32(nil), p...)
			}
		}
		snap.Nodes = append(snap.Nodes, snapshotNode{
			Prefix:    n.prefix,
			Children:  append([]int(nil), n.children...),
			Docs:      append([]fts.DocRef(nil), n.docs...),
			Positions: positions,
		})
	}

	if err := gob.NewEncoder(w).Encode(snap); err != nil {
		return fmt.Errorf("slicedradix: serialize: %w", err)
	}

	return nil
}

func Load(r io.Reader) (fts.Index, error) {
	var snap snapshotIndex
	if err := gob.NewDecoder(r).Decode(&snap); err != nil {
		return nil, fmt.Errorf("slicedradix: load: %w", err)
	}

	idx := &Index{
		root:     snap.Root,
		nodes:    make([]node, 0, len(snap.Nodes)),
		docToOrd: make(map[fts.DocID]uint32),
	}

	for i := range snap.Nodes {
		s := snap.Nodes[i]
		var positions [][]uint32
		if len(s.Positions) > 0 {
			positions = make([][]uint32, len(s.Positions))
			for j, p := range s.Positions {
				positions[j] = append([]uint32(nil), p...)
			}
		}
		idx.nodes = append(idx.nodes, node{
			prefix:    s.Prefix,
			children:  append([]int(nil), s.Children...),
			docs:      append([]fts.DocRef(nil), s.Docs...),
			positions: positions,
		})
		for _, d := range s.Docs {
			if _, ok := idx.docToOrd[d.ID]; !ok {
				idx.docToOrd[d.ID] = d.Seq
			}
		}
	}

	return idx, nil
}

func (t *Index) newNode(prefix string) int {
	t.nodes = append(t.nodes, node{prefix: prefix})
	return len(t.nodes) - 1
}

func (t *Index) ordinalFor(id fts.DocID) uint32 {
	if ord, ok := t.docToOrd[id]; ok {
		return ord
	}
	ord := uint32(len(t.docToOrd))
	t.docToOrd[id] = ord
	return ord
}

func lcp(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func (t *Index) Insert(word string, docID fts.DocID) error {
	return t.insert(word, docID, false, 0)
}

func (t *Index) InsertAt(word string, docID fts.DocID, position uint32) error {
	return t.insert(word, docID, true, position)
}

func (t *Index) insert(word string, docID fts.DocID, hasPos bool, pos uint32) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	current := t.root
	rest := word

	for {
		advanced := false
		for i, child := range t.nodes[current].children {
			p := lcp(rest, t.nodes[child].prefix)
			if p == 0 {
				continue
			}

			if p == len(t.nodes[child].prefix) {
				// The current edge matches fully, so continue inserting the
				// remaining suffix under this child.
				current = child
				rest = rest[p:]
				if rest == "" {
					// The word ends exactly on this node.
					t.addDoc(current, docID, hasPos, pos)
					return nil
				}
				advanced = true
				break
			}

			// Partial overlap: split the edge into a shared prefix plus two
			// suffixes, one for the existing child and one for the new word.
			common := t.nodes[child].prefix[:p]
			childSuffix := t.nodes[child].prefix[p:]
			newSuffix := rest[p:]

			middle := t.newNode(common)
			t.nodes[child].prefix = childSuffix
			t.nodes[middle].children = append(t.nodes[middle].children, child)
			t.nodes[current].children[i] = middle

			if newSuffix != "" {
				newIdx := t.newNode(newSuffix)
				t.addDoc(newIdx, docID, hasPos, pos)
				t.nodes[middle].children = append(t.nodes[middle].children, newIdx)
				return nil
			}

			t.addDoc(middle, docID, hasPos, pos)
			return nil
		}

		if advanced {
			// Continue from this child with the remaining suffix.
			continue
		}

		// No child matches the remaining suffix, so attach it as a new edge.
		newIdx := t.newNode(rest)
		t.addDoc(newIdx, docID, hasPos, pos)
		t.nodes[current].children = append(t.nodes[current].children, newIdx)
		return nil
	}
}

func (t *Index) addDoc(nodeIdx int, docID fts.DocID, hasPos bool, pos uint32) {
	n := &t.nodes[nodeIdx]
	if last := len(n.docs) - 1; last >= 0 && n.docs[last].ID == docID {
		n.docs[last].Count++
		if hasPos {
			t.growPositions(nodeIdx, len(n.docs))
			n.positions[last] = append(n.positions[last], pos)
		}
		return
	}
	for i := range n.docs {
		if n.docs[i].ID == docID {
			n.docs[i].Count++
			if hasPos {
				t.growPositions(nodeIdx, len(n.docs))
				n.positions[i] = append(n.positions[i], pos)
			}
			return
		}
	}
	seq := t.ordinalFor(docID)
	n.docs = append(n.docs, fts.DocRef{ID: docID, Count: 1, Seq: seq})
	if hasPos {
		t.growPositions(nodeIdx, len(n.docs))
		last := len(n.docs) - 1
		n.positions[last] = append(n.positions[last], pos)
	}
}

func (t *Index) growPositions(nodeIdx int, want int) {
	n := &t.nodes[nodeIdx]
	for len(n.positions) < want {
		n.positions = append(n.positions, nil)
	}
}

func (t *Index) Search(word string) ([]fts.DocRef, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.root
	rest := word

	for {
		nextNode, nextRest, matched, exact := t.next(current, rest)
		if nextNode == 0 || !matched {
			return nil, nil
		}
		if exact {
			return t.nodes[nextNode].docs, nil
		}
		current = nextNode
		rest = nextRest
	}
}

func (t *Index) SearchPositional(word string) ([]fts.PositionalDocRef, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.root
	rest := word

	for {
		nextNode, nextRest, matched, exact := t.next(current, rest)
		if nextNode == 0 || !matched {
			return nil, nil
		}
		if exact {
			n := &t.nodes[nextNode]
			out := make([]fts.PositionalDocRef, 0, len(n.docs))
			for i := range n.docs {
				var positions []uint32
				if i < len(n.positions) {
					positions = n.positions[i]
				}
				out = append(out, fts.PositionalDocRef{
					ID:        n.docs[i].ID,
					Positions: positions,
				})
			}
			return out, nil
		}
		current = nextNode
		rest = nextRest
	}
}

func (t *Index) next(current int, rest string) (int, string, bool, bool) {
	for _, child := range t.nodes[current].children {
		p := lcp(rest, t.nodes[child].prefix)
		if p == 0 {
			continue
		}
		if p == len(rest) {
			if p == len(t.nodes[child].prefix) && t.nodes[child].isTerminal() {
				return child, "", true, true
			}
			return 0, "", false, false
		}
		if p == len(t.nodes[child].prefix) {
			return child, rest[p:], true, false
		}
		return 0, "", false, false
	}
	return 0, "", false, false
}

func (n *node) isTerminal() bool {
	return len(n.docs) > 0
}

func (t *Index) Analyze() fts.Stats {
	var s fts.Stats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var dfs func(n int, depth int)
	dfs = func(n int, depth int) {
		s.Nodes++
		totalDepth += depth
		if t.nodes[n].isTerminal() {
			s.Leaves++
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

var _ fts.PositionalIndex = (*Index)(nil)

var _ fts.Index = (*Index)(nil)
