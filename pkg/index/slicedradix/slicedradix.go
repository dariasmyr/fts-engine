package slicedradix

import (
	"encoding/gob"
	"fmt"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"io"
	"sync"
)

type node struct {
	prefix   string
	children []int
	docs     []fts.DocRef
}

type Index struct {
	root  int
	nodes []node
	mu    sync.RWMutex
}

type snapshotNode struct {
	Prefix   string
	Children []int
	Docs     []fts.DocRef
}

type snapshotIndex struct {
	Root  int
	Nodes []snapshotNode
}

func New() *Index {
	var t Index
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
		snap.Nodes = append(snap.Nodes, snapshotNode{
			Prefix:   n.prefix,
			Children: append([]int(nil), n.children...),
			Docs:     append([]fts.DocRef(nil), n.docs...),
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
		root:  snap.Root,
		nodes: make([]node, 0, len(snap.Nodes)),
	}

	for i := range snap.Nodes {
		s := snap.Nodes[i]
		idx.nodes = append(idx.nodes, node{
			prefix:   s.Prefix,
			children: append([]int(nil), s.Children...),
			docs:     append([]fts.DocRef(nil), s.Docs...),
		})
	}

	return idx, nil
}

func (t *Index) newNode(prefix string) int {
	t.nodes = append(t.nodes, node{prefix: prefix})
	return len(t.nodes) - 1
}

func lcp(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func (t *Index) Insert(word string, docID fts.DocID) error {
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
				current = child
				rest = rest[p:]
				if rest == "" {
					t.addDoc(current, docID)
					return nil
				}
				advanced = true
				break
			}

			common := t.nodes[child].prefix[:p]
			childSuffix := t.nodes[child].prefix[p:]
			newSuffix := rest[p:]

			middle := t.newNode(common)
			t.nodes[child].prefix = childSuffix
			t.nodes[middle].children = append(t.nodes[middle].children, child)
			t.nodes[current].children[i] = middle

			if newSuffix != "" {
				newIdx := t.newNode(newSuffix)
				t.addDoc(newIdx, docID)
				t.nodes[middle].children = append(t.nodes[middle].children, newIdx)
				return nil
			}

			t.addDoc(middle, docID)
			return nil
		}

		if advanced {
			continue
		}

		newIdx := t.newNode(rest)
		t.addDoc(newIdx, docID)
		t.nodes[current].children = append(t.nodes[current].children, newIdx)
		return nil
	}
}

func (t *Index) addDoc(nodeIdx int, docID fts.DocID) {
	n := &t.nodes[nodeIdx]
	for i := range n.docs {
		if n.docs[i].ID == docID {
			n.docs[i].Count++
			return
		}
	}
	n.docs = append(n.docs, fts.DocRef{ID: docID, Count: 1})
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

var _ fts.Index = (*Index)(nil)
