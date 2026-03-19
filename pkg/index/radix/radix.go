package radix

import (
	"encoding/gob"
	"fmt"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"io"
	"sync"
)

type node struct {
	terminal bool
	prefix   string
	children []*node
	docs     map[fts.DocID]uint32
}

func newNode(prefix string) *node {
	return &node{
		prefix: prefix,
		docs:   make(map[fts.DocID]uint32),
	}
}

type Index struct {
	root *node
	mu   sync.RWMutex
}

type snapshotNode struct {
	Terminal bool
	Prefix   string
	Docs     []fts.DocRef
	Children []snapshotNode
}

func New() *Index {
	return &Index{root: newNode("")}
}

func (t *Index) Serialize(w io.Writer) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return fmt.Errorf("radix: serialize: nil root")
	}

	if err := gob.NewEncoder(w).Encode(encodeNode(t.root)); err != nil {
		return fmt.Errorf("radix: serialize: %w", err)
	}

	return nil
}

func Load(r io.Reader) (fts.Index, error) {
	var snap snapshotNode
	if err := gob.NewDecoder(r).Decode(&snap); err != nil {
		return nil, fmt.Errorf("radix: load: %w", err)
	}

	return &Index{root: decodeNode(snap)}, nil
}

func encodeNode(n *node) snapshotNode {
	if n == nil {
		return snapshotNode{}
	}

	snap := snapshotNode{
		Terminal: n.terminal,
		Prefix:   n.prefix,
		Docs:     collectDocs(n.docs),
		Children: make([]snapshotNode, 0, len(n.children)),
	}

	for _, child := range n.children {
		snap.Children = append(snap.Children, encodeNode(child))
	}

	return snap
}

func decodeNode(s snapshotNode) *node {
	n := newNode(s.Prefix)
	n.terminal = s.Terminal
	for _, doc := range s.Docs {
		n.docs[doc.ID] = doc.Count
	}

	n.children = make([]*node, 0, len(s.Children))
	for _, child := range s.Children {
		n.children = append(n.children, decodeNode(child))
	}

	return n
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

	var n *node
	for {
		for i, child := range current.children {
			p := lcp(rest, child.prefix)
			if p == 0 {
				continue
			}

			if p == len(child.prefix) {
				current = child
				rest = rest[p:]
				if rest == "" {
					current.terminal = true
					current.docs[docID]++
					return nil
				}
				goto NEXT
			}

			common := child.prefix[:p]
			childSuffix := child.prefix[p:]
			newSuffix := rest[p:]

			middle := newNode(common)
			child.prefix = childSuffix
			middle.children = append(middle.children, child)
			current.children[i] = middle

			if newSuffix != "" {
				n = newNode(newSuffix)
				n.terminal = true
				n.docs[docID]++
				middle.children = append(middle.children, n)
				return nil
			}

			middle.terminal = true
			middle.docs[docID]++
			return nil
		}

		n = newNode(rest)
		n.terminal = true
		n.docs[docID]++
		current.children = append(current.children, n)
		return nil

	NEXT:
	}
}

func (t *Index) Search(word string) ([]fts.DocRef, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.root
	rest := word

	for {
		nextNode, nextRest, matched, exact := t.next(current, rest)
		if !matched {
			return nil, nil
		}
		if exact {
			return collectDocs(nextNode.docs), nil
		}
		current = nextNode
		rest = nextRest
	}
}

func collectDocs(docs map[fts.DocID]uint32) []fts.DocRef {
	res := make([]fts.DocRef, 0, len(docs))
	for id, count := range docs {
		res = append(res, fts.DocRef{ID: id, Count: count})
	}
	return res
}

func (t *Index) next(current *node, rest string) (*node, string, bool, bool) {
	for _, child := range current.children {
		p := lcp(rest, child.prefix)
		if p == 0 {
			continue
		}
		if p == len(rest) {
			if child.terminal {
				return child, "", true, true
			}
			return nil, "", false, false
		}
		if p == len(child.prefix) {
			return child, rest[p:], true, false
		}
		return nil, "", false, false
	}

	return nil, "", false, false
}

func (t *Index) Analyze() fts.Stats {
	var s fts.Stats
	var totalDepth int

	levelChildrenSum := make(map[int]int)
	levelNodeCount := make(map[int]int)

	var dfs func(n *node, depth int)
	dfs = func(n *node, depth int) {
		s.Nodes++
		totalDepth += depth
		if n.terminal {
			s.Leaves++
		}
		if depth > s.MaxDepth {
			s.MaxDepth = depth
		}
		s.TotalDocs += len(n.docs)

		numChildren := len(n.children)
		s.TotalChildren += numChildren
		levelChildrenSum[depth] += numChildren
		levelNodeCount[depth]++

		for _, c := range n.children {
			if c != nil {
				dfs(c, depth+1)
			}
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
