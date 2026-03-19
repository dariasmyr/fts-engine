package hamtpointered

import (
	"encoding/gob"
	"fmt"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"hash/fnv"
	"io"
	"math/bits"
	"sync"
)

const (
	quant     = 5
	lowerbits = uint32(1<<quant) - 1
	depth     = 7
)

type node struct {
	bitmap   uint32
	children []any
}

type terminalNode struct {
	entries []entry
}

type entry struct {
	key  string
	docs []fts.DocRef
}

type Index struct {
	root *node
	mu   sync.RWMutex
}

type snapshotNode struct {
	Bitmap   uint32
	Children []snapshotChild
}

type snapshotChild struct {
	Node     *snapshotNode
	Terminal *snapshotTerminal
}

type snapshotTerminal struct {
	Entries []entry
}

func newNode() *node {
	return &node{children: make([]any, 0)}
}

func New() *Index {
	return &Index{root: newNode()}
}

func (t *Index) Serialize(w io.Writer) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.root == nil {
		return fmt.Errorf("hamtpointered: serialize: nil root")
	}

	if err := gob.NewEncoder(w).Encode(encodeNode(t.root)); err != nil {
		return fmt.Errorf("hamtpointered: serialize: %w", err)
	}

	return nil
}

func Load(r io.Reader) (fts.Index, error) {
	var snap snapshotNode
	if err := gob.NewDecoder(r).Decode(&snap); err != nil {
		return nil, fmt.Errorf("hamtpointered: load: %w", err)
	}

	return &Index{root: decodeNode(&snap)}, nil
}

func encodeNode(n *node) *snapshotNode {
	if n == nil {
		return nil
	}

	snap := &snapshotNode{Bitmap: n.bitmap, Children: make([]snapshotChild, 0, len(n.children))}
	for _, child := range n.children {
		s := snapshotChild{}
		switch v := child.(type) {
		case *node:
			s.Node = encodeNode(v)
		case *terminalNode:
			s.Terminal = &snapshotTerminal{Entries: append([]entry(nil), v.entries...)}
		}
		snap.Children = append(snap.Children, s)
	}

	return snap
}

func decodeNode(s *snapshotNode) *node {
	if s == nil {
		return nil
	}

	n := &node{bitmap: s.Bitmap, children: make([]any, 0, len(s.Children))}
	for i := range s.Children {
		child := s.Children[i]
		if child.Node != nil {
			n.children = append(n.children, decodeNode(child.Node))
			continue
		}
		if child.Terminal != nil {
			n.children = append(n.children, &terminalNode{entries: append([]entry(nil), child.Terminal.Entries...)})
		}
	}

	return n
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func (n *node) nextNode(hash uint32, level int) (child any, pos int, mask uint32) {
	idx := int((hash >> (level * quant)) & lowerbits)
	mask = 1 << idx
	pos = bits.OnesCount32(n.bitmap & (mask - 1))
	if n.bitmap&mask == 0 {
		return nil, pos, mask
	}
	return n.children[pos], pos, mask
}

func (n *node) appendChild(newChild any, mask uint32, pos int) {
	n.bitmap |= mask
	n.children = append(n.children[:pos], append([]any{newChild}, n.children[pos:]...)...)
}

func (n *node) insertNode(hash uint32, key string, docID fts.DocID, level int) {
	child, pos, mask := n.nextNode(hash, level)

	if level == depth {
		if child == nil {
			tn := &terminalNode{entries: []entry{{key: key, docs: []fts.DocRef{{ID: docID, Count: 1}}}}}
			n.appendChild(tn, mask, pos)
			return
		}

		t := child.(*terminalNode)
		for i := range t.entries {
			if key == t.entries[i].key {
				addDoc(&t.entries[i].docs, docID)
				return
			}
		}

		t.entries = append(t.entries, entry{key: key, docs: []fts.DocRef{{ID: docID, Count: 1}}})
		return
	}

	if child == nil {
		newChild := newNode()
		n.appendChild(newChild, mask, pos)
		child = newChild
	}

	child.(*node).insertNode(hash, key, docID, level+1)
}

func addDoc(docs *[]fts.DocRef, docID fts.DocID) {
	for i := range *docs {
		if (*docs)[i].ID == docID {
			(*docs)[i].Count++
			return
		}
	}
	*docs = append(*docs, fts.DocRef{ID: docID, Count: 1})
}

func (t *Index) Insert(word string, docID fts.DocID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.root.insertNode(hashKey(word), word, docID, 0)
	return nil
}

func (t *Index) Search(word string) ([]fts.DocRef, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	hash := hashKey(word)
	n := t.root

	for level := 0; level <= depth; level++ {
		child, _, _ := n.nextNode(hash, level)
		if child == nil {
			return nil, nil
		}

		if level == depth {
			term := child.(*terminalNode)
			for i := range term.entries {
				if word == term.entries[i].key {
					return term.entries[i].docs, nil
				}
			}
			return nil, nil
		}

		n = child.(*node)
	}

	return nil, nil
}

func (t *Index) Analyze() fts.Stats {
	var s fts.Stats
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
		case *node:
			s.TotalChildren += len(node.children)
			levelChildrenSum[depth] += len(node.children)
			levelNodeCount[depth]++
			for _, c := range node.children {
				dfs(c, depth+1)
			}
		case *terminalNode:
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

var _ fts.Index = (*Index)(nil)
