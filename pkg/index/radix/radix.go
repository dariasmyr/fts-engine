package radix

import (
	"encoding/gob"
	"fmt"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"io"
	"sort"
	"sync"
)

type node struct {
	terminal  bool
	prefix    string
	children  []*node
	docs      map[fts.DocID]uint32
	seqs      map[fts.DocID]uint32
	positions map[fts.DocID][]uint32
}

func newNode(prefix string) *node {
	return &node{
		prefix:    prefix,
		docs:      make(map[fts.DocID]uint32),
		seqs:      make(map[fts.DocID]uint32),
		positions: make(map[fts.DocID][]uint32),
	}
}

type Index struct {
	root     *node
	docToOrd map[fts.DocID]uint32
	mu       sync.RWMutex
}

type snapshotNode struct {
	Terminal  bool
	Prefix    string
	Docs      []fts.DocRef
	Positions []fts.PositionalDocRef
	Children  []snapshotNode
}

func New() *Index {
	return &Index{root: newNode(""), docToOrd: make(map[fts.DocID]uint32)}
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

	idx := &Index{root: decodeNode(snap), docToOrd: make(map[fts.DocID]uint32)}
	collectOrdinals(idx.docToOrd, idx.root)
	return idx, nil
}

func encodeNode(n *node) snapshotNode {
	if n == nil {
		return snapshotNode{}
	}

	snap := snapshotNode{
		Terminal:  n.terminal,
		Prefix:    n.prefix,
		Docs:      collectDocs(n.docs, n.seqs),
		Positions: collectPositionalDocs(n.positions, n.seqs),
		Children:  make([]snapshotNode, 0, len(n.children)),
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
		n.seqs[doc.ID] = doc.Seq
	}
	for _, doc := range s.Positions {
		n.positions[doc.ID] = append([]uint32(nil), doc.Positions...)
	}

	n.children = make([]*node, 0, len(s.Children))
	for _, child := range s.Children {
		n.children = append(n.children, decodeNode(child))
	}

	return n
}

func collectOrdinals(docToOrd map[fts.DocID]uint32, n *node) {
	if n == nil {
		return
	}
	for id, seq := range n.seqs {
		if _, ok := docToOrd[id]; !ok {
			docToOrd[id] = seq
		}
	}
	for _, child := range n.children {
		collectOrdinals(docToOrd, child)
	}
}

func (t *Index) recordOrd(id fts.DocID, ords ...fts.DocOrd) uint32 {
	if existing, ok := t.docToOrd[id]; ok {
		return existing
	}
	ord := fts.DocOrd(len(t.docToOrd))
	if len(ords) > 0 {
		ord = ords[0]
	}
	seq := uint32(ord)
	t.docToOrd[id] = seq
	return seq
}

func lcp(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

func (t *Index) Insert(word string, docID fts.DocID, ord ...fts.DocOrd) error {
	return t.insert(word, docID, false, 0, ord...)
}

func (t *Index) InsertAt(word string, docID fts.DocID, position uint32, ord ...fts.DocOrd) error {
	return t.insert(word, docID, true, position, ord...)
}

func (t *Index) insert(word string, docID fts.DocID, hasPos bool, pos uint32, ords ...fts.DocOrd) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	ord := fts.DocOrd(t.recordOrd(docID, ords...))

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
					t.recordDoc(current, docID, ord, hasPos, pos)
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
				t.recordDoc(n, docID, ord, hasPos, pos)
				middle.children = append(middle.children, n)
				return nil
			}

			t.recordDoc(middle, docID, ord, hasPos, pos)
			return nil
		}

		n = newNode(rest)
		t.recordDoc(n, docID, ord, hasPos, pos)
		current.children = append(current.children, n)
		return nil

	NEXT:
	}
}

func (t *Index) recordDoc(n *node, docID fts.DocID, ord fts.DocOrd, hasPos bool, pos uint32) {
	n.terminal = true
	if _, ok := n.docs[docID]; !ok {
		n.seqs[docID] = t.recordOrd(docID, ord)
	}
	n.docs[docID]++
	if hasPos {
		n.positions[docID] = append(n.positions[docID], pos)
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
			return collectDocs(nextNode.docs, nextNode.seqs), nil
		}
		current = nextNode
		rest = nextRest
	}
}

func (t *Index) SearchPrefix(prefix string) ([]fts.DocRef, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	merged := make(map[fts.DocID]fts.DocRef)
	t.collectPrefixDocs(t.root, prefix, merged)
	return mergedDocsSlice(merged), nil
}

func (t *Index) SearchPositional(word string) ([]fts.PositionalDocRef, error) {
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
			return collectPositionalDocs(nextNode.positions, nextNode.seqs), nil
		}
		current = nextNode
		rest = nextRest
	}
}

func collectDocs(docs map[fts.DocID]uint32, seqs map[fts.DocID]uint32) []fts.DocRef {
	res := make([]fts.DocRef, 0, len(docs))
	for id, count := range docs {
		seq := seqs[id]
		res = append(res, fts.DocRef{ID: id, Ord: fts.DocOrd(seq), Count: count, Seq: seq})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Seq < res[j].Seq })
	return res
}

func collectPositionalDocs(positions map[fts.DocID][]uint32, seqs map[fts.DocID]uint32) []fts.PositionalDocRef {
	type positionalRef struct {
		fts.PositionalDocRef
		seq uint32
	}
	res := make([]positionalRef, 0, len(positions))
	for id, pos := range positions {
		seq := seqs[id]
		res = append(res, positionalRef{PositionalDocRef: fts.PositionalDocRef{ID: id, Ord: fts.DocOrd(seq), Positions: pos}, seq: seq})
	}
	sort.Slice(res, func(i, j int) bool { return res[i].seq < res[j].seq })
	out := make([]fts.PositionalDocRef, 0, len(res))
	for _, ref := range res {
		out = append(out, ref.PositionalDocRef)
	}
	return out
}

func addMergedDoc(merged map[fts.DocID]fts.DocRef, id fts.DocID, count, seq uint32) {
	ref, ok := merged[id]
	if !ok {
		merged[id] = fts.DocRef{ID: id, Ord: fts.DocOrd(seq), Count: count, Seq: seq}
		return
	}
	ref.Count += count
	merged[id] = ref
}

func mergedDocsSlice(merged map[fts.DocID]fts.DocRef) []fts.DocRef {
	out := make([]fts.DocRef, 0, len(merged))
	for _, doc := range merged {
		out = append(out, doc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

func (t *Index) collectPrefixDocs(current *node, prefix string, merged map[fts.DocID]fts.DocRef) {
	if current == nil {
		return
	}
	if prefix == "" {
		t.collectSubtreeDocs(current, merged)
		return
	}

	for _, child := range current.children {
		p := lcp(prefix, child.prefix)
		if p == 0 {
			continue
		}
		if p == len(prefix) {
			t.collectSubtreeDocs(child, merged)
			continue
		}
		if p == len(child.prefix) {
			t.collectPrefixDocs(child, prefix[p:], merged)
		}
	}
}

func (t *Index) collectSubtreeDocs(current *node, merged map[fts.DocID]fts.DocRef) {
	if current == nil {
		return
	}
	if current.terminal {
		for id, count := range current.docs {
			addMergedDoc(merged, id, count, current.seqs[id])
		}
	}
	for _, child := range current.children {
		t.collectSubtreeDocs(child, merged)
	}
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
var _ fts.PrefixIndex = (*Index)(nil)
var _ fts.PositionalIndex = (*Index)(nil)
