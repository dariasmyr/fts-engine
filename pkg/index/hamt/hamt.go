package hamt

import (
	"encoding/gob"
	"fmt"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"hash/fnv"
	"io"
	"math/bits"
	"slices"
	"sort"
	"strings"
	"sync"
)

const (
	quant     = 5
	lowerbits = uint32(1<<quant) - 1
	depth     = 7
)

type documents []fts.DocRef

func (d documents) Add(id fts.DocID, seq uint32, positions [][]uint32, hasPos bool, pos uint32) (documents, [][]uint32) {
	i := sort.Search(len(d), func(i int) bool { return d[i].ID >= id })
	if i < len(d) && d[i].ID == id {
		d[i].Count++
		if hasPos {
			positions = growPositions(positions, len(d))
			positions[i] = append(positions[i], pos)
		}
		return d, positions
	}
	d = append(d, fts.DocRef{})
	copy(d[i+1:], d[i:])
	d[i] = fts.DocRef{ID: id, Count: 1, Seq: seq}
	if hasPos {
		positions = growPositions(positions, len(d))
		copy(positions[i+1:], positions[i:])
		positions[i] = []uint32{pos}
	}
	return d, positions
}

type entry struct {
	key       string
	docs      documents
	positions [][]uint32
}

type nodeptr = uint32

type terminal struct {
	entries []entry
}

func (t *terminal) Append(word string, id fts.DocID, seq uint32, hasPos bool, pos uint32) {
	i := sort.Search(len(t.entries), func(i int) bool { return t.entries[i].key >= word })
	if i < len(t.entries) && t.entries[i].key == word {
		t.entries[i].docs, t.entries[i].positions = t.entries[i].docs.Add(id, seq, t.entries[i].positions, hasPos, pos)
		return
	}
	t.entries = append(t.entries, entry{})
	copy(t.entries[i+1:], t.entries[i:])
	e := entry{key: word, docs: documents{{ID: id, Count: 1, Seq: seq}}}
	if hasPos {
		e.positions = [][]uint32{{pos}}
	}
	t.entries[i] = e
}

func (t *terminal) Find(word string) documents {
	i := sort.Search(len(t.entries), func(i int) bool { return t.entries[i].key >= word })
	if i < len(t.entries) && t.entries[i].key == word {
		return t.entries[i].docs
	}
	return nil
}

func (t *terminal) FindPositional(word string) []fts.PositionalDocRef {
	i := sort.Search(len(t.entries), func(i int) bool { return t.entries[i].key >= word })
	if i < len(t.entries) && t.entries[i].key == word {
		return collectPositionalDocs(t.entries[i].docs, t.entries[i].positions)
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
	mu       sync.RWMutex
	nodes    []node
	terms    []terminal
	docToOrd map[fts.DocID]uint32
}

type snapshotEntry struct {
	Key       string
	Docs      []fts.DocRef
	Positions [][]uint32
}

type snapshotTerminal struct {
	Entries []snapshotEntry
}

type snapshotNode struct {
	Bitmap   uint32
	Children []nodeptr
}

type snapshotIndex struct {
	Nodes []snapshotNode
	Terms []snapshotTerminal
}

func New() *Index {
	return &Index{nodes: make([]node, 1), docToOrd: make(map[fts.DocID]uint32)}
}

func (t *Index) Serialize(w io.Writer) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	snap := snapshotIndex{
		Nodes: make([]snapshotNode, 0, len(t.nodes)),
		Terms: make([]snapshotTerminal, 0, len(t.terms)),
	}

	for i := range t.nodes {
		n := t.nodes[i]
		snap.Nodes = append(snap.Nodes, snapshotNode{
			Bitmap:   n.bitmap,
			Children: append([]nodeptr(nil), n.children...),
		})
	}

	for i := range t.terms {
		term := t.terms[i]
		entries := make([]snapshotEntry, 0, len(term.entries))
		for _, e := range term.entries {
			entries = append(entries, snapshotEntry{Key: e.key, Docs: append([]fts.DocRef(nil), e.docs...), Positions: clonePositions(e.positions)})
		}
		snap.Terms = append(snap.Terms, snapshotTerminal{Entries: entries})
	}

	if err := gob.NewEncoder(w).Encode(snap); err != nil {
		return fmt.Errorf("hamt: serialize: %w", err)
	}

	return nil
}

func Load(r io.Reader) (fts.Index, error) {
	var snap snapshotIndex
	if err := gob.NewDecoder(r).Decode(&snap); err != nil {
		return nil, fmt.Errorf("hamt: load: %w", err)
	}

	idx := &Index{
		nodes:    make([]node, 0, len(snap.Nodes)),
		terms:    make([]terminal, 0, len(snap.Terms)),
		docToOrd: make(map[fts.DocID]uint32),
	}

	for i := range snap.Nodes {
		n := snap.Nodes[i]
		idx.nodes = append(idx.nodes, node{
			bitmap:   n.Bitmap,
			children: append([]nodeptr(nil), n.Children...),
		})
	}

	for i := range snap.Terms {
		s := snap.Terms[i]
		entries := make([]entry, 0, len(s.Entries))
		for _, e := range s.Entries {
			for _, d := range e.Docs {
				if _, ok := idx.docToOrd[d.ID]; !ok {
					idx.docToOrd[d.ID] = d.Seq
				}
			}
			entries = append(entries, entry{key: e.Key, docs: append([]fts.DocRef(nil), e.Docs...), positions: clonePositions(e.Positions)})
		}
		idx.terms = append(idx.terms, terminal{entries: entries})
	}

	if len(idx.nodes) == 0 {
		idx.nodes = make([]node, 1)
	}

	return idx, nil
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

	out := append([]fts.DocRef(nil), docs...)
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (t *Index) SearchPositional(key string) ([]fts.PositionalDocRef, error) {
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

	docs := term.FindPositional(key)
	if docs == nil {
		return nil, nil
	}

	return docs, nil
}

func (t *Index) SearchPrefix(prefix string) ([]fts.DocRef, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	merged := make(map[fts.DocID]fts.DocRef)
	for i := range t.terms {
		for _, entry := range t.terms[i].entries {
			if !strings.HasPrefix(entry.key, prefix) {
				continue
			}
			for _, doc := range entry.docs {
				addMergedDoc(merged, doc.ID, doc.Count, doc.Seq)
			}
		}
	}

	return mergedDocsSlice(merged), nil
}

func (t *Index) Insert(word string, id fts.DocID) error {
	return t.insert(word, id, false, 0)
}

func (t *Index) InsertAt(word string, id fts.DocID, position uint32) error {
	return t.insert(word, id, true, position)
}

func (t *Index) insert(word string, id fts.DocID, hasPos bool, pos uint32) error {
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

	seq := t.ordinalFor(id)
	t.terms[termPtr].Append(word, id, seq, hasPos, pos)
	return nil
}

func (t *Index) ordinalFor(id fts.DocID) uint32 {
	if ord, ok := t.docToOrd[id]; ok {
		return ord
	}
	ord := uint32(len(t.docToOrd))
	t.docToOrd[id] = ord
	return ord
}

func growPositions(positions [][]uint32, want int) [][]uint32 {
	for len(positions) < want {
		positions = append(positions, nil)
	}
	return positions
}

func clonePositions(src [][]uint32) [][]uint32 {
	if len(src) == 0 {
		return nil
	}
	out := make([][]uint32, len(src))
	for i := range src {
		out[i] = append([]uint32(nil), src[i]...)
	}
	return out
}

func collectPositionalDocs(docs []fts.DocRef, positions [][]uint32) []fts.PositionalDocRef {
	out := make([]fts.PositionalDocRef, 0, len(docs))
	for i := range docs {
		var pos []uint32
		if i < len(positions) {
			pos = positions[i]
		}
		out = append(out, fts.PositionalDocRef{ID: docs[i].ID, Positions: pos})
	}
	return out
}

func addMergedDoc(merged map[fts.DocID]fts.DocRef, id fts.DocID, count, seq uint32) {
	ref, ok := merged[id]
	if !ok {
		merged[id] = fts.DocRef{ID: id, Count: count, Seq: seq}
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
var _ fts.PrefixIndex = (*Index)(nil)
var _ fts.PositionalIndex = (*Index)(nil)
