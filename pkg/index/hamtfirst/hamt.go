package hamtfirst

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"math/bits"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/segment"
)

const (
	quant     = 5
	lowerbits = uint32(1<<quant) - 1
	depth     = 7
)

type postingList struct {
	first fts.Posting
	rest  []fts.Posting
}

func newPostingList(ord fts.DocOrd) postingList {
	return postingList{first: newPosting(ord)}
}

func postingListFromSlice(postings []fts.Posting) (postingList, error) {
	if len(postings) == 0 {
		return postingList{}, fmt.Errorf("empty posting list")
	}
	return postingList{
		first: postings[0],
		rest:  append([]fts.Posting(nil), postings[1:]...),
	}, nil
}

func newPosting(ord fts.DocOrd) fts.Posting {
	return fts.Posting{Ord: ord, Count: 1, Seq: uint32(ord)}
}

func (p *postingList) Len() int {
	return 1 + len(p.rest)
}

func (p *postingList) At(i int) *fts.Posting {
	if i == 0 {
		return &p.first
	}
	return &p.rest[i-1]
}

func (p *postingList) Add(ord fts.DocOrd) (idx int, added bool) {
	lastIdx := p.Len() - 1
	last := p.At(lastIdx)
	if last.Ord == ord {
		last.Count++
		return lastIdx, false
	}
	if ord > last.Ord {
		p.rest = append(p.rest, newPosting(ord))
		return p.Len() - 1, true
	}

	idx = sort.Search(p.Len(), func(i int) bool { return p.At(i).Ord >= ord })
	if posting := p.At(idx); posting.Ord == ord {
		posting.Count++
		return idx, false
	}

	p.insertAt(idx, newPosting(ord))
	return idx, true
}

func (p *postingList) insertAt(idx int, posting fts.Posting) {
	if idx == 0 {
		p.rest = append(p.rest, fts.Posting{})
		copy(p.rest[1:], p.rest[:len(p.rest)-1])
		p.rest[0] = p.first
		p.first = posting
		return
	}

	restIdx := idx - 1
	p.rest = append(p.rest, fts.Posting{})
	copy(p.rest[restIdx+1:], p.rest[restIdx:])
	p.rest[restIdx] = posting
}

func (p *postingList) CloneSlice() []fts.Posting {
	out := make([]fts.Posting, p.Len())
	out[0] = p.first
	copy(out[1:], p.rest)
	return out
}

type entry struct {
	key       string
	docs      postingList
	positions [][]uint32
}

func newEntry(key string, ord fts.DocOrd, hasPos bool, pos uint32) entry {
	e := entry{key: key, docs: newPostingList(ord)}
	if hasPos {
		e.positions = [][]uint32{{pos}}
	}
	return e
}

func (e *entry) add(ord fts.DocOrd, hasPos bool, pos uint32) {
	idx, added := e.docs.Add(ord)
	if !added {
		if hasPos {
			e.positions = growPositions(e.positions, e.docs.Len())
			e.positions[idx] = append(e.positions[idx], pos)
		}
		return
	}

	if !hasPos && idx >= len(e.positions) {
		return
	}

	e.positions = growPositions(e.positions, e.docs.Len())
	copy(e.positions[idx+1:], e.positions[idx:])
	e.positions[idx] = nil
	if hasPos {
		e.positions[idx] = []uint32{pos}
	}
}

type nodeptr = uint32

type terminal struct {
	entries []entry
}

func (t *terminal) Append(word string, ord fts.DocOrd, hasPos bool, pos uint32) {
	i := sort.Search(len(t.entries), func(i int) bool { return t.entries[i].key >= word })
	if i < len(t.entries) && t.entries[i].key == word {
		t.entries[i].add(ord, hasPos, pos)
		return
	}

	t.entries = append(t.entries, entry{})
	copy(t.entries[i+1:], t.entries[i:])
	t.entries[i] = newEntry(word, ord, hasPos, pos)
}

func (t *terminal) Find(word string) *entry {
	i := sort.Search(len(t.entries), func(i int) bool { return t.entries[i].key >= word })
	if i < len(t.entries) && t.entries[i].key == word {
		return &t.entries[i]
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

type snapshotEntry struct {
	Key       string
	Docs      []fts.Posting
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
	return &Index{nodes: make([]node, 1)}
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
		for j := range term.entries {
			e := &term.entries[j]
			entries = append(entries, snapshotEntry{
				Key:       e.key,
				Docs:      e.docs.CloneSlice(),
				Positions: clonePositions(e.positions),
			})
		}
		snap.Terms = append(snap.Terms, snapshotTerminal{Entries: entries})
	}

	if err := gob.NewEncoder(w).Encode(snap); err != nil {
		return fmt.Errorf("hamtfirst: serialize: %w", err)
	}
	return nil
}

func Load(r io.Reader) (fts.Index, error) {
	var snap snapshotIndex
	if err := gob.NewDecoder(r).Decode(&snap); err != nil {
		return nil, fmt.Errorf("hamtfirst: load: %w", err)
	}

	idx := &Index{
		nodes: make([]node, 0, len(snap.Nodes)),
		terms: make([]terminal, 0, len(snap.Terms)),
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
		for _, encoded := range s.Entries {
			docs, err := postingListFromSlice(encoded.Docs)
			if err != nil {
				return nil, fmt.Errorf("hamtfirst: load term %q: %w", encoded.Key, err)
			}
			entries = append(entries, entry{
				key:       encoded.Key,
				docs:      docs,
				positions: clonePositions(encoded.Positions),
			})
		}
		idx.terms = append(idx.terms, terminal{entries: entries})
	}
	if len(idx.nodes) == 0 {
		idx.nodes = make([]node, 1)
	}
	return idx, nil
}

func (t *Index) Search(key string) ([]fts.Posting, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	e := t.findEntry(key)
	if e == nil {
		return nil, nil
	}
	out := e.docs.CloneSlice()
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (t *Index) SearchPositional(key string) ([]fts.PositionalPosting, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	e := t.findEntry(key)
	if e == nil {
		return nil, nil
	}
	return collectPositionalDocs(&e.docs, e.positions), nil
}

func (t *Index) findEntry(key string) *entry {
	n := nodeptr(0)
	hash := strhash32(key)
	for range depth - 1 {
		var ok bool
		n, ok = t.nextNode(n, hash)
		if !ok {
			return nil
		}
		hash >>= quant
	}

	term := t.terms[n]
	if term.entries == nil {
		return nil
	}
	return term.Find(key)
}

func (t *Index) SearchPrefix(prefix string) ([]fts.Posting, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	merged := make(map[fts.DocOrd]fts.Posting)
	for i := range t.terms {
		for j := range t.terms[i].entries {
			e := &t.terms[i].entries[j]
			if !strings.HasPrefix(e.key, prefix) {
				continue
			}
			addMergedPosting(merged, e.docs.first)
			for _, posting := range e.docs.rest {
				addMergedPosting(merged, posting)
			}
		}
	}
	return mergedPostingsSlice(merged), nil
}

func (t *Index) ExportSegmentTerms(yield func(segment.TermPostings) error) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for i := range t.terms {
		entries := t.terms[i].entries
		for j := range entries {
			if err := yield(segment.TermPostings{
				Term:      entries[j].key,
				Postings:  entries[j].docs.CloneSlice(),
				Positions: clonePositions(entries[j].positions),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Index) Insert(word string, ord fts.DocOrd) error {
	return t.insert(word, false, 0, ord)
}

func (t *Index) InsertAt(word string, position uint32, ord fts.DocOrd) error {
	return t.insert(word, true, position, ord)
}

func (t *Index) insert(word string, hasPos bool, pos uint32, ord fts.DocOrd) error {
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
	t.terms[termPtr].Append(word, ord, hasPos, pos)
	return nil
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

func collectPositionalDocs(docs *postingList, positions [][]uint32) []fts.PositionalPosting {
	out := make([]fts.PositionalPosting, docs.Len())
	if len(positions) > 0 {
		out[0].Positions = positions[0]
	}
	out[0].Ord = docs.first.Ord

	for restIdx, posting := range docs.rest {
		logicalIdx := restIdx + 1
		out[logicalIdx].Ord = posting.Ord
		if logicalIdx < len(positions) {
			out[logicalIdx].Positions = positions[logicalIdx]
		}
	}
	return out
}

func addMergedPosting(merged map[fts.DocOrd]fts.Posting, posting fts.Posting) {
	ref, ok := merged[posting.Ord]
	if !ok {
		merged[posting.Ord] = posting
		return
	}
	ref.Count += posting.Count
	merged[posting.Ord] = ref
}

func mergedPostingsSlice(merged map[fts.DocOrd]fts.Posting) []fts.Posting {
	out := make([]fts.Posting, 0, len(merged))
	for _, posting := range merged {
		out = append(out, posting)
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
			for i := range term.entries {
				s.TotalDocs += term.entries[i].docs.Len()
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
		for _, child := range n.children {
			if currentDepth == depth-2 {
				dfs(child, currentDepth+1, true)
			} else {
				dfs(child, currentDepth+1, false)
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
var _ fts.Analyzer = (*Index)(nil)
var _ fts.Serializable = (*Index)(nil)
var _ segment.Source = (*Index)(nil)
