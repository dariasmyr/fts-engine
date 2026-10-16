// Package hamt provides a HAMT-based full-text index.
package hamt

import (
	"encoding/gob"
	"fmt"
	"io"
	"math/bits"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/dariasmyr/fts-engine/v2/internal/fnv"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/posting"
)

const (
	quant     = 5
	lowerbits = uint32(1<<quant) - 1
	depth     = 7
)

type documents []fts.Posting

func (d documents) Add(ord fts.DocOrd, positions [][]uint32, hasPos bool, pos uint32) (documents, [][]uint32) {
	last := len(d) - 1
	if last >= 0 && d[last].Ord == ord {
		d[last].Count++
		if hasPos {
			positions = growPositions(positions, len(d))
			positions[last] = append(positions[last], pos)
		}
		return d, positions
	}
	if last < 0 || ord > d[last].Ord {
		d = append(d, fts.Posting{Ord: ord, Count: 1})
		if hasPos {
			positions = growPositions(positions, len(d))
			positions[len(d)-1] = []uint32{pos}
		}
		return d, positions
	}

	i := sort.Search(len(d), func(i int) bool { return d[i].Ord >= ord })
	if i < len(d) && d[i].Ord == ord {
		d[i].Count++
		if hasPos {
			positions = growPositions(positions, len(d))
			positions[i] = append(positions[i], pos)
		}
		return d, positions
	}
	d = append(d, fts.Posting{})
	copy(d[i+1:], d[i:])
	d[i] = fts.Posting{Ord: ord, Count: 1}
	if hasPos || i < len(positions) {
		positions = growPositions(positions, len(d))
		copy(positions[i+1:], positions[i:])
		positions[i] = nil
		if hasPos {
			positions[i] = []uint32{pos}
		}
	}
	return d, positions
}

type entry struct {
	key       string
	docs      documents
	positions [][]uint32
}

type exportTermRef struct {
	key      string
	terminal int
	entry    int
}

type nodeptr = uint32

type terminal struct {
	entries []entry
}

func (t *terminal) Append(word string, ord fts.DocOrd, hasPos bool, pos uint32) {
	i := sort.Search(len(t.entries), func(i int) bool { return t.entries[i].key >= word })
	if i < len(t.entries) && t.entries[i].key == word {
		t.entries[i].docs, t.entries[i].positions = t.entries[i].docs.Add(ord, t.entries[i].positions, hasPos, pos)
		return
	}
	t.entries = append(t.entries, entry{})
	copy(t.entries[i+1:], t.entries[i:])
	e := entry{key: word, docs: documents{{Ord: ord, Count: 1}}}
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

func (t *terminal) FindPositional(word string) []fts.PositionalPosting {
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
		for _, e := range term.entries {
			entries = append(entries, snapshotEntry{Key: e.key, Docs: append([]fts.Posting(nil), e.docs...), Positions: clonePositions(e.positions)})
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
		for _, e := range s.Entries {
			entries = append(entries, entry{key: e.Key, docs: append([]fts.Posting(nil), e.Docs...), positions: clonePositions(e.Positions)})
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

	n := nodeptr(0)
	hash := fnv.Sum32a(key)
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

	out := append([]fts.Posting(nil), docs...)
	sort.Slice(out, func(i, j int) bool { return out[i].Ord < out[j].Ord })
	return out, nil
}

func (t *Index) SearchPositional(key string) ([]fts.PositionalPosting, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	n := nodeptr(0)
	hash := fnv.Sum32a(key)
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

func (t *Index) SearchPrefix(prefix string) ([]fts.Posting, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	merged := make(map[fts.DocOrd]fts.Posting)
	for i := range t.terms {
		for _, entry := range t.terms[i].entries {
			if !strings.HasPrefix(entry.key, prefix) {
				continue
			}
			for _, doc := range entry.docs {
				addMergedDoc(merged, doc.Ord, doc.Count)
			}
		}
	}

	return mergedDocsSlice(merged), nil
}

func (t *Index) ExportPostings(yield func(posting.TermData) error) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	termCount := 0
	for i := range t.terms {
		termCount += len(t.terms[i].entries)
	}
	terms := make([]exportTermRef, 0, termCount)
	for i := range t.terms {
		entries := t.terms[i].entries
		for j := range entries {
			terms = append(terms, exportTermRef{
				key:      entries[j].key,
				terminal: i,
				entry:    j,
			})
		}
	}
	slices.SortFunc(terms, func(a, b exportTermRef) int { return strings.Compare(a.key, b.key) })
	for _, ref := range terms {
		entry := &t.terms[ref.terminal].entries[ref.entry]
		if err := yield(posting.TermData{
			Term:      ref.key,
			Postings:  []posting.Posting(entry.docs),
			Positions: entry.positions,
		}); err != nil {
			return err
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

	hash := fnv.Sum32a(word)
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

func collectPositionalDocs(docs []fts.Posting, positions [][]uint32) []fts.PositionalPosting {
	out := make([]fts.PositionalPosting, 0, len(docs))
	for i := range docs {
		var pos []uint32
		if i < len(positions) {
			pos = positions[i]
		}
		out = append(out, fts.PositionalPosting{Ord: docs[i].Ord, Positions: pos})
	}
	return out
}

func addMergedDoc(merged map[fts.DocOrd]fts.Posting, ord fts.DocOrd, count uint32) {
	ref, ok := merged[ord]
	if !ok {
		merged[ord] = fts.Posting{Ord: ord, Count: count}
		return
	}
	ref.Count += count
	merged[ord] = ref
}

func mergedDocsSlice(merged map[fts.DocOrd]fts.Posting) []fts.Posting {
	out := make([]fts.Posting, 0, len(merged))
	for _, doc := range merged {
		out = append(out, doc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ord < out[j].Ord })
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

var _ fts.Index = (*Index)(nil)
var _ fts.MutableIndex = (*Index)(nil)
var _ fts.PrefixIndex = (*Index)(nil)
var _ fts.PositionalIndex = (*Index)(nil)
var _ fts.MutablePositionalIndex = (*Index)(nil)
var _ posting.Exporter = (*Index)(nil)
