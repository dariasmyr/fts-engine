package trigram

import (
	"errors"
	"fmt"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"sync"
)

var ErrInvalidTrigramSize = errors.New("trigram must have exactly 3 characters")

type node struct {
	docs     map[fts.DocID]uint32
	children [26]*node
}

func newNode() *node {
	return &node{docs: make(map[fts.DocID]uint32)}
}

type Index struct {
	root *node
	mu   sync.RWMutex
}

func New() *Index {
	return &Index{root: newNode()}
}

func (t *Index) Insert(trigram string, docID fts.DocID) error {
	if len(trigram) != 3 {
		return ErrInvalidTrigramSize
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	n := t.root
	for i := 0; i < 3; i++ {
		idx := trigram[i] - 'a'
		if idx >= 26 {
			return fmt.Errorf("invalid character in trigram %v", trigram)
		}
		if n.children[idx] == nil {
			n.children[idx] = newNode()
		}
		n = n.children[idx]
	}
	n.docs[docID]++
	return nil
}

func (t *Index) Search(word string) ([]fts.DocRef, error) {
	if len(word) != 3 {
		return nil, ErrInvalidTrigramSize
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	n := t.root
	for i := 0; i < 3; i++ {
		idx := word[i] - 'a'
		if idx >= 26 {
			return nil, fmt.Errorf("invalid character in trigram %v", word)
		}
		if n.children[idx] == nil {
			return nil, nil
		}
		n = n.children[idx]
	}

	res := make([]fts.DocRef, 0, len(n.docs))
	for id, count := range n.docs {
		res = append(res, fts.DocRef{ID: id, Count: count})
	}
	return res, nil
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
		if len(n.docs) > 0 {
			s.Leaves++
		}
		if depth > s.MaxDepth {
			s.MaxDepth = depth
		}
		s.TotalDocs += len(n.docs)

		filled := 0
		for _, c := range n.children {
			if c != nil {
				filled++
			}
		}
		levelChildrenSum[depth] += filled
		levelNodeCount[depth]++
		s.TotalChildren += filled

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
