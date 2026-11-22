package exact

import (
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type topK struct {
	limit int
	// hits is a max-heap: hits[0] is the worst retained result. The heap stays
	// bounded by limit while Searcher scans the complete vector store.
	hits []vector.Hit
}

func newTopK(limit int) *topK {
	limit = max(0, limit)
	return &topK{limit: limit, hits: make([]vector.Hit, 0, limit)}
}

func (t *topK) add(hit vector.Hit) {
	if t.limit == 0 || math.IsNaN(hit.Distance) || math.IsInf(hit.Distance, 0) {
		return
	}
	if len(t.hits) < t.limit {
		t.hits = append(t.hits, hit)
		t.siftUp(len(t.hits) - 1)
		return
	}
	if compareHits(hit, t.hits[0]) >= 0 {
		return
	}
	t.hits[0] = hit
	t.siftDown(0)
}

func (t *topK) siftUp(child int) {
	for child > 0 {
		parent := heapParent(child)
		if !isWorse(t.hits[child], t.hits[parent]) {
			return
		}
		t.hits[parent], t.hits[child] = t.hits[child], t.hits[parent]
		child = parent
	}
}

func (t *topK) siftDown(parent int) {
	for {
		left := heapLeftChild(parent)
		if left >= len(t.hits) {
			return
		}
		worstChild := left
		right := left + 1
		if right < len(t.hits) && isWorse(t.hits[right], t.hits[left]) {
			worstChild = right
		}
		if !isWorse(t.hits[worstChild], t.hits[parent]) {
			return
		}
		t.hits[parent], t.hits[worstChild] = t.hits[worstChild], t.hits[parent]
		parent = worstChild
	}
}

// A complete binary tree is stored level by level in the slice:
//
//	     0
//	  1     2
//	3  4  5  6
//
// Consequently, children 1 and 2 have parent 0, children 3 and 4 have parent
// 1, and so on. Integer division implements exactly that grouping.
func heapParent(child int) int { return (child - 1) / 2 }

func heapLeftChild(parent int) int { return parent*2 + 1 }

func isWorse(a, b vector.Hit) bool { return compareHits(a, b) > 0 }

func (t *topK) results() []vector.Hit {
	// Heap order is only partial. Searcher is finished adding results, so sort
	// the retained K hits in place and transfer the slice to the Result.
	slices.SortFunc(t.hits, compareHits)
	return t.hits
}

func compareHits(a, b vector.Hit) int {
	if a.Distance < b.Distance {
		return -1
	}
	if a.Distance > b.Distance {
		return 1
	}
	if a.Ordinal < b.Ordinal {
		return -1
	}
	if a.Ordinal > b.Ordinal {
		return 1
	}
	return 0
}
