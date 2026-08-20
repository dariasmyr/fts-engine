package vector

import (
	"container/heap"
	"math"
	"slices"
)

// TopK keeps the best finite-distance hits up to a fixed limit. It is not safe
// for concurrent use.
type TopK struct {
	limit int
	hits  hitHeap
}

func NewTopK(limit int) *TopK {
	return &TopK{
		limit: max(0, limit),
		hits:  hitHeap{positions: make(map[Ordinal]int)},
	}
}

func (t *TopK) Len() int {
	if t == nil {
		return 0
	}
	return len(t.hits.items)
}

// Add considers hit for the bounded result set. It returns true when hit was
// retained. NaN and infinite distances are rejected.
func (t *TopK) Add(hit Hit) bool {
	if t == nil || t.limit == 0 || math.IsNaN(hit.Distance) || math.IsInf(hit.Distance, 0) {
		return false
	}
	if index, exists := t.hits.positions[hit.Ordinal]; exists {
		if !betterHit(hit, t.hits.items[index]) {
			return false
		}
		t.hits.items[index] = hit
		heap.Fix(&t.hits, index)
		return true
	}
	if len(t.hits.items) < t.limit {
		heap.Push(&t.hits, hit)
		return true
	}
	if !betterHit(hit, t.hits.items[0]) {
		return false
	}
	delete(t.hits.positions, t.hits.items[0].Ordinal)
	t.hits.items[0] = hit
	t.hits.positions[hit.Ordinal] = 0
	heap.Fix(&t.hits, 0)
	return true
}

// Results returns a copy ordered by ascending distance and then ascending
// ordinal.
func (t *TopK) Results() []Hit {
	if t == nil || len(t.hits.items) == 0 {
		return []Hit{}
	}
	results := append([]Hit(nil), t.hits.items...)
	slices.SortFunc(results, compareHits)
	return results
}

func compareHits(a, b Hit) int {
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

func betterHit(a, b Hit) bool { return compareHits(a, b) < 0 }

type hitHeap struct {
	items     []Hit
	positions map[Ordinal]int
}

func (h hitHeap) Len() int { return len(h.items) }

// Less reverses the normal hit order so the root is the worst retained hit.
func (h hitHeap) Less(i, j int) bool { return compareHits(h.items[i], h.items[j]) > 0 }

func (h hitHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.positions[h.items[i].Ordinal] = i
	h.positions[h.items[j].Ordinal] = j
}

func (h *hitHeap) Push(value any) {
	hit := value.(Hit)
	h.positions[hit.Ordinal] = len(h.items)
	h.items = append(h.items, hit)
}

func (h *hitHeap) Pop() any {
	last := len(h.items) - 1
	value := h.items[last]
	delete(h.positions, value.Ordinal)
	h.items[last] = Hit{}
	h.items = h.items[:last]
	return value
}
