package hnsw

import (
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func navigationBetter(a, b searchCandidate) bool {
	if a.distance != b.distance {
		return a.distance < b.distance
	}
	return a.node < b.node
}

func resultBetter(a, b searchCandidate) bool {
	if a.distance != b.distance {
		return a.distance < b.distance
	}
	if a.vectorOrdinal != b.vectorOrdinal {
		return a.vectorOrdinal < b.vectorOrdinal
	}
	return a.node < b.node
}

func resultWorse(a, b searchCandidate) bool { return resultBetter(b, a) }

type candidateHeap struct {
	items []searchCandidate
}

func (h *candidateHeap) Len() int { return len(h.items) }

func (h *candidateHeap) Push(value searchCandidate) {
	h.items = append(h.items, value)
	for child := len(h.items) - 1; child > 0; {
		parent := (child - 1) / 2
		if !navigationBetter(h.items[child], h.items[parent]) {
			break
		}
		h.items[parent], h.items[child] = h.items[child], h.items[parent]
		child = parent
	}
}

func (h *candidateHeap) Pop() (searchCandidate, bool) {
	if len(h.items) == 0 {
		return searchCandidate{}, false
	}
	result := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items = h.items[:last]
	for parent := 0; ; {
		left := parent*2 + 1
		if left >= len(h.items) {
			break
		}
		best := left
		right := left + 1
		if right < len(h.items) && navigationBetter(h.items[right], h.items[left]) {
			best = right
		}
		if !navigationBetter(h.items[best], h.items[parent]) {
			break
		}
		h.items[parent], h.items[best] = h.items[best], h.items[parent]
		parent = best
	}
	return result, true
}

type resultHeap struct {
	items    []searchCandidate
	capacity int
}

func newResultHeap(capacity int) resultHeap {
	return resultHeap{items: make([]searchCandidate, 0, capacity), capacity: capacity}
}

func (h *resultHeap) Len() int { return len(h.items) }

func (h *resultHeap) Worst() (searchCandidate, bool) {
	if len(h.items) == 0 {
		return searchCandidate{}, false
	}
	return h.items[0], true
}

func (h *resultHeap) Add(value searchCandidate) {
	if h.capacity == 0 || math.IsNaN(value.distance) || math.IsInf(value.distance, 0) {
		return
	}
	if len(h.items) == h.capacity {
		if !resultBetter(value, h.items[0]) {
			return
		}
		h.items[0] = value
		h.siftDown(0)
		return
	}
	h.items = append(h.items, value)
	for child := len(h.items) - 1; child > 0; {
		parent := (child - 1) / 2
		if !resultWorse(h.items[child], h.items[parent]) {
			break
		}
		h.items[parent], h.items[child] = h.items[child], h.items[parent]
		child = parent
	}
}

func (h *resultHeap) Results(k int) []vector.Hit {
	items := h.Candidates()
	if len(items) > k {
		items = items[:k]
	}
	hits := make([]vector.Hit, len(items))
	for i, item := range items {
		hits[i] = vector.Hit{Ordinal: item.vectorOrdinal, Distance: item.distance}
	}
	return hits
}

func (h *resultHeap) Candidates() []searchCandidate {
	items := append([]searchCandidate(nil), h.items...)
	slices.SortFunc(items, func(a, b searchCandidate) int {
		if resultBetter(a, b) {
			return -1
		}
		if resultBetter(b, a) {
			return 1
		}
		return 0
	})
	return items
}

func (h *resultHeap) siftDown(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h.items) {
			return
		}
		worst := left
		right := left + 1
		if right < len(h.items) && resultWorse(h.items[right], h.items[left]) {
			worst = right
		}
		if !resultWorse(h.items[worst], h.items[parent]) {
			return
		}
		h.items[parent], h.items[worst] = h.items[worst], h.items[parent]
		parent = worst
	}
}
