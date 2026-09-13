package exactsearch

import (
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// TopK retains the best unique-row hits from an exact matrix scan.
type TopK struct {
	limit int
	hits  hitHeap
}

func NewTopK(limit int) *TopK {
	limit = max(0, limit)
	return &TopK{limit: limit, hits: make(hitHeap, 0, limit)}
}

func (t *TopK) Add(hit vector.Hit) {
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

func (t *TopK) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if compareHits(t.hits[parent], t.hits[index]) >= 0 {
			return
		}
		t.hits[parent], t.hits[index] = t.hits[index], t.hits[parent]
		index = parent
	}
}

func (t *TopK) siftDown(index int) {
	for {
		left := index*2 + 1
		if left >= len(t.hits) {
			return
		}
		worseChild := left
		right := left + 1
		if right < len(t.hits) && compareHits(t.hits[right], t.hits[left]) > 0 {
			worseChild = right
		}
		if compareHits(t.hits[index], t.hits[worseChild]) >= 0 {
			return
		}
		t.hits[index], t.hits[worseChild] = t.hits[worseChild], t.hits[index]
		index = worseChild
	}
}

func (t *TopK) Results() []vector.Hit {
	results := append([]vector.Hit(nil), t.hits...)
	slices.SortFunc(results, compareHits)
	return results
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

type hitHeap []vector.Hit
