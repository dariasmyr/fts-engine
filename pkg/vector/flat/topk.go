package flat

import (
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// exactTopK omits duplicate tracking because a flat scan offers every ordinal
// exactly once.
type exactTopK struct {
	limit int
	hits  exactHitHeap
}

func newExactTopK(limit int) *exactTopK {
	limit = max(0, limit)
	return &exactTopK{limit: limit, hits: make(exactHitHeap, 0, limit)}
}

func (t *exactTopK) Add(hit vector.Hit) {
	if t.limit == 0 || math.IsNaN(hit.Distance) || math.IsInf(hit.Distance, 0) {
		return
	}
	if len(t.hits) < t.limit {
		t.hits = append(t.hits, hit)
		t.siftUp(len(t.hits) - 1)
		return
	}
	if compareExactHits(hit, t.hits[0]) >= 0 {
		return
	}
	t.hits[0] = hit
	t.siftDown(0)
}

func (t *exactTopK) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if compareExactHits(t.hits[parent], t.hits[index]) >= 0 {
			return
		}
		t.hits[parent], t.hits[index] = t.hits[index], t.hits[parent]
		index = parent
	}
}

func (t *exactTopK) siftDown(index int) {
	for {
		left := index*2 + 1
		if left >= len(t.hits) {
			return
		}
		worseChild := left
		right := left + 1
		if right < len(t.hits) && compareExactHits(t.hits[right], t.hits[left]) > 0 {
			worseChild = right
		}
		if compareExactHits(t.hits[index], t.hits[worseChild]) >= 0 {
			return
		}
		t.hits[index], t.hits[worseChild] = t.hits[worseChild], t.hits[index]
		index = worseChild
	}
}

func (t *exactTopK) Results() []vector.Hit {
	results := append([]vector.Hit(nil), t.hits...)
	slices.SortFunc(results, compareExactHits)
	return results
}

func compareExactHits(a, b vector.Hit) int {
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

type exactHitHeap []vector.Hit
