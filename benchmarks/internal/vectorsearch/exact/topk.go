package exact

import (
	"math"
	"slices"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type topK struct {
	limit int
	hits  []vector.Hit
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

func (t *topK) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if compareHits(t.hits[parent], t.hits[index]) >= 0 {
			return
		}
		t.hits[parent], t.hits[index] = t.hits[index], t.hits[parent]
		index = parent
	}
}

func (t *topK) siftDown(index int) {
	for {
		left := index*2 + 1
		if left >= len(t.hits) {
			return
		}
		worse := left
		right := left + 1
		if right < len(t.hits) && compareHits(t.hits[right], t.hits[left]) > 0 {
			worse = right
		}
		if compareHits(t.hits[index], t.hits[worse]) >= 0 {
			return
		}
		t.hits[index], t.hits[worse] = t.hits[worse], t.hits[index]
		index = worse
	}
}

func (t *topK) results() []vector.Hit {
	results := slices.Clone(t.hits)
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
