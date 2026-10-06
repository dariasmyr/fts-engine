package hnsw

import (
	"slices"
)

func (b *builder) distanceNodes(a, c nodeOrdinal) float64 {
	aVector := b.vectorByNode(a)
	cVector := b.vectorByNode(c)
	return b.calculator.DistancePrepared(aVector, cVector)
}

func (b *builder) vectorByNode(node nodeOrdinal) []float32 {
	ordinal := b.graph.nodes[node].vectorOrdinal
	start := int(ordinal) * b.calculator.Dimensions()
	return b.graph.values[start : start+b.calculator.Dimensions()]
}

func (b *builder) selectNeighbors(candidates []searchCandidate, limit int) []nodeOrdinal {
	b.orderNeighbors(candidates)
	selected := make([]nodeOrdinal, 0, min(limit, len(candidates)))
	// Reject candidates already represented by a selected, closer neighbor. This
	// preserves links into different regions instead of only the nearest cluster.
	for _, candidate := range candidates {
		diverse := true
		for _, existing := range selected {
			if b.distanceNodes(candidate.node, existing) < candidate.distance {
				diverse = false
				break
			}
		}
		if diverse {
			selected = append(selected, candidate.node)
			if len(selected) == limit {
				break
			}
		}
	}
	return selected
}

func (b *builder) orderNeighbors(candidates []searchCandidate) {
	slices.SortFunc(candidates, func(a, c searchCandidate) int {
		if a.distance < c.distance {
			return -1
		}
		if a.distance > c.distance {
			return 1
		}
		if a.node < c.node {
			return -1
		}
		if a.node > c.node {
			return 1
		}
		return 0
	})
}

func (b *builder) addReverseLink(owner, neighbor nodeOrdinal, level int) {
	links := b.graph.nodes[owner].links[level]
	candidates := resetSearchCandidates(b.neighborWork, len(links)+1)
	for _, link := range links {
		candidates = append(candidates, searchCandidate{node: link, distance: b.distanceNodes(owner, link)})
	}
	candidates = append(candidates, searchCandidate{node: neighbor, distance: b.distanceNodes(owner, neighbor)})
	limit := b.buildInfo.neighborLimit(level)
	// Reverse insertion can overflow an existing node even though the new node
	// selected only MaxNeighbors links, so prune relative to the existing owner.
	if len(candidates) > limit {
		links = b.selectNeighbors(candidates, limit)
	} else {
		b.orderNeighbors(candidates)
		if cap(links) < len(candidates) {
			links = make([]nodeOrdinal, len(candidates))
		} else {
			links = links[:len(candidates)]
		}
		for i := range candidates {
			links[i] = candidates[i].node
		}
	}
	b.graph.nodes[owner].links[level] = links
	b.neighborWork = candidates[:0]
}
