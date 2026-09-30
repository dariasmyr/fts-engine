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

func (b *builder) selectNeighbors(owner nodeOrdinal, candidates []nodeOrdinal, limit int) []nodeOrdinal {
	ordered := b.orderNeighbors(owner, candidates)
	selected := make([]nodeOrdinal, 0, min(limit, len(ordered)))
	// Reject candidates already represented by a selected, closer neighbor. This
	// preserves links into different regions instead of only the nearest cluster.
	for _, candidate := range ordered {
		ownerDistance := b.distanceNodes(owner, candidate)
		diverse := true
		for _, existing := range selected {
			if b.distanceNodes(candidate, existing) < ownerDistance {
				diverse = false
				break
			}
		}
		if diverse {
			selected = append(selected, candidate)
			if len(selected) == limit {
				break
			}
		}
	}
	return selected
}

func (b *builder) orderNeighbors(owner nodeOrdinal, candidates []nodeOrdinal) []nodeOrdinal {
	seen := make(map[nodeOrdinal]struct{}, len(candidates))
	ordered := make([]nodeOrdinal, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == owner {
			continue
		}
		if _, duplicate := seen[candidate]; duplicate {
			continue
		}
		seen[candidate] = struct{}{}
		ordered = append(ordered, candidate)
	}
	slices.SortFunc(ordered, func(a, c nodeOrdinal) int {
		aDistance := b.distanceNodes(owner, a)
		cDistance := b.distanceNodes(owner, c)
		if aDistance < cDistance {
			return -1
		}
		if aDistance > cDistance {
			return 1
		}
		if a < c {
			return -1
		}
		if a > c {
			return 1
		}
		return 0
	})
	return ordered
}

func (b *builder) addReverseLink(owner, neighbor nodeOrdinal, level int) {
	links := append(append([]nodeOrdinal(nil), b.graph.nodes[owner].links[level]...), neighbor)
	limit := b.buildInfo.neighborLimit(level)
	// Reverse insertion can overflow an existing node even though the new node
	// selected only MaxNeighbors links, so prune relative to the existing owner.
	if len(links) > limit {
		links = b.selectNeighbors(owner, links, limit)
	} else {
		links = b.orderNeighbors(owner, links)
	}
	b.graph.nodes[owner].links[level] = links
}
