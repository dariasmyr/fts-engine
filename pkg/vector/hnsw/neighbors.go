package hnsw

import (
	"slices"
)

func (b *Builder) distanceNodes(a, c NodeOrdinal) float64 {
	aVector := b.vectorByNode(a)
	cVector := b.vectorByNode(c)
	return b.calculator.DistancePrepared(aVector, cVector)
}

func (b *Builder) vectorByNode(node NodeOrdinal) []float32 {
	ordinal := b.graph.nodes[node].vectorOrdinal
	start := int(ordinal) * b.calculator.Dimensions()
	return b.graph.values[start : start+b.calculator.Dimensions()]
}

func (b *Builder) selectNeighbors(owner NodeOrdinal, candidates []NodeOrdinal, limit int) []NodeOrdinal {
	ordered := b.orderNeighbors(owner, candidates)
	selected := make([]NodeOrdinal, 0, min(limit, len(ordered)))
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

func (b *Builder) orderNeighbors(owner NodeOrdinal, candidates []NodeOrdinal) []NodeOrdinal {
	seen := make(map[NodeOrdinal]struct{}, len(candidates))
	ordered := make([]NodeOrdinal, 0, len(candidates))
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
	slices.SortFunc(ordered, func(a, c NodeOrdinal) int {
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

func (b *Builder) addReverseLink(owner, neighbor NodeOrdinal, level int) {
	links := append(append([]NodeOrdinal(nil), b.graph.nodes[owner].links[level]...), neighbor)
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
