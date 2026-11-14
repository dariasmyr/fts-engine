package hnsw

import (
	"context"
	"slices"
)

func (b *builder) distanceNodes(a, c nodeOrdinal) float64 {
	aVector := b.vectorByNode(a)
	cVector := b.vectorByNode(c)
	return b.space.DistancePrepared(aVector, cVector)
}

func (b *builder) vectorByNode(node nodeOrdinal) []float32 {
	start := int(node) * b.space.Dimensions()
	return b.graph.values[start : start+b.space.Dimensions()]
}

func (b *builder) selectClosestNeighbors(ctx context.Context, candidates []candidate, limit int) ([]nodeOrdinal, error) {
	if err := b.sortNeighbors(ctx, candidates); err != nil {
		return nil, err
	}
	selected := make([]nodeOrdinal, 0, min(limit, len(candidates)))
	rejected := make([]nodeOrdinal, 0, min(limit, len(candidates)))
	// Reject candidates already represented by a selected, closer neighbor. This
	// preserves links into different regions instead of only the nearest cluster.
	work := 0
	for _, candidate := range candidates {
		if err := periodicContextError(ctx, work); err != nil {
			return nil, err
		}
		work++

		// diverce means that the candidate is not too close to any already selected neighbor, so we can add it to the selected list.
		diverse := true
		for _, existing := range selected {
			if err := periodicContextError(ctx, work); err != nil {
				return nil, err
			}
			work++
			if b.distanceNodes(candidate.node, existing) < candidate.distance {
				diverse = false
				break
			}
		}
		// If the candidate is not too close to any already selected neighbor,
		// we add it to the selected list. If not, we add it to the rejected list.
		if diverse {
			selected = append(selected, candidate.node)
			if len(selected) == limit {
				return selected, ctx.Err()
			}
		} else {
			rejected = append(rejected, candidate.node)
		}
	}
	// fill unused slots only after choosing diverse neighbors. This may
	// increase graph degree and recall, at the cost of more traversed links.
	for i, node := range rejected {
		if err := periodicContextError(ctx, i); err != nil {
			return nil, err
		}
		if len(selected) == limit {
			break
		}
		selected = append(selected, node)
	}
	return selected, ctx.Err()
}

func (b *builder) sortNeighbors(ctx context.Context, candidates []candidate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// sort candidates by distance, then by node ordinal
	slices.SortFunc(candidates, func(a, c candidate) int {
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
	return ctx.Err()
}

// addReverseLink adds existingNode → newNode. If existingNode is full, its
// links are selected again using distances from existingNode.
func (b *builder) addReverseLink(ctx context.Context, existingNode, newNode nodeOrdinal, level int) error {
	links := b.graph.nodes[existingNode].links[level]
	candidates := prepareCandidateBuffer(b.workspace.reverseLinkCandidates, len(links)+1)
	for i, link := range links {
		if err := periodicContextError(ctx, i); err != nil {
			return err
		}
		candidates = append(candidates, candidate{node: link, distance: b.distanceNodes(existingNode, link)})
	}
	candidates = append(candidates, candidate{node: newNode, distance: b.distanceNodes(existingNode, newNode)})
	// the maximum number of links allowed for a node is determined by the level
	// base level (level 0) allows for twice as many links as higher levels
	limit := b.config.MaxNeighbors
	if level == 0 {
		limit *= 2
	}
	closestNeighbors := links
	// if the number of candidates exceeds the limit, we select the closest neighbors
	if len(candidates) > limit {
		var err error
		closestNeighbors, err = b.selectClosestNeighbors(ctx, candidates, limit)
		if err != nil {
			return err
		}
	} else {
		// if the number of candidates is within the limit, we sort them and keep all
		if err := b.sortNeighbors(ctx, candidates); err != nil {
			return err
		}
		if cap(closestNeighbors) < len(candidates) {
			closestNeighbors = make([]nodeOrdinal, len(candidates))
		} else {
			closestNeighbors = closestNeighbors[:len(candidates)]
		}
		// fill the closestNeighbors slice with the node ordinals from the candidates
		for i := range candidates {
			closestNeighbors[i] = candidates[i].node
		}
	}
	b.graph.nodes[existingNode].links[level] = closestNeighbors
	b.workspace.reverseLinkCandidates = candidates[:0]
	return ctx.Err()
}
