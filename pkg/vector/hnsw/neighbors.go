package hnsw

import (
	"context"
	"slices"
)

func (b *builder) distanceNodes(a, c nodeOrdinal) float64 {
	aVector := b.vectorByNode(a)
	cVector := b.vectorByNode(c)
	return b.calculator.DistancePrepared(aVector, cVector)
}

func (b *builder) vectorByNode(node nodeOrdinal) []float32 {
	start := int(node) * b.calculator.Dimensions()
	return b.graph.values[start : start+b.calculator.Dimensions()]
}

func (b *builder) selectNeighbors(ctx context.Context, candidates []searchCandidate, limit int) ([]nodeOrdinal, error) {
	if err := b.orderNeighbors(ctx, candidates); err != nil {
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
		if diverse {
			selected = append(selected, candidate.node)
			if len(selected) == limit {
				return selected, ctx.Err()
			}
		} else {
			rejected = append(rejected, candidate.node)
		}
	}
	// Fill unused slots only after choosing diverse neighbors. This may
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

func (b *builder) orderNeighbors(ctx context.Context, candidates []searchCandidate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	return ctx.Err()
}

func (b *builder) addReverseLink(ctx context.Context, owner, neighbor nodeOrdinal, level int) error {
	links := b.graph.nodes[owner].links[level]
	candidates := resetSearchCandidates(b.workspace.neighbors, len(links)+1)
	for i, link := range links {
		if err := periodicContextError(ctx, i); err != nil {
			return err
		}
		candidates = append(candidates, searchCandidate{node: link, distance: b.distanceNodes(owner, link)})
	}
	candidates = append(candidates, searchCandidate{node: neighbor, distance: b.distanceNodes(owner, neighbor)})
	limit := b.config.MaxNeighbors
	if level == 0 {
		limit *= 2
	}
	// Reverse insertion can overflow an existing node even though the new node
	// selected only MaxNeighbors links, so prune relative to the existing owner.
	if len(candidates) > limit {
		var err error
		links, err = b.selectNeighbors(ctx, candidates, limit)
		if err != nil {
			return err
		}
	} else {
		if err := b.orderNeighbors(ctx, candidates); err != nil {
			return err
		}
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
	b.workspace.neighbors = candidates[:0]
	return ctx.Err()
}
