package hnsw

import (
	"context"
	"slices"
)

const invalidNodeOrdinal = ^nodeOrdinal(0)

type level0Reachability struct {
	isReachable      []bool
	reachedFrom      []nodeOrdinal
	unreachableNodes []nodeOrdinal
	reachableCount   int
}

type connectivityRepairStats struct {
	initiallyUnreachable int
	addedLinks           int
	replacedLinks        int
}

// level0LinkChange is a prepared update to one node's outgoing arrows. For
// example, to add C → B it contains fromNode=C and C's complete updated list.
type level0LinkChange struct {
	fromNode        nodeOrdinal
	updatedLinks    []nodeOrdinal
	replacesOldLink bool
}

// validateLevel0Reachability follows level-0 arrows from the entry node:
//
//	entry A → B → C     D → A
//
// A, B, and C are reachable. D is not: its arrow points in the wrong
// direction. reachedFrom records the first arrow used to reach each node. For
// example, reachedFrom[C] == B means that preserving B → C preserves the
// known path to C while repair changes other links.
func validateLevel0Reachability(ctx context.Context, graph graphData) (level0Reachability, error) {
	if err := ctx.Err(); err != nil {
		return level0Reachability{}, err
	}
	nodes := len(graph.nodes)
	state := level0Reachability{
		isReachable: make([]bool, nodes),
		reachedFrom: make([]nodeOrdinal, nodes),
	}
	for i := range state.reachedFrom {
		state.reachedFrom[i] = invalidNodeOrdinal
	}
	if nodes == 0 {
		if graph.hasEntry {
			return level0Reachability{}, errInvalidGraph
		}
		return state, nil
	}
	if !graph.hasEntry || uint64(graph.entry) >= uint64(nodes) {
		return level0Reachability{}, errInvalidGraph
	}

	queue := make([]nodeOrdinal, 1, nodes)
	queue[0] = graph.entry
	state.isReachable[graph.entry] = true
	state.reachedFrom[graph.entry] = graph.entry
	state.reachableCount = 1
	work := 0
	for head := 0; head < len(queue); head++ {
		if err := periodicContextError(ctx, work); err != nil {
			return level0Reachability{}, err
		}
		work++
		node := queue[head]
		if len(graph.nodes[node].links) == 0 {
			return level0Reachability{}, errInvalidGraph
		}
		for _, neighbor := range graph.nodes[node].links[0] {
			if err := periodicContextError(ctx, work); err != nil {
				return level0Reachability{}, err
			}
			work++
			if uint64(neighbor) >= uint64(nodes) || neighbor == node {
				return level0Reachability{}, errInvalidGraph
			}
			if state.isReachable[neighbor] {
				continue
			}
			state.isReachable[neighbor] = true
			state.reachedFrom[neighbor] = node
			state.reachableCount++
			queue = append(queue, neighbor)
		}
	}
	state.unreachableNodes = make([]nodeOrdinal, 0, nodes-state.reachableCount)
	for node, isReachable := range state.isReachable {
		if err := periodicContextError(ctx, work); err != nil {
			return level0Reachability{}, err
		}
		work++
		if !isReachable {
			state.unreachableNodes = append(state.unreachableNodes, nodeOrdinal(node))
		}
	}
	return state, ctx.Err()
}

// repairLevel0Connectivity keeps adding reachable → unreachable arrows until
// every node can be reached from entry. It never publishes partial work:
// freeze validates the finished graph again before packing it.
func (b *builder) repairLevel0Connectivity(
	ctx context.Context,
	state level0Reachability,
) (connectivityRepairStats, error) {
	stats := connectivityRepairStats{initiallyUnreachable: len(state.unreachableNodes)}
	if stats.initiallyUnreachable == 0 {
		return stats, ctx.Err()
	}
	if len(b.graph.nodes) == 0 || !b.graph.hasEntry || state.reachableCount == 0 {
		return stats, errConnectivityRepair
	}

	// Each iteration adds an arrow from the reachable side to one unreachable
	// node. Following that node's existing arrows may make its whole component
	// reachable at once:
	//
	//	entry A → C     D → E → F
	//	       add C → D
	//	entry A → C → D → E → F
	nextUnreachableIndex := 0
	for state.reachableCount < len(b.graph.nodes) {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		for nextUnreachableIndex < len(state.isReachable) && state.isReachable[nextUnreachableIndex] {
			nextUnreachableIndex++
		}
		if nextUnreachableIndex == len(state.isReachable) {
			return stats, errConnectivityRepair
		}
		unreachableNode := nodeOrdinal(nextUnreachableIndex)
		linkChange, err := b.findLinkToUnreachableNode(
			ctx,
			unreachableNode,
			state,
		)
		if err != nil {
			return stats, err
		}

		reachableNode := linkChange.fromNode
		previousReachableNodeLinks := b.graph.nodes[reachableNode].links[0]
		b.graph.nodes[reachableNode].links[0] = linkChange.updatedLinks
		previousUnreachableNodeLinks := b.graph.nodes[unreachableNode].links[0]
		addedReverseLink, err := b.addReverseRepairLinkIfSpace(
			ctx,
			unreachableNode,
			reachableNode,
		)
		if err != nil {
			b.graph.nodes[reachableNode].links[0] = previousReachableNodeLinks
			return stats, err
		}

		reachableBefore := state.reachableCount
		newlyReachableNodes, err := b.markNodesReachableFrom(
			ctx,
			&state,
			unreachableNode,
			reachableNode,
		)
		if err != nil || state.reachableCount <= reachableBefore {
			b.graph.nodes[reachableNode].links[0] = previousReachableNodeLinks
			if addedReverseLink {
				b.graph.nodes[unreachableNode].links[0] = previousUnreachableNodeLinks
			}
			undoReachabilityMarks(&state, newlyReachableNodes)
			if err != nil {
				return stats, err
			}
			return stats, errConnectivityRepair
		}
		stats.addedLinks++
		if linkChange.replacesOldLink {
			stats.replacedLinks++
		}
	}
	return stats, ctx.Err()
}

// findLinkToUnreachableNode chooses the reachable end of a new arrow:
//
//	reachableNode → unreachableNode
//
// It first uses the normal HNSW level search to prefer nearby vectors. The
// linear scan is only a fallback when those nearby nodes are full and cannot
// safely replace a link.
func (b *builder) findLinkToUnreachableNode(
	ctx context.Context,
	unreachableNode nodeOrdinal,
	state level0Reachability,
) (level0LinkChange, error) {
	reachableCandidates, err := b.findNearestCandidatesOnLevel(
		ctx,
		unreachableNode,
		[]nodeOrdinal{b.graph.entry},
		b.config.EfConstruction,
		0,
	)
	if err != nil {
		return level0LinkChange{}, err
	}
	defer func() {
		b.workspace.nearestCandidates = reachableCandidates[:0]
	}()
	if err := b.sortNeighbors(ctx, reachableCandidates); err != nil {
		return level0LinkChange{}, err
	}
	for _, candidate := range reachableCandidates {
		if !state.isReachable[candidate.node] {
			continue
		}
		linkChange, canConnect, err := b.planLinkToUnreachableNode(
			ctx,
			candidate.node,
			unreachableNode,
			state,
			false,
		)
		if err != nil {
			return level0LinkChange{}, err
		}
		if canConnect {
			return linkChange, nil
		}
	}

	closestReachableNode, err := b.findClosestReachableNodeWithLinkSpace(
		ctx,
		unreachableNode,
		state,
	)
	if err != nil {
		return level0LinkChange{}, err
	}
	linkChange, canConnect, err := b.planLinkToUnreachableNode(
		ctx,
		closestReachableNode,
		unreachableNode,
		state,
		true,
	)
	if err != nil {
		return level0LinkChange{}, err
	}
	if !canConnect {
		return level0LinkChange{}, errConnectivityRepair
	}
	return linkChange, nil
}

// findClosestReachableNodeWithLinkSpace is the exhaustive fallback. A node is
// usable when it either has a free level-0 slot or has an old arrow that can be
// replaced without losing any currently reachable node.
func (b *builder) findClosestReachableNodeWithLinkSpace(
	ctx context.Context,
	unreachableNode nodeOrdinal,
	state level0Reachability,
) (nodeOrdinal, error) {
	closestReachableNode := invalidNodeOrdinal
	closestDistance := 0.0
	for node, isReachable := range state.isReachable {
		if err := periodicContextError(ctx, node); err != nil {
			return 0, err
		}
		if !isReachable {
			continue
		}
		reachableNode := nodeOrdinal(node)
		if !b.canAddOrReplaceLink(reachableNode, state) {
			continue
		}
		distance := b.distanceNodes(unreachableNode, reachableNode)
		if closestReachableNode == invalidNodeOrdinal || distance < closestDistance ||
			distance == closestDistance && reachableNode < closestReachableNode {
			closestReachableNode = reachableNode
			closestDistance = distance
		}
	}
	if closestReachableNode == invalidNodeOrdinal {
		return 0, errConnectivityRepair
	}
	return closestReachableNode, nil
}

// planLinkToUnreachableNode prepares the new level-0 links for:
//
//	reachableNode → unreachableNode
//
// If reachableNode has space, the arrow is appended. If it is full, normal
// HNSW neighbor selection gets the first chance to choose which arrow to drop.
// During the exhaustive fallback, allowDirectSafeReplacement permits replacing
// the farthest arrow that is not needed by the known paths from entry.
func (b *builder) planLinkToUnreachableNode(
	ctx context.Context,
	reachableNode, unreachableNode nodeOrdinal,
	state level0Reachability,
	allowDirectSafeReplacement bool,
) (level0LinkChange, bool, error) {
	links := b.graph.nodes[reachableNode].links[0]
	limit := b.config.MaxNeighbors * 2
	if limit <= 0 {
		return level0LinkChange{}, false, nil
	}
	if len(links) > limit || slices.Contains(links, unreachableNode) {
		return level0LinkChange{}, false, errInvalidGraph
	}
	if len(links) < limit {
		updatedLinks := append([]nodeOrdinal(nil), links...)
		updatedLinks = append(updatedLinks, unreachableNode)
		if err := b.sortNodeLinksByDistance(ctx, reachableNode, updatedLinks); err != nil {
			return level0LinkChange{}, false, err
		}
		return level0LinkChange{fromNode: reachableNode, updatedLinks: updatedLinks}, true, nil
	}

	if !allowDirectSafeReplacement {
		selectionCandidates := make([]candidate, 0, len(links)+1)
		for _, neighbor := range links {
			selectionCandidates = append(selectionCandidates, candidate{
				node: neighbor, distance: b.distanceNodes(reachableNode, neighbor),
			})
		}
		selectionCandidates = append(selectionCandidates, candidate{
			node: unreachableNode, distance: b.distanceNodes(reachableNode, unreachableNode),
		})
		selectedLinks, err := b.selectClosestNeighbors(ctx, selectionCandidates, limit)
		if err != nil {
			return level0LinkChange{}, false, err
		}
		if !slices.Contains(selectedLinks, unreachableNode) {
			return level0LinkChange{}, false, nil
		}
		for _, previousNeighbor := range links {
			wasRemoved := !slices.Contains(selectedLinks, previousNeighbor)
			isNeededForKnownPath := state.reachedFrom[previousNeighbor] == reachableNode
			if wasRemoved && isNeededForKnownPath {
				return level0LinkChange{}, false, nil
			}
		}
		return level0LinkChange{
			fromNode:        reachableNode,
			updatedLinks:    selectedLinks,
			replacesOldLink: true,
		}, true, nil
	}

	replaceIndex, found := b.findReplaceableLink(reachableNode, state)
	if !found {
		return level0LinkChange{}, false, nil
	}
	updatedLinks := append([]nodeOrdinal(nil), links...)
	updatedLinks[replaceIndex] = unreachableNode
	if err := b.sortNodeLinksByDistance(ctx, reachableNode, updatedLinks); err != nil {
		return level0LinkChange{}, false, err
	}
	return level0LinkChange{
		fromNode:        reachableNode,
		updatedLinks:    updatedLinks,
		replacesOldLink: true,
	}, true, nil
}

// canAddOrReplaceLink reports whether reachableNode can provide the left side
// of reachableNode → unreachableNode without exceeding the level-0 limit.
func (b *builder) canAddOrReplaceLink(reachableNode nodeOrdinal, state level0Reachability) bool {
	links := b.graph.nodes[reachableNode].links[0]
	limit := b.config.MaxNeighbors * 2
	if len(links) < limit {
		return true
	}
	if len(links) > limit {
		return false
	}
	_, found := b.findReplaceableLink(reachableNode, state)
	return found
}

// findReplaceableLink returns the farthest arrow that is not part of the
// recorded paths from entry. For example:
//
//	entry A → C → E
//	      A → D
//	      C → D     // removable: A → D remains
//
// C → E is not removable when reachedFrom[E] == C, because it is the recorded
// path to E. C → D is removable when reachedFrom[D] == A.
func (b *builder) findReplaceableLink(
	reachableNode nodeOrdinal,
	state level0Reachability,
) (int, bool) {
	links := b.graph.nodes[reachableNode].links[0]
	replaceIndex := -1
	farthestDistance := 0.0
	for i, neighbor := range links {
		if state.reachedFrom[neighbor] == reachableNode {
			continue
		}
		distance := b.distanceNodes(reachableNode, neighbor)
		if replaceIndex < 0 || distance > farthestDistance ||
			distance == farthestDistance && neighbor > links[replaceIndex] {
			replaceIndex = i
			farthestDistance = distance
		}
	}
	return replaceIndex, replaceIndex >= 0
}

func (b *builder) sortNodeLinksByDistance(
	ctx context.Context,
	node nodeOrdinal,
	links []nodeOrdinal,
) error {
	candidates := make([]candidate, 0, len(links))
	for _, neighbor := range links {
		candidates = append(candidates, candidate{
			node: neighbor, distance: b.distanceNodes(node, neighbor),
		})
	}
	if err := b.sortNeighbors(ctx, candidates); err != nil {
		return err
	}
	for i := range candidates {
		links[i] = candidates[i].node
	}
	return ctx.Err()
}

// addReverseRepairLinkIfSpace optionally turns:
//
//	reachableNode → newlyReachableNode
//
// into a two-way connection. The forward arrow above restores reachability;
// the reverse arrow is only a navigation improvement, so it is skipped rather
// than pruning another link when newlyReachableNode is full.
func (b *builder) addReverseRepairLinkIfSpace(
	ctx context.Context,
	newlyReachableNode, reachableNode nodeOrdinal,
) (bool, error) {
	links := b.graph.nodes[newlyReachableNode].links[0]
	if slices.Contains(links, reachableNode) || len(links) >= b.config.MaxNeighbors*2 {
		return false, nil
	}
	updated := append([]nodeOrdinal(nil), links...)
	updated = append(updated, reachableNode)
	if err := b.sortNodeLinksByDistance(ctx, newlyReachableNode, updated); err != nil {
		return false, err
	}
	b.graph.nodes[newlyReachableNode].links[0] = updated
	return true, nil
}

// markNodesReachableFrom follows existing arrows from startNode. One repair
// link may therefore restore a whole component:
//
//	before: entry A → C     D → E → F
//	after:  entry A → C → D → E → F
func (b *builder) markNodesReachableFrom(
	ctx context.Context,
	state *level0Reachability,
	startNode, reachedFrom nodeOrdinal,
) ([]nodeOrdinal, error) {
	if state.isReachable[startNode] {
		return nil, nil
	}
	newlyReachableNodes := make([]nodeOrdinal, 1)
	newlyReachableNodes[0] = startNode
	state.isReachable[startNode] = true
	state.reachedFrom[startNode] = reachedFrom
	state.reachableCount++
	work := 0
	for head := 0; head < len(newlyReachableNodes); head++ {
		if err := periodicContextError(ctx, work); err != nil {
			return newlyReachableNodes, err
		}
		work++
		node := newlyReachableNodes[head]
		for _, neighbor := range b.graph.nodes[node].links[0] {
			if err := periodicContextError(ctx, work); err != nil {
				return newlyReachableNodes, err
			}
			work++
			if uint64(neighbor) >= uint64(len(b.graph.nodes)) || neighbor == node {
				return newlyReachableNodes, errInvalidGraph
			}
			if state.isReachable[neighbor] {
				continue
			}
			state.isReachable[neighbor] = true
			state.reachedFrom[neighbor] = node
			state.reachableCount++
			newlyReachableNodes = append(newlyReachableNodes, neighbor)
		}
	}
	return newlyReachableNodes, ctx.Err()
}

func undoReachabilityMarks(state *level0Reachability, nodes []nodeOrdinal) {
	for _, node := range nodes {
		state.isReachable[node] = false
		state.reachedFrom[node] = invalidNodeOrdinal
		state.reachableCount--
	}
}
