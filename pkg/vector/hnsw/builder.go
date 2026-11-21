package hnsw

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// builder constructs one HNSW graph. It is single-writer and is not searchable.
type builder struct {
	config    BuildConfig
	spec      vector.Spec
	matrix    *vector.Matrix
	graph     graphData
	rng       levelRNG
	workspace buildWorkspace
}

func newBuilder(
	config BuildConfig,
	spec vector.Spec,
	matrix *vector.Matrix,
	vectorCount int,
) *builder {
	return &builder{
		config: config,
		spec:   spec,
		matrix: matrix,
		graph: graphData{
			nodes: make([]mutableNode, 0, vectorCount),
		},
		rng: newLevelRNG(config.Seed),
		workspace: buildWorkspace{
			seenEpoch: make([]uint32, vectorCount),
		},
	}
}

// add registers the next already validated matrix row.
func (b *builder) add(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(b.graph.nodes) >= cap(b.graph.nodes) {
		return errCapacityExceeded
	}

	ordinal := nodeOrdinal(len(b.graph.nodes))
	level := b.rng.level(b.config.MaxNeighbors)
	b.graph.nodes = append(b.graph.nodes, mutableNode{
		level: level,
		// preallocate the slice of links for each level
		// we will fill the links later during the insert process
		links: make([][]buildNeighbor, int(level)+1),
	})

	if !b.graph.hasEntry {
		b.graph.entry = ordinal
		b.graph.hasEntry = true
		return nil
	}
	return b.insert(ctx, ordinal)
}

func (b *builder) freeze(ctx context.Context, config Config) (*Index, error) {
	reachability, err := validateLevel0Reachability(ctx, b.graph)
	if err != nil {
		return nil, err
	}
	if len(reachability.unreachableNodes) > 0 {
		if _, err := b.repairLevel0Connectivity(ctx, reachability); err != nil {
			return nil, err
		}
		final, err := validateLevel0Reachability(ctx, b.graph)
		if err != nil {
			return nil, err
		}
		if len(final.unreachableNodes) > 0 {
			return nil, errConnectivityRepair
		}
	}

	topology, err := packGraph(b.spec, b.config.info(), b.graph)
	if err != nil {
		return nil, err
	}
	return newIndex(topology, b.matrix, config)
}

func (b *builder) insert(ctx context.Context, newNode nodeOrdinal) error {
	newNodeLevel := int(b.graph.nodes[newNode].level)
	entryNode := b.graph.entry
	entryLevel := int(b.graph.nodes[entryNode].level)
	closestCandidate := candidate{
		node:     entryNode,
		distance: b.distanceNodes(newNode, entryNode),
		accepted: true,
	}

	// Descend through levels where the new node will not have links. The
	// closest node found on one level is the starting point for the next.
	for level := entryLevel; level > newNodeLevel; level-- {
		var err error
		closestCandidate, err = b.findClosestCandidateOnLevel(ctx, newNode, closestCandidate, level)
		if err != nil {
			return err
		}
	}

	// On levels shared with the existing graph, find candidate neighbors,
	// select actual links, and use those candidates to seed the next level.
	levelStartCandidates := append(b.workspace.levelStartCandidates[:0], closestCandidate)
	for level := min(newNodeLevel, entryLevel); level >= 0; level-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		nearestCandidatesOnLevel, err := b.findNearestCandidatesOnLevel(
			ctx,
			newNode,
			levelStartCandidates,
			b.config.EfConstruction,
			level,
		)
		if err != nil {
			return err
		}
		selection, err := b.selectNeighbors(
			ctx,
			nearestCandidatesOnLevel,
			b.config.MaxNeighbors,
			true,
			b.graph.nodes[newNode].links[level],
		)
		if err != nil {
			return err
		}
		selectedNeighbors := selection.neighbors
		b.graph.nodes[newNode].links[level] = selectedNeighbors
		b.setVerifiedPrefix(newNode, level, selection.verifiedCount)
		for i, neighbor := range selectedNeighbors {
			if err := periodicContextError(ctx, i); err != nil {
				return err
			}
			if err := b.addReverseLink(ctx, neighbor.node, newNode, level); err != nil {
				return err
			}
		}
		// At the next lower level, reuse both the nodes and their target-relative
		// distances as search starting points.
		if len(nearestCandidatesOnLevel) > 0 {
			levelStartCandidates = append(levelStartCandidates[:0], nearestCandidatesOnLevel...)
		}
		// reset the workspace for the next level
		b.workspace.nearestCandidates = nearestCandidatesOnLevel[:0]
	}
	// reset the workspace for the next insertion
	b.workspace.levelStartCandidates = levelStartCandidates[:0]
	if newNodeLevel > entryLevel {
		b.graph.entry = newNode
	}
	return ctx.Err()
}

// findClosestCandidateOnLevel performs greedy descent through a level above
// the new node's own level. The new node cannot have links there, so this only
// finds a closer entry point for the next lower level.
func (b *builder) findClosestCandidateOnLevel(
	ctx context.Context,
	targetNode nodeOrdinal,
	startCandidate candidate,
	level int,
) (candidate, error) {
	closestCandidate := startCandidate
	processedItems := 0
	for {
		if err := periodicContextError(ctx, processedItems); err != nil {
			return candidate{}, err
		}
		processedItems++
		closerCandidate := closestCandidate
		for _, neighbor := range b.graph.nodes[closestCandidate.node].links[level] {
			if err := periodicContextError(ctx, processedItems); err != nil {
				return candidate{}, err
			}
			processedItems++
			distance := b.distanceNodes(targetNode, neighbor.node)
			if distance < closestCandidate.distance &&
				(distance < closerCandidate.distance ||
					distance == closerCandidate.distance && neighbor.node < closerCandidate.node) {
				closerCandidate = candidate{
					node:     neighbor.node,
					distance: distance,
					accepted: true,
				}
			}
		}
		if closerCandidate.node == closestCandidate.node {
			return closestCandidate, nil
		}
		closestCandidate = closerCandidate
	}
}

// findNearestCandidatesOnLevel searches a level shared by the new node and the
// existing graph, from the new node's highest shared level down through level
// 0. It keeps up to candidateLimit nearest nodes for neighbor selection while
// expanding the nearest pending node first.
func (b *builder) findNearestCandidatesOnLevel(
	ctx context.Context,
	targetNode nodeOrdinal,
	startCandidates []candidate,
	candidateLimit int,
	level int,
) ([]candidate, error) {
	maxCandidates := min(candidateLimit, len(b.graph.nodes)-1)
	nearestCandidates := newResultHeapWithBuffer(maxCandidates, b.workspace.nearestCandidates)
	pendingCandidates := candidateHeap{
		items: prepareEmptyBuffer(b.workspace.pendingCandidates, min(maxCandidates, len(startCandidates))),
	}
	b.workspace.nextEpoch()

	considerCandidate := func(start candidate) {
		if start.node == targetNode || !b.workspace.markSeen(start.node) {
			return
		}
		candidate := candidate{
			node:     start.node,
			distance: start.distance,
			accepted: true,
		}
		pendingCandidates.Push(candidate)
		nearestCandidates.Add(candidate)
	}
	processedItems := 0
	// A candidate remains worth exploring while the result beam has space or
	// it is closer than the current worst result.
	addCandidateIfCompetitive := func(node nodeOrdinal, distance float64) {
		neighborCandidate := candidate{
			node:     node,
			distance: distance,
			accepted: true,
		}
		farthestNearestCandidate, hasNearestCandidate := nearestCandidates.Worst()
		isCandidateSetFull := nearestCandidates.Len() >= maxCandidates
		if !hasNearestCandidate || !isCandidateSetFull ||
			candidateCloser(neighborCandidate, farthestNearestCandidate) {
			pendingCandidates.Push(neighborCandidate)
			nearestCandidates.Add(neighborCandidate)
		}
	}
	for _, startCandidate := range startCandidates {
		if err := periodicContextError(ctx, processedItems); err != nil {
			return nil, err
		}
		processedItems++
		considerCandidate(startCandidate)
	}
	for pendingCandidates.Len() > 0 {
		if err := periodicContextError(ctx, processedItems); err != nil {
			return nil, err
		}
		processedItems++
		closestPendingCandidate, _ := pendingCandidates.Pop()
		farthestNearestCandidate, hasNearestCandidate := nearestCandidates.Worst()
		isCandidateSetFull := nearestCandidates.Len() >= maxCandidates
		if hasNearestCandidate && isCandidateSetFull &&
			candidateCloser(farthestNearestCandidate, closestPendingCandidate) {
			break
		}
		var neighborBatch [4]nodeOrdinal
		batchCount := 0
		for _, neighbor := range b.graph.nodes[closestPendingCandidate.node].links[level] {
			if err := periodicContextError(ctx, processedItems); err != nil {
				return nil, err
			}
			processedItems++
			if neighbor.node == targetNode || !b.workspace.markSeen(neighbor.node) {
				continue
			}
			// Batch only unique, unseen neighbors. This preserves the guarantee
			// that every node is evaluated at most once during this level search.
			neighborBatch[batchCount] = neighbor.node
			batchCount++
			if batchCount == len(neighborBatch) {
				// Calculate distances for the whole batch instead of one call per neighbor.
				distances := b.distanceNodesBatch(targetNode, neighborBatch, batchCount)
				// Apply results in link order so heap decisions remain identical to
				// the original scalar search.
				for i := range batchCount {
					addCandidateIfCompetitive(neighborBatch[i], distances[i])
				}
				batchCount = 0
			}
		}
		// Process the final one to three nodes without allocation. The batch
		// dispatcher uses scalar, batch x2, or batch x2 plus scalar respectively.
		if batchCount > 0 {
			distances := b.distanceNodesBatch(targetNode, neighborBatch, batchCount)
			for i := range batchCount {
				addCandidateIfCompetitive(neighborBatch[i], distances[i])
			}
		}
	}
	b.workspace.pendingCandidates = pendingCandidates.items[:0]
	return nearestCandidates.items, ctx.Err()
}
