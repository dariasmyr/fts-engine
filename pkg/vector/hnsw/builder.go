package hnsw

import (
	"context"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// builder constructs one HNSW graph. It is single-writer and is not searchable.
type builder struct {
	config     BuildConfig
	calculator vector.Calculator
	graph      graphData
	rng        levelRNG
	workspace  buildWorkspace
}

func newBuilder(config BuildConfig, calculator vector.Calculator, vectorCount, components int) *builder {
	return &builder{
		config:     config,
		calculator: calculator,
		graph: graphData{
			values: make([]float32, components),
			nodes:  make([]mutableNode, 0, vectorCount),
		},
		rng: newLevelRNG(config.Seed),
		workspace: buildWorkspace{
			seenEpoch: make([]uint32, vectorCount),
		},
	}
}

// add validates and copies the next prepared row. Its node ordinal is the
// source row ordinal, preserving the package-wide node == vector invariant.
func (b *builder) add(ctx context.Context, prepared []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.calculator.ValidatePrepared(prepared); err != nil {
		return err
	}
	if len(b.graph.nodes) >= cap(b.graph.nodes) {
		return errCapacityExceeded
	}

	node := nodeOrdinal(len(b.graph.nodes))
	level := b.rng.level(b.config.MaxNeighbors)
	start := int(node) * b.calculator.Dimensions()
	copy(b.graph.values[start:start+b.calculator.Dimensions()], prepared)
	b.graph.nodes = append(b.graph.nodes, mutableNode{
		level: level,
		links: make([][]nodeOrdinal, int(level)+1),
	})

	if !b.graph.hasEntry {
		b.graph.entry = node
		b.graph.hasEntry = true
		return nil
	}
	return b.insert(ctx, node)
}

func (b *builder) freeze(source vector.PreparedVectorStore, search SearchConfig) (*Index, error) {
	topology, err := packGraph(b.calculator, b.config.info(), b.graph)
	if err != nil {
		return nil, err
	}
	return newIndex(topology, source, search)
}

func (b *builder) insert(ctx context.Context, node nodeOrdinal) error {
	newLevel := int(b.graph.nodes[node].level)
	oldEntry := b.graph.entry
	oldMaxLevel := int(b.graph.nodes[oldEntry].level)
	current := oldEntry
	for level := oldMaxLevel; level > newLevel; level-- {
		var err error
		current, err = b.greedyBuild(ctx, node, current, level)
		if err != nil {
			return err
		}
	}

	entries := append(b.workspace.entries[:0], current)
	for level := min(newLevel, oldMaxLevel); level >= 0; level-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidates, err := b.searchLayer(ctx, node, entries, b.config.EfConstruction, level)
		if err != nil {
			return err
		}
		selected, err := b.selectNeighbors(ctx, candidates, b.config.MaxNeighbors)
		if err != nil {
			return err
		}
		b.graph.nodes[node].links[level] = selected
		for i, neighbor := range selected {
			if err := periodicContextError(ctx, i); err != nil {
				return err
			}
			if err := b.addReverseLink(ctx, neighbor, node, level); err != nil {
				return err
			}
		}
		if len(candidates) > 0 {
			entries = entries[:0]
			for _, candidate := range candidates {
				entries = append(entries, candidate.node)
			}
		}
		b.workspace.results = candidates[:0]
	}
	b.workspace.entries = entries[:0]
	if newLevel > oldMaxLevel {
		b.graph.entry = node
	}
	return ctx.Err()
}

func (b *builder) greedyBuild(ctx context.Context, queryNode, current nodeOrdinal, level int) (nodeOrdinal, error) {
	currentDistance := b.distanceNodes(queryNode, current)
	work := 0
	for {
		if err := periodicContextError(ctx, work); err != nil {
			return 0, err
		}
		work++
		best := current
		bestDistance := currentDistance
		for _, neighbor := range b.graph.nodes[current].links[level] {
			if err := periodicContextError(ctx, work); err != nil {
				return 0, err
			}
			work++
			distance := b.distanceNodes(queryNode, neighbor)
			if distance < currentDistance && (distance < bestDistance || distance == bestDistance && neighbor < best) {
				best = neighbor
				bestDistance = distance
			}
		}
		if best == current {
			return current, nil
		}
		current = best
		currentDistance = bestDistance
	}
}

func (b *builder) searchLayer(ctx context.Context, queryNode nodeOrdinal, entryPoints []nodeOrdinal, ef, level int) ([]searchCandidate, error) {
	capacity := min(ef, len(b.graph.nodes)-1)
	results := newResultHeapWithBuffer(capacity, b.workspace.results)
	frontier := candidateHeap{items: resetSearchCandidates(b.workspace.frontier, min(capacity, len(entryPoints)))}
	b.workspace.nextEpoch()

	offer := func(node nodeOrdinal) {
		if node == queryNode || !b.workspace.markSeen(node) {
			return
		}
		candidate := searchCandidate{node: node, distance: b.distanceNodes(queryNode, node), accepted: true}
		frontier.Push(candidate)
		results.Add(candidate)
	}
	work := 0
	for _, entry := range entryPoints {
		if err := periodicContextError(ctx, work); err != nil {
			return nil, err
		}
		work++
		offer(entry)
	}
	for frontier.Len() > 0 {
		if err := periodicContextError(ctx, work); err != nil {
			return nil, err
		}
		work++
		candidate, _ := frontier.Pop()
		if worst, ok := results.Worst(); ok && results.Len() >= capacity && navigationBetter(worst, candidate) {
			break
		}
		for _, neighbor := range b.graph.nodes[candidate.node].links[level] {
			if err := periodicContextError(ctx, work); err != nil {
				return nil, err
			}
			work++
			if neighbor == queryNode || !b.workspace.markSeen(neighbor) {
				continue
			}
			discovered := searchCandidate{node: neighbor, distance: b.distanceNodes(queryNode, neighbor), accepted: true}
			worst, full := results.Worst()
			if !full || results.Len() < capacity || navigationBetter(discovered, worst) {
				frontier.Push(discovered)
				results.Add(discovered)
			}
		}
	}
	b.workspace.frontier = frontier.items[:0]
	return results.items, ctx.Err()
}
