package hnsw

import (
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
func (b *builder) add(prepared []float32) error {
	if err := validatePreparedVector(b.calculator, prepared); err != nil {
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
	b.insert(node)
	return nil
}

func (b *builder) freeze(source vector.PreparedVectorStore, search SearchConfig) (*Index, error) {
	return newBuiltIndex(b.calculator, search, b.config.info(), b.graph, source)
}

func (b *builder) insert(node nodeOrdinal) {
	newLevel := int(b.graph.nodes[node].level)
	oldEntry := b.graph.entry
	oldMaxLevel := int(b.graph.nodes[oldEntry].level)
	current := oldEntry
	for level := oldMaxLevel; level > newLevel; level-- {
		current = b.greedyBuild(node, current, level)
	}

	entries := append(b.workspace.entries[:0], current)
	for level := min(newLevel, oldMaxLevel); level >= 0; level-- {
		candidates := b.searchLayer(node, entries, b.config.EfConstruction, level)
		selected := b.selectNeighbors(candidates, b.config.MaxNeighbors)
		b.graph.nodes[node].links[level] = selected
		for _, neighbor := range selected {
			b.addReverseLink(neighbor, node, level)
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
}

func (b *builder) greedyBuild(queryNode, current nodeOrdinal, level int) nodeOrdinal {
	currentDistance := b.distanceNodes(queryNode, current)
	for {
		best := current
		bestDistance := currentDistance
		for _, neighbor := range b.graph.nodes[current].links[level] {
			distance := b.distanceNodes(queryNode, neighbor)
			if distance < currentDistance && (distance < bestDistance || distance == bestDistance && neighbor < best) {
				best = neighbor
				bestDistance = distance
			}
		}
		if best == current {
			return current
		}
		current = best
		currentDistance = bestDistance
	}
}

func (b *builder) searchLayer(queryNode nodeOrdinal, entryPoints []nodeOrdinal, ef, level int) []searchCandidate {
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
	for _, entry := range entryPoints {
		offer(entry)
	}
	for frontier.Len() > 0 {
		candidate, _ := frontier.Pop()
		if worst, ok := results.Worst(); ok && results.Len() >= capacity && navigationBetter(worst, candidate) {
			break
		}
		for _, neighbor := range b.graph.nodes[candidate.node].links[level] {
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
	return results.items
}
