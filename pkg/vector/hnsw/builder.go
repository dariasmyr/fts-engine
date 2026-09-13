package hnsw

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Builder constructs one HNSW graph. It is single-writer and is not searchable.
type Builder struct {
	buildConfig  BuildConfig
	searchConfig SearchConfig
	buildInfo    BuildInfo
	space        vector.Space
	expected     int
	present      []bool
	graph        graphData
	rng          levelRNG
	source       PreparedVectorSource
}

func NewBuilder(buildConfig BuildConfig, searchConfig SearchConfig, vectorCount int) (*Builder, error) {
	space, components, err := buildConfig.validate(vectorCount)
	if err != nil {
		return nil, err
	}
	if err := searchConfig.validate(); err != nil {
		return nil, err
	}
	return &Builder{
		buildConfig: buildConfig, searchConfig: searchConfig, buildInfo: buildConfig.info(),
		space: space, expected: vectorCount, present: make([]bool, vectorCount),
		graph: graphData{values: make([]float32, components), nodes: make([]mutableNode, 0, vectorCount)},
		rng:   newLevelRNG(buildConfig.Seed),
	}, nil
}

// Add prepares value and inserts one graph node mapped to ordinal.
func (b *Builder) Add(ctx context.Context, ordinal vector.Ordinal, value []float32) (NodeOrdinal, error) {
	if ctx == nil {
		return 0, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(b.graph.nodes) >= b.expected {
		return 0, ErrCapacityExceeded
	}
	if uint64(ordinal) >= uint64(b.expected) {
		return 0, fmt.Errorf("%w: %d", vector.ErrOrdinalOutOfRange, ordinal)
	}
	if b.present[ordinal] {
		return 0, fmt.Errorf("%w: %d", ErrDuplicateOrdinal, ordinal)
	}
	prepared, err := b.space.Prepare(value)
	if err != nil {
		return 0, err
	}
	return b.addPrepared(ordinal, prepared)
}

// addPrepared validates and copies an already prepared row without normalizing
// it again. This preserves source float32 bits exactly.
func (b *Builder) addPrepared(ordinal vector.Ordinal, prepared []float32) (NodeOrdinal, error) {
	if len(b.graph.nodes) >= b.expected {
		return 0, ErrCapacityExceeded
	}
	if uint64(ordinal) >= uint64(b.expected) {
		return 0, fmt.Errorf("%w: %d", vector.ErrOrdinalOutOfRange, ordinal)
	}
	if b.present[ordinal] {
		return 0, fmt.Errorf("%w: %d", ErrDuplicateOrdinal, ordinal)
	}
	if err := validatePreparedVector(b.space, prepared); err != nil {
		return 0, err
	}

	nextRNG := b.rng
	level := nextRNG.level(b.buildConfig.MaxNeighbors)
	nodeOrdinal := NodeOrdinal(len(b.graph.nodes))
	rowStart := int(ordinal) * b.space.Dimensions()
	copy(b.graph.values[rowStart:rowStart+b.space.Dimensions()], prepared)
	b.graph.nodes = append(b.graph.nodes, mutableNode{
		vectorOrdinal: ordinal, level: level, links: make([][]NodeOrdinal, int(level)+1),
	})
	b.present[ordinal] = true
	b.rng = nextRNG

	if !b.graph.hasEntry {
		b.graph.entry = nodeOrdinal
		b.graph.hasEntry = true
		return nodeOrdinal, nil
	}
	b.insert(nodeOrdinal)
	return nodeOrdinal, nil
}

func (b *Builder) Len() int { return len(b.graph.nodes) }

// Check validates the current partial graph and reports directed reachability.
func (b *Builder) Check() (GraphStats, error) {
	// The backing matrix includes rows for ordinals not added yet. Compact only
	// present rows so the common validator can also check an incomplete build.
	compact := graphData{nodes: make([]mutableNode, len(b.graph.nodes)), hasEntry: b.graph.hasEntry, entry: b.graph.entry}
	compact.values = make([]float32, len(b.graph.nodes)*b.space.Dimensions())
	seen := make([]bool, b.expected)
	for nodeOrdinal, node := range b.graph.nodes {
		if uint64(node.vectorOrdinal) >= uint64(b.expected) || seen[node.vectorOrdinal] || !b.present[node.vectorOrdinal] {
			return GraphStats{}, ErrInvalidGraph
		}
		seen[node.vectorOrdinal] = true
		sourceStart := int(node.vectorOrdinal) * b.space.Dimensions()
		targetStart := nodeOrdinal * b.space.Dimensions()
		copy(compact.values[targetStart:targetStart+b.space.Dimensions()], b.graph.values[sourceStart:sourceStart+b.space.Dimensions()])
		compact.nodes[nodeOrdinal] = mutableNode{vectorOrdinal: vector.Ordinal(nodeOrdinal), level: node.level, links: make([][]NodeOrdinal, len(node.links))}
		for level := range node.links {
			compact.nodes[nodeOrdinal].links[level] = append([]NodeOrdinal(nil), node.links[level]...)
		}
	}
	return validateGraphData(b.space, b.buildInfo, compact)
}

// Freeze validates and copies a complete graph into an immutable packed Reader.
func (b *Builder) Freeze() (*Reader, error) {
	if len(b.graph.nodes) != b.expected {
		return nil, ErrBuilderIncomplete
	}
	for _, present := range b.present {
		if !present {
			return nil, ErrBuilderIncomplete
		}
	}
	// newReaderFromGraph validates and copies every retained section, so passing
	// the mutable graph directly avoids a redundant full graph clone.
	return newReaderFromGraph(b.space, b.searchConfig, b.buildInfo, b.graph, b.source)
}

func (b *Builder) insert(node NodeOrdinal) {
	newLevel := int(b.graph.nodes[node].level)
	oldEntry := b.graph.entry
	oldMaxLevel := int(b.graph.nodes[oldEntry].level)
	current := oldEntry
	// Levels above the new node are navigation-only: the node cannot own links there.
	for level := oldMaxLevel; level > newLevel; level-- {
		current = b.greedyBuild(node, current, level)
	}
	entryPoints := []NodeOrdinal{current}
	// Every shared level gets an independent beam search over links[level]. Its
	// candidates seed the next lower level, but links from different levels never mix.
	for level := min(newLevel, oldMaxLevel); level >= 0; level-- {
		candidates := b.searchLayer(node, entryPoints, b.buildConfig.EfConstruction, level)
		candidateNodes := make([]NodeOrdinal, len(candidates))
		for i, candidate := range candidates {
			candidateNodes[i] = candidate.node
		}
		selected := b.selectNeighbors(node, candidateNodes, b.buildConfig.MaxNeighbors)
		b.graph.nodes[node].links[level] = selected
		for _, neighbor := range selected {
			b.addReverseLink(neighbor, node, level)
		}
		if len(candidateNodes) > 0 {
			entryPoints = candidateNodes
		}
	}
	if newLevel > oldMaxLevel {
		b.graph.entry = node
	}
}

func (b *Builder) greedyBuild(queryNode, current NodeOrdinal, level int) NodeOrdinal {
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

func (b *Builder) searchLayer(queryNode NodeOrdinal, entryPoints []NodeOrdinal, ef, level int) []searchCandidate {
	// ef bounds the retained multi-hop candidate set on this one level. Direct
	// adjacency is bounded separately by MaxNeighbors above level 0 and by the
	// derived LevelZeroMaxNeighbors on level 0.
	capacity := min(ef, len(b.graph.nodes)-1)
	results := newResultHeap(capacity)
	frontier := candidateHeap{items: make([]searchCandidate, 0, min(capacity, len(entryPoints)))}
	seen := make(map[NodeOrdinal]struct{}, min(capacity, len(b.graph.nodes)))
	offer := func(node NodeOrdinal) {
		if node == queryNode {
			return
		}
		if _, exists := seen[node]; exists {
			return
		}
		seen[node] = struct{}{}
		distance := b.distanceNodes(queryNode, node)
		candidate := searchCandidate{node: node, vectorOrdinal: vector.Ordinal(node), distance: distance, accepted: true}
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
			if _, exists := seen[neighbor]; exists || neighbor == queryNode {
				continue
			}
			seen[neighbor] = struct{}{}
			distance := b.distanceNodes(queryNode, neighbor)
			discovered := searchCandidate{node: neighbor, vectorOrdinal: vector.Ordinal(neighbor), distance: distance, accepted: true}
			worst, full := results.Worst()
			if !full || results.Len() < capacity || navigationBetter(discovered, worst) {
				frontier.Push(discovered)
				results.Add(discovered)
			}
		}
	}
	return results.Candidates()
}
