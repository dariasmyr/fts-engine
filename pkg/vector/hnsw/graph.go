package hnsw

import (
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type mutableNode struct {
	vectorOrdinal vector.Ordinal
	level         uint8
	links         [][]NodeOrdinal
}

type graphData struct {
	values   []float32
	nodes    []mutableNode
	entry    NodeOrdinal
	hasEntry bool
}

func validateBuildInfo(info BuildInfo) error {
	if info.BuildVersion != BuildVersion || info.LevelGeneratorVersion != LevelGeneratorVersion {
		return ErrInvalidBuildConfig
	}
	return validatePersistedBuildInfo(info)
}

func validatePersistedBuildInfo(info BuildInfo) error {
	if info.BuildVersion == 0 || info.LevelGeneratorVersion == 0 {
		return ErrInvalidBuildConfig
	}
	if info.MaxNeighbors < 2 || info.MaxNeighbors > MaxSupportedNeighbors || info.MaxNeighbors > math.MaxInt/2 {
		return ErrInvalidBuildConfig
	}
	if info.LevelZeroMaxNeighbors != info.MaxNeighbors*2 {
		return ErrInvalidBuildConfig
	}
	if info.EfConstruction < info.MaxNeighbors || info.EfConstruction > MaxEfConstruction {
		return ErrInvalidBuildConfig
	}
	return nil
}

func validateGraphData(calculator vector.Calculator, info BuildInfo, graph graphData) (GraphStats, error) {
	if err := validateBuildInfo(info); err != nil {
		return GraphStats{}, err
	}
	vectorCount, err := validateGraphShape(calculator, graph)
	if err != nil {
		return GraphStats{}, err
	}
	if vectorCount == 0 {
		if graph.hasEntry {
			return GraphStats{}, ErrInvalidGraph
		}
		return GraphStats{MaxLevel: -1}, nil
	}
	if !graph.hasEntry || uint64(graph.entry) >= uint64(len(graph.nodes)) {
		return GraphStats{}, ErrInvalidGraph
	}

	seenOrdinals := make([]bool, vectorCount)
	maxLevel := 0
	var totalLinks uint64
	for nodeOrdinal, node := range graph.nodes {
		if err := validateGraphNode(calculator, graph, nodeOrdinal, seenOrdinals); err != nil {
			return GraphStats{}, err
		}
		if err := validateNodeLinks(info, graph, nodeOrdinal, &totalLinks); err != nil {
			return GraphStats{}, err
		}
		maxLevel = max(maxLevel, int(node.level))
	}
	if int(graph.nodes[graph.entry].level) != maxLevel {
		return GraphStats{}, fmt.Errorf("%w: entry level", ErrInvalidGraph)
	}
	return graphStatistics(graph, maxLevel), nil
}

func validateGraphShape(calculator vector.Calculator, graph graphData) (int, error) {
	dimensions := calculator.Dimensions()
	if dimensions <= 0 || len(graph.values)%dimensions != 0 {
		return 0, ErrInvalidGraph
	}
	vectorCount := len(graph.values) / dimensions
	if vectorCount != len(graph.nodes) || uint64(vectorCount) >= math.MaxUint32 {
		return 0, ErrInvalidGraph
	}
	return vectorCount, nil
}

func validateGraphNode(calculator vector.Calculator, graph graphData, nodeOrdinal int, seenOrdinals []bool) error {
	node := graph.nodes[nodeOrdinal]
	if int(node.level) > MaxLevel || len(node.links) != int(node.level)+1 ||
		uint64(node.vectorOrdinal) >= uint64(len(seenOrdinals)) || seenOrdinals[node.vectorOrdinal] {
		return fmt.Errorf("%w: node %d metadata", ErrInvalidGraph, nodeOrdinal)
	}
	seenOrdinals[node.vectorOrdinal] = true
	dimensions := calculator.Dimensions()
	rowStart := int(node.vectorOrdinal) * dimensions
	if err := validatePreparedVector(calculator, graph.values[rowStart:rowStart+dimensions]); err != nil {
		return fmt.Errorf("%w: node %d vector: %v", ErrInvalidGraph, nodeOrdinal, err)
	}
	return nil
}

func validateNodeLinks(info BuildInfo, graph graphData, nodeOrdinal int, totalLinks *uint64) error {
	for level, neighbors := range graph.nodes[nodeOrdinal].links {
		if len(neighbors) > info.neighborLimit(level) {
			return fmt.Errorf("%w: node %d level %d degree", ErrInvalidGraph, nodeOrdinal, level)
		}
		seenNeighbors := make(map[NodeOrdinal]struct{}, len(neighbors))
		for _, neighbor := range neighbors {
			if uint64(neighbor) >= uint64(len(graph.nodes)) || int(neighbor) == nodeOrdinal || int(graph.nodes[neighbor].level) < level {
				return fmt.Errorf("%w: node %d level %d link", ErrInvalidGraph, nodeOrdinal, level)
			}
			if _, duplicate := seenNeighbors[neighbor]; duplicate {
				return fmt.Errorf("%w: node %d level %d duplicate link", ErrInvalidGraph, nodeOrdinal, level)
			}
			seenNeighbors[neighbor] = struct{}{}
		}
		*totalLinks += uint64(len(neighbors))
		if *totalLinks >= math.MaxUint32 {
			return ErrInvalidGraph
		}
	}
	return nil
}

func validatePreparedVector(calculator vector.Calculator, value []float32) error {
	if err := calculator.Validate(value); err != nil {
		return err
	}
	if calculator.Normalization() == vector.NormalizationUnitLength {
		var normSquared float64
		for _, component := range value {
			normSquared += float64(component) * float64(component)
		}
		if math.Abs(normSquared-1) > 1e-4 {
			return ErrInvalidGraph
		}
	}
	for _, component := range value {
		if math.Float32bits(component) == 1<<31 {
			return ErrInvalidGraph
		}
	}
	return nil
}

func graphStatistics(graph graphData, maxLevel int) GraphStats {
	stats := GraphStats{
		NodeCount: len(graph.nodes), VectorCount: len(graph.nodes), MaxLevel: maxLevel,
		LevelNodeCounts: make([]int, maxLevel+1), LevelLinkCounts: make([]int, maxLevel+1),
	}
	for _, node := range graph.nodes {
		for level, neighbors := range node.links {
			stats.LevelNodeCounts[level]++
			stats.LevelLinkCounts[level] += len(neighbors)
		}
		if len(node.links[0]) == 0 {
			stats.ZeroDegreeNodes++
		}
	}
	visited := make([]bool, len(graph.nodes))
	queue := []NodeOrdinal{graph.entry}
	visited[graph.entry] = true
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		stats.ReachableNodes++
		for _, neighbor := range graph.nodes[node].links[0] {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	stats.UnreachableNodes = len(graph.nodes) - stats.ReachableNodes
	return stats
}

func cloneGraphData(graph graphData) graphData {
	cloned := graphData{
		values: append([]float32(nil), graph.values...), entry: graph.entry, hasEntry: graph.hasEntry,
		nodes: make([]mutableNode, len(graph.nodes)),
	}
	for i, node := range graph.nodes {
		cloned.nodes[i] = mutableNode{vectorOrdinal: node.vectorOrdinal, level: node.level, links: make([][]NodeOrdinal, len(node.links))}
		for level := range node.links {
			cloned.nodes[i].links[level] = append([]NodeOrdinal(nil), node.links[level]...)
		}
	}
	return cloned
}
