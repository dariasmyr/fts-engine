package hnsw

import (
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type mutableNode struct {
	level uint8
	links [][]nodeOrdinal
}

type graphData struct {
	values   []float32
	nodes    []mutableNode
	entry    nodeOrdinal
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
			return GraphStats{}, errInvalidGraph
		}
		return GraphStats{MaxLevel: -1}, nil
	}
	if !graph.hasEntry || uint64(graph.entry) >= uint64(len(graph.nodes)) {
		return GraphStats{}, errInvalidGraph
	}

	maxLevel := 0
	var totalLinks uint64
	marks := make([]uint32, vectorCount)
	var epoch uint32
	for nodeOrdinal, node := range graph.nodes {
		if err := validateGraphNode(calculator, graph, nodeOrdinal); err != nil {
			return GraphStats{}, err
		}
		if err := validateNodeLinks(info, graph, nodeOrdinal, &totalLinks, marks, &epoch); err != nil {
			return GraphStats{}, err
		}
		maxLevel = max(maxLevel, int(node.level))
	}
	if int(graph.nodes[graph.entry].level) != maxLevel {
		return GraphStats{}, fmt.Errorf("%w: entry level", errInvalidGraph)
	}
	return graphStatistics(graph, maxLevel), nil
}

func validateGraphShape(calculator vector.Calculator, graph graphData) (int, error) {
	dimensions := calculator.Dimensions()
	if dimensions <= 0 || len(graph.values)%dimensions != 0 {
		return 0, errInvalidGraph
	}
	vectorCount := len(graph.values) / dimensions
	if vectorCount != len(graph.nodes) || uint64(vectorCount) >= math.MaxUint32 {
		return 0, errInvalidGraph
	}
	return vectorCount, nil
}

func validateGraphNode(calculator vector.Calculator, graph graphData, nodeOrdinal int) error {
	node := graph.nodes[nodeOrdinal]
	if int(node.level) > MaxLevel || len(node.links) != int(node.level)+1 {
		return fmt.Errorf("%w: node %d metadata", errInvalidGraph, nodeOrdinal)
	}
	dimensions := calculator.Dimensions()
	rowStart := nodeOrdinal * dimensions
	if err := calculator.ValidatePrepared(graph.values[rowStart : rowStart+dimensions]); err != nil {
		return fmt.Errorf("%w: node %d vector: %v", errInvalidGraph, nodeOrdinal, err)
	}
	return nil
}

func validateNodeLinks(info BuildInfo, graph graphData, nodeIndex int, totalLinks *uint64, marks []uint32, epoch *uint32) error {
	for level, neighbors := range graph.nodes[nodeIndex].links {
		if len(neighbors) > info.neighborLimit(level) {
			return fmt.Errorf("%w: node %d level %d degree", errInvalidGraph, nodeIndex, level)
		}
		(*epoch)++
		if *epoch == 0 {
			clear(marks)
			*epoch = 1
		}
		for _, neighbor := range neighbors {
			if uint64(neighbor) >= uint64(len(graph.nodes)) || int(neighbor) == nodeIndex || int(graph.nodes[neighbor].level) < level {
				return fmt.Errorf("%w: node %d level %d link", errInvalidGraph, nodeIndex, level)
			}
			if marks[neighbor] == *epoch {
				return fmt.Errorf("%w: node %d level %d duplicate link", errInvalidGraph, nodeIndex, level)
			}
			marks[neighbor] = *epoch
		}
		*totalLinks += uint64(len(neighbors))
		if *totalLinks >= math.MaxUint32 {
			return errInvalidGraph
		}
	}
	return nil
}

func validatePreparedVector(calculator vector.Calculator, value []float32) error {
	return calculator.ValidatePrepared(value)
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
	queue := make([]nodeOrdinal, 1, len(graph.nodes))
	queue[0] = graph.entry
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
