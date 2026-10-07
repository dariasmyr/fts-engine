package hnsw

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// nodeOrdinal is a dense graph-local node number and is always equal to the
// corresponding vector.Ordinal.
type nodeOrdinal = uint32

type topology struct {
	calculator vector.Calculator
	buildInfo  BuildInfo
	stats      GraphStats

	levels   []uint8
	entry    nodeOrdinal
	hasEntry bool

	level0Offsets   []uint32
	level0Neighbors []nodeOrdinal

	upperNodeOffsets []uint32
	upperLinkOffsets []uint32
	upperNeighbors   []nodeOrdinal
}

func packGraph(calculator vector.Calculator, buildInfo BuildInfo, graph graphData, stats GraphStats) (topology, error) {
	packed := topology{
		calculator:       calculator,
		buildInfo:        buildInfo,
		stats:            cloneGraphStats(stats),
		levels:           make([]uint8, len(graph.nodes)),
		entry:            graph.entry,
		hasEntry:         graph.hasEntry,
		level0Offsets:    make([]uint32, len(graph.nodes)+1),
		upperNodeOffsets: make([]uint32, len(graph.nodes)+1),
	}

	var level0Count uint64
	var upperPlacementCount uint64
	var upperNeighborCount uint64
	for nodeIndex, node := range graph.nodes {
		packed.levels[nodeIndex] = node.level
		packed.level0Offsets[nodeIndex] = uint32(level0Count)
		level0Count += uint64(len(node.links[0]))
		packed.upperNodeOffsets[nodeIndex] = uint32(upperPlacementCount)
		upperPlacementCount += uint64(node.level)
		for level := 1; level <= int(node.level); level++ {
			upperNeighborCount += uint64(len(node.links[level]))
		}
		if level0Count >= math.MaxUint32 || upperPlacementCount >= math.MaxUint32 || upperNeighborCount >= math.MaxUint32 {
			return topology{}, errInvalidGraph
		}
	}
	packed.level0Offsets[len(graph.nodes)] = uint32(level0Count)
	packed.upperNodeOffsets[len(graph.nodes)] = uint32(upperPlacementCount)
	packed.level0Neighbors = make([]nodeOrdinal, 0, int(level0Count))
	packed.upperLinkOffsets = make([]uint32, int(upperPlacementCount)+1)
	packed.upperNeighbors = make([]nodeOrdinal, 0, int(upperNeighborCount))

	placement := 0
	for _, node := range graph.nodes {
		packed.level0Neighbors = append(packed.level0Neighbors, node.links[0]...)
		for level := 1; level <= int(node.level); level++ {
			packed.upperLinkOffsets[placement] = uint32(len(packed.upperNeighbors))
			packed.upperNeighbors = append(packed.upperNeighbors, node.links[level]...)
			placement++
		}
	}
	packed.upperLinkOffsets[placement] = uint32(len(packed.upperNeighbors))
	return packed, nil
}

func graphStatisticsForTrustedGraph(graph graphData) GraphStats {
	if len(graph.nodes) == 0 {
		return GraphStats{MaxLevel: -1}
	}
	return graphStatistics(graph, int(graph.nodes[graph.entry].level))
}

func (i *Index) entryPoint() (nodeOrdinal, int, bool) {
	if i == nil || !i.topology.hasEntry {
		return 0, 0, false
	}
	return i.topology.entry, int(i.topology.levels[i.topology.entry]), true
}

func (i *Index) neighborView(node nodeOrdinal, level int) ([]nodeOrdinal, bool) {
	if i == nil || level < 0 || uint64(node) >= uint64(len(i.topology.levels)) || level > int(i.topology.levels[node]) {
		return nil, false
	}
	if level == 0 {
		start := i.topology.level0Offsets[node]
		end := i.topology.level0Offsets[int(node)+1]
		return i.topology.level0Neighbors[start:end], true
	}
	placement := i.topology.upperNodeOffsets[node] + uint32(level-1)
	start := i.topology.upperLinkOffsets[placement]
	end := i.topology.upperLinkOffsets[placement+1]
	return i.topology.upperNeighbors[start:end], true
}

func (i *Index) nodeLevel(node nodeOrdinal) (uint8, bool) {
	if i == nil || uint64(node) >= uint64(len(i.topology.levels)) {
		return 0, false
	}
	return i.topology.levels[node], true
}

func (i *Index) neighbors(node nodeOrdinal, level int) ([]nodeOrdinal, bool) {
	return i.neighborView(node, level)
}
