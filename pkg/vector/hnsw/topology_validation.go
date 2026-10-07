package hnsw

import (
	"context"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func validatePreparedVectorStoreContext(ctx context.Context, source vector.PreparedVectorStore, calculator vector.Calculator, count int) error {
	if source == nil || isNilPreparedVectorStore(source) || source.Len() != count || source.Dimensions() != calculator.Dimensions() || source.Metric() != calculator.Metric() || source.Normalization() != calculator.Normalization() {
		return ErrCorruptGraphData
	}
	components, ok := checkedMultiply(uint64(count), uint64(calculator.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return ErrCorruptGraphData
	}
	scratch := make([]float32, calculator.Dimensions())
	for row := range count {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range scratch {
			scratch[i] = float32(math.NaN())
		}
		if err := source.ReadVectorInto(ctx, vector.Ordinal(row), scratch); err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return contextErr
			}
			return fmt.Errorf("%w: read vector row %d: %v", ErrGraphVectorStore, row, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validatePreparedVector(calculator, scratch); err != nil {
			return fmt.Errorf("%w: vector row %d: %v", ErrGraphVectorStore, row, err)
		}
	}
	return nil
}

func validatePackedTopologyContext(ctx context.Context, topology topology) (GraphStats, error) {
	if err := ctx.Err(); err != nil {
		return GraphStats{}, err
	}
	if topology.calculator.Dimensions() <= 0 || !topology.calculator.Metric().Valid() {
		return GraphStats{}, errInvalidGraph
	}
	nodes := len(topology.levels)
	if uint64(nodes) >= math.MaxUint32 || len(topology.level0Offsets) != nodes+1 || len(topology.upperNodeOffsets) != nodes+1 || len(topology.upperLinkOffsets) == 0 || uint64(len(topology.level0Neighbors)) >= math.MaxUint32 || uint64(len(topology.upperLinkOffsets)-1) >= math.MaxUint32 || uint64(len(topology.upperNeighbors)) >= math.MaxUint32 {
		return GraphStats{}, errInvalidGraph
	}
	if links, ok := checkedAdd(uint64(len(topology.level0Neighbors)), uint64(len(topology.upperNeighbors))); !ok || links > uint64(math.MaxInt) {
		return GraphStats{}, errInvalidGraph
	}
	if nodes == 0 {
		if topology.hasEntry || topology.entry != 0 || len(topology.level0Neighbors) != 0 || len(topology.upperLinkOffsets) != 1 || len(topology.upperNeighbors) != 0 || topology.level0Offsets[0] != 0 || topology.upperNodeOffsets[0] != 0 || topology.upperLinkOffsets[0] != 0 {
			return GraphStats{}, errInvalidGraph
		}
		return GraphStats{MaxLevel: -1}, nil
	}
	if !topology.hasEntry || uint64(topology.entry) >= uint64(nodes) {
		return GraphStats{}, errInvalidGraph
	}
	maxLevel, placements := 0, uint64(0)
	for node := range nodes {
		if err := contextProgressCheck(ctx, node); err != nil {
			return GraphStats{}, err
		}
		level := int(topology.levels[node])
		if level > MaxLevel || uint64(topology.upperNodeOffsets[node]) != placements {
			return GraphStats{}, errInvalidGraph
		}
		placements += uint64(level)
		if placements >= math.MaxUint32 {
			return GraphStats{}, errInvalidGraph
		}
		maxLevel = max(maxLevel, level)
	}
	if uint64(topology.upperNodeOffsets[nodes]) != placements || placements != uint64(len(topology.upperLinkOffsets)-1) || int(topology.levels[topology.entry]) != maxLevel {
		return GraphStats{}, errInvalidGraph
	}
	if !validOffsetsContext(ctx, topology.level0Offsets, len(topology.level0Neighbors)) || !validOffsetsContext(ctx, topology.upperLinkOffsets, len(topology.upperNeighbors)) {
		if err := ctx.Err(); err != nil {
			return GraphStats{}, err
		}
		return GraphStats{}, errInvalidGraph
	}
	stats := GraphStats{NodeCount: nodes, VectorCount: nodes, MaxLevel: maxLevel, LevelNodeCounts: make([]int, maxLevel+1), LevelLinkCounts: make([]int, maxLevel+1)}
	marks := make([]uint64, nodes)
	var epoch uint64
	for node := range nodes {
		if err := contextProgressCheck(ctx, node); err != nil {
			return GraphStats{}, err
		}
		for level := 0; level <= int(topology.levels[node]); level++ {
			neighbors, ok := topologyNeighborView(topology, nodeOrdinal(node), level)
			if !ok || len(neighbors) > topology.buildInfo.neighborLimit(level) {
				return GraphStats{}, errInvalidGraph
			}
			stats.LevelNodeCounts[level]++
			stats.LevelLinkCounts[level] += len(neighbors)
			epoch++
			for _, neighbor := range neighbors {
				if uint64(neighbor) >= uint64(nodes) || int(neighbor) == node || int(topology.levels[neighbor]) < level || marks[neighbor] == epoch {
					return GraphStats{}, errInvalidGraph
				}
				marks[neighbor] = epoch
			}
		}
		if topology.level0Offsets[node] == topology.level0Offsets[node+1] {
			stats.ZeroDegreeNodes++
		}
	}
	visited := make([]bool, nodes)
	queue := make([]nodeOrdinal, 1, nodes)
	queue[0] = topology.entry
	visited[topology.entry] = true
	processed := 0
	for len(queue) > 0 {
		if err := contextProgressCheck(ctx, processed); err != nil {
			return GraphStats{}, err
		}
		node := queue[0]
		queue = queue[1:]
		processed++
		stats.ReachableNodes++
		neighbors, _ := topologyNeighborView(topology, node, 0)
		for _, neighbor := range neighbors {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	stats.UnreachableNodes = nodes - stats.ReachableNodes
	if err := ctx.Err(); err != nil {
		return GraphStats{}, err
	}
	return stats, nil
}

func topologyNeighborView(topology topology, node nodeOrdinal, level int) ([]nodeOrdinal, bool) {
	if level < 0 || uint64(node) >= uint64(len(topology.levels)) || level > int(topology.levels[node]) {
		return nil, false
	}
	if level == 0 {
		return topology.level0Neighbors[topology.level0Offsets[node]:topology.level0Offsets[node+1]], true
	}
	placement := topology.upperNodeOffsets[node] + uint32(level-1)
	return topology.upperNeighbors[topology.upperLinkOffsets[placement]:topology.upperLinkOffsets[placement+1]], true
}

func validOffsetsContext(ctx context.Context, offsets []uint32, values int) bool {
	if len(offsets) == 0 || offsets[0] != 0 || uint64(offsets[len(offsets)-1]) != uint64(values) {
		return false
	}
	for i := 1; i < len(offsets); i++ {
		if contextProgressCheck(ctx, i) != nil {
			return false
		}
		if offsets[i] < offsets[i-1] || uint64(offsets[i]) > uint64(values) {
			return false
		}
	}
	return true
}
func contextProgressCheck(ctx context.Context, index int) error {
	if index&0x3fff != 0 {
		return nil
	}
	return ctx.Err()
}

func checkedAdd(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}
