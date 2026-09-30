package hnsw

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// aligned4 is retained for package-local format tests; byte encoding lives in
// the format subpackage.
func aligned4(value uint64) uint64 { return (value + 3) &^ 3 }

func validateIndexVectorsContext(ctx context.Context, reader *Index) error {
	if reader == nil || reader.vectors == nil || reader.vectors.Len() != reader.Len() || reader.vectors.Dimensions() != reader.Dimensions() || reader.vectors.Metric() != reader.Metric() || reader.vectors.Normalization() != reader.topology.calculator.Normalization() {
		return ErrCorruptGraphData
	}
	components, ok := checkedMultiply(uint64(reader.Len()), uint64(reader.Dimensions()))
	if !ok || components > uint64(math.MaxInt) {
		return ErrCorruptGraphData
	}
	scratch := make([]float32, reader.Dimensions())
	for row := range reader.Len() {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := range scratch {
			scratch[i] = float32(math.NaN())
		}
		if err := reader.vectors.ReadVectorInto(ctx, vector.Ordinal(row), scratch); err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return contextErr
			}
			return fmt.Errorf("%w: read vector row %d: %v", ErrGraphVectorStore, row, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validatePreparedVector(reader.topology.calculator, scratch); err != nil {
			return fmt.Errorf("%w: vector row %d: %v", ErrGraphVectorStore, row, err)
		}
	}
	return nil
}

func validatePackedTopologyContext(ctx context.Context, reader *Index) (GraphStats, error) {
	if err := ctx.Err(); err != nil {
		return GraphStats{}, err
	}
	if reader == nil || reader.Dimensions() <= 0 || !reader.Metric().Valid() {
		return GraphStats{}, errInvalidGraph
	}
	nodes := len(reader.topology.nodeToVector)
	if uint64(nodes) >= math.MaxUint32 || len(reader.topology.levels) != nodes || len(reader.topology.level0Offsets) != nodes+1 || len(reader.topology.upperNodeOffsets) != nodes+1 || len(reader.topology.upperLinkOffsets) == 0 || uint64(len(reader.topology.level0Neighbors)) >= math.MaxUint32 || uint64(len(reader.topology.upperLinkOffsets)-1) >= math.MaxUint32 || uint64(len(reader.topology.upperNeighbors)) >= math.MaxUint32 {
		return GraphStats{}, errInvalidGraph
	}
	if links, ok := checkedAdd(uint64(len(reader.topology.level0Neighbors)), uint64(len(reader.topology.upperNeighbors))); !ok || links > uint64(math.MaxInt) {
		return GraphStats{}, errInvalidGraph
	}
	if nodes == 0 {
		if reader.topology.hasEntry || reader.topology.entry != 0 || len(reader.topology.level0Neighbors) != 0 || len(reader.topology.upperLinkOffsets) != 1 || len(reader.topology.upperNeighbors) != 0 || reader.topology.level0Offsets[0] != 0 || reader.topology.upperNodeOffsets[0] != 0 || reader.topology.upperLinkOffsets[0] != 0 {
			return GraphStats{}, errInvalidGraph
		}
		return GraphStats{MaxLevel: -1}, nil
	}
	if !reader.topology.hasEntry || uint64(reader.topology.entry) >= uint64(nodes) {
		return GraphStats{}, errInvalidGraph
	}
	seenVectors := make([]bool, nodes)
	maxLevel, placements := 0, uint64(0)
	for node := range nodes {
		if err := contextProgressCheck(ctx, node); err != nil {
			return GraphStats{}, err
		}
		ordinal, level := reader.topology.nodeToVector[node], int(reader.topology.levels[node])
		if uint64(ordinal) >= uint64(nodes) || seenVectors[ordinal] || level > MaxLevel || uint64(reader.topology.upperNodeOffsets[node]) != placements {
			return GraphStats{}, errInvalidGraph
		}
		seenVectors[ordinal] = true
		placements += uint64(level)
		if placements >= math.MaxUint32 {
			return GraphStats{}, errInvalidGraph
		}
		maxLevel = max(maxLevel, level)
	}
	if uint64(reader.topology.upperNodeOffsets[nodes]) != placements || placements != uint64(len(reader.topology.upperLinkOffsets)-1) || int(reader.topology.levels[reader.topology.entry]) != maxLevel {
		return GraphStats{}, errInvalidGraph
	}
	if !validOffsetsContext(ctx, reader.topology.level0Offsets, len(reader.topology.level0Neighbors)) || !validOffsetsContext(ctx, reader.topology.upperLinkOffsets, len(reader.topology.upperNeighbors)) {
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
		for level := 0; level <= int(reader.topology.levels[node]); level++ {
			neighbors, ok := reader.neighborView(nodeOrdinal(node), level)
			if !ok || len(neighbors) > reader.topology.buildInfo.neighborLimit(level) {
				return GraphStats{}, errInvalidGraph
			}
			stats.LevelNodeCounts[level]++
			stats.LevelLinkCounts[level] += len(neighbors)
			epoch++
			for _, neighbor := range neighbors {
				if uint64(neighbor) >= uint64(nodes) || int(neighbor) == node || int(reader.topology.levels[neighbor]) < level || marks[neighbor] == epoch {
					return GraphStats{}, errInvalidGraph
				}
				marks[neighbor] = epoch
			}
		}
		if reader.topology.level0Offsets[node] == reader.topology.level0Offsets[node+1] {
			stats.ZeroDegreeNodes++
		}
	}
	visited := make([]bool, nodes)
	queue := make([]nodeOrdinal, 1, nodes)
	queue[0] = reader.topology.entry
	visited[reader.topology.entry] = true
	processed := 0
	for len(queue) > 0 {
		if err := contextProgressCheck(ctx, processed); err != nil {
			return GraphStats{}, err
		}
		node := queue[0]
		queue = queue[1:]
		processed++
		stats.ReachableNodes++
		neighbors, _ := reader.neighborView(node, 0)
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
func indexConfigEncodable(index *Index) bool {
	values := [...]int{index.Dimensions(), index.topology.searchConfig.DefaultEfSearch, index.topology.searchConfig.MaxEfSearch, index.topology.searchConfig.DefaultVisitLimit, index.topology.searchConfig.MaxVisitLimit, index.topology.searchConfig.MaxK, index.topology.buildInfo.MaxNeighbors, index.topology.buildInfo.LevelZeroMaxNeighbors, index.topology.buildInfo.EfConstruction}
	for _, value := range values {
		if value < 0 || uint64(value) > math.MaxUint32 {
			return false
		}
	}
	return true
}

func normalizeGraphLimits(limits GraphLimits) GraphLimits {
	defaults := DefaultGraphLimits()
	if limits.MaxDimensions == 0 {
		limits.MaxDimensions = defaults.MaxDimensions
	}
	if limits.MaxVectors == 0 {
		limits.MaxVectors = defaults.MaxVectors
	}
	if limits.MaxVectorBytes == 0 {
		limits.MaxVectorBytes = defaults.MaxVectorBytes
	}
	if limits.MaxGraphBytes == 0 {
		limits.MaxGraphBytes = defaults.MaxGraphBytes
	}
	if limits.MaxLinks == 0 {
		limits.MaxLinks = defaults.MaxLinks
	}
	if limits.MaxLevel == 0 {
		limits.MaxLevel = defaults.MaxLevel
	}
	if limits.MaxEfSearch == 0 {
		limits.MaxEfSearch = defaults.MaxEfSearch
	}
	if limits.MaxVisitLimit == 0 {
		limits.MaxVisitLimit = defaults.MaxVisitLimit
	}
	if limits.MaxK == 0 {
		limits.MaxK = defaults.MaxK
	}
	if limits.MaxNeighbors == 0 {
		limits.MaxNeighbors = defaults.MaxNeighbors
	}
	if limits.MaxEfConstruction == 0 {
		limits.MaxEfConstruction = defaults.MaxEfConstruction
	}
	return limits
}
func validateGraphLimits(limits GraphLimits) error {
	if limits.MaxDimensions <= 0 || limits.MaxVectors <= 0 || limits.MaxVectorBytes == 0 || limits.MaxGraphBytes < graphFormatHeaderSize+graphFormatFooterSize || limits.MaxLinks == 0 || limits.MaxLevel < 0 || limits.MaxLevel > MaxLevel || limits.MaxEfSearch <= 0 || limits.MaxVisitLimit <= 0 || limits.MaxK <= 0 || limits.MaxNeighbors < 2 || limits.MaxNeighbors > MaxSupportedNeighbors || limits.MaxEfConstruction < 2 || limits.MaxEfConstruction > MaxEfConstruction {
		return ErrGraphLimitExceeded
	}
	return nil
}
func validVectorFileReference(reference VectorFileReference) bool {
	return reference.Size != 0 && reference.SHA256 != [sha256.Size]byte{}
}
func checkedAdd(a, b uint64) (uint64, bool) {
	if b > math.MaxUint64-a {
		return 0, false
	}
	return a + b, true
}
