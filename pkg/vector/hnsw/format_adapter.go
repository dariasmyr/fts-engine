package hnsw

import (
	"context"
	"errors"

	"github.com/dariasmyr/fts-engine/pkg/vector"
	vhng "github.com/dariasmyr/fts-engine/pkg/vector/hnsw/internal/format"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func fileMetadata(metadata vhng.Metadata) FileMetadata {
	return FileMetadata{Size: metadata.Size, CRC32: metadata.CRC32, SHA256: metadata.SHA256}
}

func graphToFormat(reader *HNSWIndex, vectors VectorFileReference) vhng.Graph {
	levels := append([]uint8(nil), reader.topology.levels...)
	maxLevel := uint8(0xff)
	if reader.Len() != 0 {
		maxLevel = 0
		for _, level := range levels {
			if level > maxLevel {
				maxLevel = level
			}
		}
	}
	return vhng.Graph{
		Dimensions: uint32(reader.Dimensions()), Metric: uint8(reader.Metric()), Normalization: uint8(reader.Normalization()), HasEntry: reader.topology.hasEntry, Entry: uint32(reader.topology.entry), MaxLevel: maxLevel,
		Search:  vhng.SearchConfig{DefaultEfSearch: uint32(reader.topology.searchConfig.DefaultEfSearch), MaxEfSearch: uint32(reader.topology.searchConfig.MaxEfSearch), DefaultVisitLimit: uint32(reader.topology.searchConfig.DefaultVisitLimit), MaxVisitLimit: uint32(reader.topology.searchConfig.MaxVisitLimit), MaxK: uint32(reader.topology.searchConfig.MaxK)},
		Build:   vhng.BuildInfo{BuildVersion: reader.topology.buildInfo.BuildVersion, LevelGeneratorVersion: reader.topology.buildInfo.LevelGeneratorVersion, MaxNeighbors: uint32(reader.topology.buildInfo.MaxNeighbors), LevelZeroMaxNeighbors: uint32(reader.topology.buildInfo.LevelZeroMaxNeighbors), EfConstruction: uint32(reader.topology.buildInfo.EfConstruction), Seed: reader.topology.buildInfo.Seed},
		Vectors: vhng.Reference{Size: vectors.Size, SHA256: vectors.SHA256}, NodeToVector: toUint32s(reader.topology.nodeToVector), Levels: levels,
		Level0Offsets: append([]uint32(nil), reader.topology.level0Offsets...), Level0Links: toUint32s(reader.topology.level0Neighbors), UpperNodeOffsets: append([]uint32(nil), reader.topology.upperNodeOffsets...), UpperLinkOffsets: append([]uint32(nil), reader.topology.upperLinkOffsets...), UpperLinks: toUint32s(reader.topology.upperNeighbors),
	}
}

func formatLimits(limits GraphLimits) vhng.Limits {
	return vhng.Limits{MaxGraphBytes: limits.MaxGraphBytes, MaxVectors: uint64(limits.MaxVectors), MaxLinks: limits.MaxLinks, MaxDimensions: uint64(limits.MaxDimensions), MaxLevel: uint64(limits.MaxLevel)}
}

func indexFromFormat(ctx context.Context, graph vhng.Graph, vectors vectorstore.PreparedVectorStore, limits GraphLimits) (*HNSWIndex, error) {
	dimensions := int(graph.Dimensions)
	metric := vector.Metric(graph.Metric)
	normalization := vector.Normalization(graph.Normalization)
	calculator, err := vector.NewCalculator(dimensions, metric)
	if err != nil || calculator.Normalization() != normalization {
		return nil, ErrCorruptGraphData
	}
	searchConfig := SearchConfig{DefaultEfSearch: int(graph.Search.DefaultEfSearch), MaxEfSearch: int(graph.Search.MaxEfSearch), DefaultVisitLimit: int(graph.Search.DefaultVisitLimit), MaxVisitLimit: int(graph.Search.MaxVisitLimit), MaxK: int(graph.Search.MaxK)}
	buildInfo := BuildInfo{BuildVersion: graph.Build.BuildVersion, LevelGeneratorVersion: graph.Build.LevelGeneratorVersion, MaxNeighbors: int(graph.Build.MaxNeighbors), LevelZeroMaxNeighbors: int(graph.Build.LevelZeroMaxNeighbors), EfConstruction: int(graph.Build.EfConstruction), Seed: graph.Build.Seed}
	if searchConfig.DefaultEfSearch > limits.MaxEfSearch || searchConfig.MaxEfSearch > limits.MaxEfSearch || searchConfig.DefaultVisitLimit > limits.MaxVisitLimit || searchConfig.MaxVisitLimit > limits.MaxVisitLimit || searchConfig.MaxK > limits.MaxK {
		return nil, ErrGraphLimitExceeded
	}
	if err := searchConfig.validate(); err != nil {
		return nil, ErrCorruptGraphData
	}
	if buildInfo.MaxNeighbors > limits.MaxNeighbors || buildInfo.LevelZeroMaxNeighbors > limits.MaxNeighbors*2 || buildInfo.EfConstruction > limits.MaxEfConstruction {
		return nil, ErrGraphLimitExceeded
	}
	if err := validatePersistedBuildInfo(buildInfo); err != nil {
		return nil, ErrCorruptGraphData
	}
	components, ok := checkedMultiply(uint64(len(graph.NodeToVector)), uint64(dimensions))
	vectorBytes, bytesOK := checkedMultiply(components, 4)
	if !ok || !bytesOK || vectorBytes > limits.MaxVectorBytes {
		return nil, ErrGraphLimitExceeded
	}
	reader := &HNSWIndex{topology: topology{calculator: calculator, searchConfig: searchConfig, buildInfo: buildInfo, nodeToVector: toOrdinals(graph.NodeToVector), levels: append([]uint8(nil), graph.Levels...), level0Offsets: append([]uint32(nil), graph.Level0Offsets...), level0Neighbors: toNodeOrdinals(graph.Level0Links), upperNodeOffsets: append([]uint32(nil), graph.UpperNodeOffsets...), upperLinkOffsets: append([]uint32(nil), graph.UpperLinkOffsets...), upperNeighbors: toNodeOrdinals(graph.UpperLinks)}, vectors: vectors}
	reader.topology.hasEntry, reader.topology.entry = graph.HasEntry, NodeOrdinal(graph.Entry)
	stats, err := validatePackedTopologyContext(ctx, reader)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrCorruptGraphData
	}
	if (stats.MaxLevel < 0 && graph.MaxLevel != 0xff) || (stats.MaxLevel >= 0 && graph.MaxLevel != uint8(stats.MaxLevel)) {
		return nil, ErrCorruptGraphData
	}
	if stats.MaxLevel > limits.MaxLevel {
		return nil, ErrGraphLimitExceeded
	}
	if vectors.Len() != reader.Len() || vectors.Dimensions() != dimensions || vectors.Metric() != metric || vectors.Normalization() != normalization {
		return nil, ErrGraphVectorStore
	}
	if err := validateHNSWIndexVectorsContext(ctx, reader); err != nil {
		return nil, err
	}
	reader.topology.stats, reader.topology.validated = cloneGraphStats(stats), true
	return reader, nil
}

func mapFormatError(err error) error {
	switch {
	case errors.Is(err, vhng.ErrCorruptData):
		return ErrCorruptGraphData
	case errors.Is(err, vhng.ErrUnsupportedVersion):
		return ErrUnsupportedGraphVersion
	case errors.Is(err, vhng.ErrLimitExceeded):
		return ErrGraphLimitExceeded
	case errors.Is(err, vhng.ErrReferenceMismatch):
		return ErrVectorFileRefMismatch
	default:
		return err
	}
}
func toUint32s[T ~uint32](values []T) []uint32 {
	result := make([]uint32, len(values))
	for i, value := range values {
		result[i] = uint32(value)
	}
	return result
}
func toOrdinals(values []uint32) []vector.Ordinal {
	result := make([]vector.Ordinal, len(values))
	for i, value := range values {
		result[i] = vector.Ordinal(value)
	}
	return result
}
func toNodeOrdinals(values []uint32) []NodeOrdinal {
	result := make([]NodeOrdinal, len(values))
	for i, value := range values {
		result[i] = NodeOrdinal(value)
	}
	return result
}
