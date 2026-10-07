package hnsw

func calculateStorageStats(index *Index) StorageStats {
	stats := StorageStats{
		VectorRows:        index.Len(),
		GraphNodes:        index.Len(),
		DirectedLinks:     len(index.topology.level0Neighbors) + len(index.topology.upperNeighbors),
		VectorBytes:       uint64(index.Len()) * uint64(index.Dimensions()) * 4,
		NodeMetadataBytes: uint64(len(index.topology.levels)),
		OffsetBytes:       uint64(len(index.topology.level0Offsets)+len(index.topology.upperNodeOffsets)+len(index.topology.upperLinkOffsets)) * 4,
		LinkBytes:         uint64(len(index.topology.level0Neighbors)+len(index.topology.upperNeighbors)) * 4,
		GraphFileBytes:    encodedGraphSize(index),
	}
	for _, count := range index.topology.stats.LevelNodeCounts {
		stats.LevelPlacements += count
	}
	stats.TotalBytes = stats.VectorBytes + stats.NodeMetadataBytes + stats.OffsetBytes + stats.LinkBytes
	return stats
}

// Report returns a cached diagnostic snapshot. Slice fields are cloned so the
// immutable index cannot be mutated through the report.
func (i *Index) Report() Report {
	if i == nil {
		return Report{}
	}
	return Report{
		Build:   i.topology.buildInfo,
		Search:  i.search,
		Graph:   cloneGraphStats(i.topology.stats),
		Storage: i.storage,
	}
}

func cloneGraphStats(stats GraphStats) GraphStats {
	stats.LevelNodeCounts = append([]int(nil), stats.LevelNodeCounts...)
	stats.LevelLinkCounts = append([]int(nil), stats.LevelLinkCounts...)
	return stats
}
