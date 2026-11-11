package hnsw

import (
	"github.com/dariasmyr/fts-engine/v2/internal/hnswmodel"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// TopologyStats describes the HNSW topology without copying its CSR arrays.
type TopologyStats struct {
	NodeCount           int
	Level0LinkCount     int
	UpperLayerNodeCount int
	UpperLinkCount      int

	MaxNeighbors          int
	LevelZeroMaxNeighbors int
	EfConstruction        int
	Seed                  uint64
}

// TopologyStats returns allocation-free metadata about index topology.
func (i *Index) TopologyStats() TopologyStats {
	if i == nil {
		return TopologyStats{}
	}

	t := i.topology
	return TopologyStats{
		NodeCount:             len(t.levels),
		Level0LinkCount:       len(t.level0Neighbors),
		UpperLayerNodeCount:   len(t.upperLinkOffsets) - 1,
		UpperLinkCount:        len(t.upperNeighbors),
		MaxNeighbors:          t.build.maxNeighbors,
		LevelZeroMaxNeighbors: t.build.levelZeroMaxNeighbors,
		EfConstruction:        t.build.efConstruction,
		Seed:                  t.build.seed,
	}
}

// Snapshot returns an internal, detached representation of index topology.
func Snapshot(index *Index) hnswmodel.Snapshot {
	if index == nil {
		return hnswmodel.Snapshot{}
	}

	t := index.topology
	return hnswmodel.Snapshot{
		Dimensions:            t.space.Dimensions(),
		Metric:                t.space.Metric(),
		Normalization:         t.space.Normalization(),
		BuildVersion:          t.build.buildVersion,
		LevelGeneratorVersion: t.build.levelGeneratorVersion,
		MaxNeighbors:          t.build.maxNeighbors,
		LevelZeroMaxNeighbors: t.build.levelZeroMaxNeighbors,
		EfConstruction:        t.build.efConstruction,
		Seed:                  t.build.seed,
		Entry:                 t.entry,
		HasEntry:              t.hasEntry,
		Levels:                append([]uint8(nil), t.levels...),
		Level0Offsets:         append([]uint32(nil), t.level0Offsets...),
		Level0Links:           append([]uint32(nil), t.level0Neighbors...),
		UpperNodeOffsets:      append([]uint32(nil), t.upperNodeOffsets...),
		UpperLinkOffsets:      append([]uint32(nil), t.upperLinkOffsets...),
		UpperLinks:            append([]uint32(nil), t.upperNeighbors...),
	}
}

// Restore creates an in-memory index from a trusted, repository-internal
// snapshot. Durable decoders must validate untrusted data before calling it.
func Restore(snapshot hnswmodel.Snapshot, vectors vector.PreparedVectorStore, config Config) (*Index, error) {
	space, err := vector.NewSpace(snapshot.Dimensions, snapshot.Metric)
	if err != nil || space.Normalization() != snapshot.Normalization {
		return nil, ErrInvalidSource
	}
	build := BuildConfig{
		MaxNeighbors:   snapshot.MaxNeighbors,
		EfConstruction: snapshot.EfConstruction,
		Seed:           snapshot.Seed,
	}
	if err := config.Validate(); err != nil || config.Build != build ||
		snapshot.LevelZeroMaxNeighbors != snapshot.MaxNeighbors*2 ||
		snapshot.BuildVersion == 0 || snapshot.LevelGeneratorVersion == 0 {
		return nil, errInvalidGraph
	}

	t := topology{
		space: space,
		build: buildInfo{
			buildVersion:          snapshot.BuildVersion,
			levelGeneratorVersion: snapshot.LevelGeneratorVersion,
			maxNeighbors:          snapshot.MaxNeighbors,
			levelZeroMaxNeighbors: snapshot.LevelZeroMaxNeighbors,
			efConstruction:        snapshot.EfConstruction,
			seed:                  snapshot.Seed,
		},
		levels:           append([]uint8(nil), snapshot.Levels...),
		entry:            snapshot.Entry,
		hasEntry:         snapshot.HasEntry,
		level0Offsets:    append([]uint32(nil), snapshot.Level0Offsets...),
		level0Neighbors:  append([]uint32(nil), snapshot.Level0Links...),
		upperNodeOffsets: append([]uint32(nil), snapshot.UpperNodeOffsets...),
		upperLinkOffsets: append([]uint32(nil), snapshot.UpperLinkOffsets...),
		upperNeighbors:   append([]uint32(nil), snapshot.UpperLinks...),
	}
	return newIndex(t, vectors, config)
}
