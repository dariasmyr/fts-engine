package hnsw

import (
	"github.com/dariasmyr/fts-engine/internal/hnswmodel"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// Snapshot returns an internal, detached representation of index topology.
func Snapshot(index *Index) hnswmodel.Snapshot {
	if index == nil {
		return hnswmodel.Snapshot{}
	}

	t := index.topology
	return hnswmodel.Snapshot{
		Dimensions:            t.calculator.Dimensions(),
		Metric:                t.calculator.Metric(),
		Normalization:         t.calculator.Normalization(),
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
func Restore(snapshot hnswmodel.Snapshot, vectors vector.PreparedVectorStore, search SearchConfig) (*Index, error) {
	calculator, err := vector.NewCalculator(snapshot.Dimensions, snapshot.Metric)
	if err != nil || calculator.Normalization() != snapshot.Normalization {
		return nil, ErrInvalidSource
	}
	build := BuildConfig{
		MaxNeighbors:   snapshot.MaxNeighbors,
		EfConstruction: snapshot.EfConstruction,
		Seed:           snapshot.Seed,
	}
	if err := build.validate(); err != nil || snapshot.LevelZeroMaxNeighbors != snapshot.MaxNeighbors*2 ||
		snapshot.BuildVersion == 0 || snapshot.LevelGeneratorVersion == 0 {
		return nil, errInvalidGraph
	}

	t := topology{
		calculator: calculator,
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
	return newIndex(t, vectors, search)
}
