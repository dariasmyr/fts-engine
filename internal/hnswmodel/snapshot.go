package hnswmodel

import "github.com/dariasmyr/fts-engine/pkg/vector"

// Snapshot is the repository-internal transfer form of immutable HNSW
// topology. Node positions are vector ordinals; no separate mapping exists.
type Snapshot struct {
	Dimensions    int
	Metric        vector.Metric
	Normalization vector.Normalization

	BuildVersion          uint32
	LevelGeneratorVersion uint32
	MaxNeighbors          int
	LevelZeroMaxNeighbors int
	EfConstruction        int
	Seed                  uint64

	Entry    uint32
	HasEntry bool

	Levels           []uint8
	Level0Offsets    []uint32
	Level0Links      []uint32
	UpperNodeOffsets []uint32
	UpperLinkOffsets []uint32
	UpperLinks       []uint32
}
