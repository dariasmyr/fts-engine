package hnsw

import "errors"

const (
	maxLevelLimit         = 63
	maxSupportedNeighbors = 1024
	maxEfConstruction     = 1_000_000

	buildVersion          uint32 = 1
	levelGeneratorVersion uint32 = 1
)

var ErrInvalidConfig = errors.New("vector/hnsw: invalid configuration")

// BuildConfig controls only HNSW graph construction.
// It affects the resulting topology.
type BuildConfig struct {
	MaxNeighbors   int
	EfConstruction int
	Seed           uint64
}

func (c BuildConfig) validate() error {
	if c.MaxNeighbors < 2 || c.MaxNeighbors > maxSupportedNeighbors {
		return ErrInvalidConfig
	}
	if c.EfConstruction < c.MaxNeighbors || c.EfConstruction > maxEfConstruction {
		return ErrInvalidConfig
	}
	return nil
}

func (c BuildConfig) info() buildInfo {
	return buildInfo{
		buildVersion:          buildVersion,
		levelGeneratorVersion: levelGeneratorVersion,
		maxNeighbors:          c.MaxNeighbors,
		levelZeroMaxNeighbors: c.MaxNeighbors * 2,
		efConstruction:        c.EfConstruction,
		seed:                  c.Seed,
	}
}

// SearchConfig contains HNSW runtime defaults. It does not affect graph
// construction.
type SearchConfig struct {
	EfSearch   int
	VisitLimit int
}

func (c SearchConfig) validate() error {
	if c.EfSearch <= 0 || c.VisitLimit <= 0 {
		return ErrInvalidConfig
	}
	return nil
}

type buildInfo struct {
	buildVersion          uint32
	levelGeneratorVersion uint32
	maxNeighbors          int
	levelZeroMaxNeighbors int
	efConstruction        int
	seed                  uint64
}

func (i buildInfo) neighborLimit(level int) int {
	if level == 0 {
		return i.levelZeroMaxNeighbors
	}
	return i.maxNeighbors
}
