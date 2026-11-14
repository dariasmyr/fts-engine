package hnsw

import "errors"

const (
	maxLevelLimit         = 63
	maxSupportedNeighbors = 1024
	maxEfConstruction     = 1_000_000

	buildVersion          uint32 = 3
	levelGeneratorVersion uint32 = 1
)

var ErrInvalidConfig = errors.New("vector/hnsw: invalid configuration")

// Config is the complete static HNSW configuration.
type Config struct {
	Build  BuildConfig
	Search SearchConfig
}

// Validate checks the complete static HNSW configuration.
func (c Config) Validate() error {
	if err := c.Build.Validate(); err != nil {
		return err
	}
	return c.Search.Validate()
}

// BuildConfig controls only HNSW graph construction.
// It affects the resulting topology.
type BuildConfig struct {
	MaxNeighbors   int
	EfConstruction int
	Seed           uint64
}

// Validate checks whether the graph construction configuration is supported by
// the HNSW implementation.
func (c BuildConfig) Validate() error {
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

// Validate checks whether the runtime search defaults are valid for HNSW.
func (c SearchConfig) Validate() error {
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
