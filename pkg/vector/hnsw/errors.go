package hnsw

import "errors"

var (
	ErrInvalidSearchConfig = errors.New("vector/hnsw: invalid search configuration")
	ErrInvalidBuildConfig  = errors.New("vector/hnsw: invalid build configuration")
	ErrBuildSourceMismatch = errors.New("vector/hnsw: build source metadata mismatch")
	errInvalidGraph        = errors.New("vector/hnsw: invalid graph")
	errCapacityExceeded    = errors.New("vector/hnsw: builder capacity exceeded")
)

// MaxLevel is the largest level accepted by the search primitives.
const MaxLevel = 63

const (
	// BuildVersion identifies insertion, selection, and pruning semantics.
	BuildVersion uint32 = 1
	// LevelGeneratorVersion identifies the deterministic PRNG and level mapping.
	LevelGeneratorVersion uint32 = 1
	// MaxSupportedNeighbors bounds configured MaxNeighbors. The derived level-0
	// reverse-list limit is twice this value.
	MaxSupportedNeighbors = 1024
	// MaxEfConstruction is a defensive bound on construction-search memory.
	MaxEfConstruction = 1_000_000
)
