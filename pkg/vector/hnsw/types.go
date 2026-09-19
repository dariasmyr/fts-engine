// Package hnsw provides search primitives for hierarchical navigable small
// world graphs.
package hnsw

import (
	"errors"
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

var (
	ErrInvalidSearchConfig = errors.New("vector/hnsw: invalid search configuration")
	ErrInvalidBuildConfig  = errors.New("vector/hnsw: invalid build configuration")
	ErrBuildSourceMismatch = errors.New("vector/hnsw: build source metadata mismatch")
	ErrInvalidGraph        = errors.New("vector/hnsw: invalid graph")
	ErrCapacityExceeded    = errors.New("vector/hnsw: builder capacity exceeded")
	ErrDuplicateOrdinal    = errors.New("vector/hnsw: vector ordinal already added")
	ErrBuilderIncomplete   = errors.New("vector/hnsw: builder does not contain every vector ordinal")
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

// NodeOrdinal is a dense graph-local node number. It is intentionally distinct
// from vector.Ordinal even when one graph node maps to one vector row.
type NodeOrdinal uint32

// SearchConfig bounds request-local HNSW search work and allocations.
type SearchConfig struct {
	// DefaultEfSearch is used when SearchOptions.EfSearch is zero.
	DefaultEfSearch int
	// MaxEfSearch bounds the accepted request value and result heap.
	MaxEfSearch int
	// DefaultVisitLimit is used when SearchOptions.VisitLimit is zero.
	DefaultVisitLimit int
	// MaxVisitLimit bounds uniquely scored graph nodes per request.
	MaxVisitLimit int
	// MaxK bounds the number of returned hits.
	MaxK int
}

// BuildConfig controls deterministic single-writer graph construction.
type BuildConfig struct {
	// Dimensions and Metric define the vector space shared by stored vectors and queries.
	Dimensions int
	Metric     vector.Metric
	// MaxVectors and MaxVectorBytes bound the preallocated vector matrix.
	MaxVectors     int
	MaxVectorBytes uint64
	// MaxNeighbors is the standard HNSW M parameter. A new node selects at
	// most this many neighbors per level; level-0 reverse lists may grow to twice it.
	MaxNeighbors int
	// EfConstruction bounds the best candidate set retained while traversing
	// one level to choose MaxNeighbors links. It does not bound direct node degree.
	EfConstruction int
	// Seed makes level assignment deterministic for a fixed insertion order.
	Seed uint64
}

type BuildPhase string

const (
	BuildPhasePreflight BuildPhase = "preflight"
	BuildPhaseVectors   BuildPhase = "vectors"
	BuildPhaseFreeze    BuildPhase = "freeze"
	BuildPhaseComplete  BuildPhase = "complete"
)

type BuildProgress struct {
	Phase     BuildPhase
	Completed int
	Total     int
}

// BuildOptions configures a complete synchronous build. Progress, when set, is
// invoked synchronously by Build and must return before the build continues.
type BuildOptions struct {
	BuildConfig  BuildConfig
	SearchConfig SearchConfig
	Progress     func(BuildProgress)
}

// BuildInfo records graph-construction provenance. Exact links, not provenance,
// remain authoritative after freezing or persistence.
type BuildInfo struct {
	BuildVersion          uint32
	LevelGeneratorVersion uint32
	MaxNeighbors          int
	LevelZeroMaxNeighbors int
	EfConstruction        int
	Seed                  uint64
}

// GraphStats reports structural cardinalities and directed level-0 reachability.
type GraphStats struct {
	NodeCount        int
	VectorCount      int
	MaxLevel         int
	LevelNodeCounts  []int
	LevelLinkCounts  []int
	ReachableNodes   int
	UnreachableNodes int
	ZeroDegreeNodes  int
}

// StorageStats reports logical in-memory cardinalities and packed byte counts.
type StorageStats struct {
	VectorRows        int
	GraphNodes        int
	LevelPlacements   int
	DirectedLinks     int
	VectorBytes       uint64
	NodeMetadataBytes uint64
	OffsetBytes       uint64
	LinkBytes         uint64
	TotalBytes        uint64
}

func (c SearchConfig) validate() error {
	if c.DefaultEfSearch <= 0 || c.MaxEfSearch < c.DefaultEfSearch {
		return ErrInvalidSearchConfig
	}
	if c.DefaultVisitLimit <= 0 || c.MaxVisitLimit < c.DefaultVisitLimit {
		return ErrInvalidSearchConfig
	}
	if c.MaxK <= 0 || c.MaxK > c.MaxEfSearch {
		return ErrInvalidSearchConfig
	}
	return nil
}

func (c BuildConfig) validate(vectorCount int) (vector.Space, int, error) {
	space, err := vector.NewSpace(c.Dimensions, c.Metric)
	if err != nil {
		return vector.Space{}, 0, err
	}
	if err := c.validateCapacity(vectorCount); err != nil {
		return vector.Space{}, 0, err
	}
	if err := c.validateConstructionParameters(); err != nil {
		return vector.Space{}, 0, err
	}
	components, err := c.validateVectorAllocation(vectorCount)
	if err != nil {
		return vector.Space{}, 0, err
	}
	return space, components, nil
}

func (c BuildConfig) validateCapacity(vectorCount int) error {
	if c.MaxVectors <= 0 || uint64(c.MaxVectors) >= math.MaxUint32 || vectorCount < 0 || vectorCount > c.MaxVectors {
		return ErrInvalidBuildConfig
	}
	return nil
}

func (c BuildConfig) validateConstructionParameters() error {
	if c.MaxNeighbors < 2 || c.MaxNeighbors > MaxSupportedNeighbors || c.MaxNeighbors > math.MaxInt/2 {
		return ErrInvalidBuildConfig
	}
	if c.EfConstruction < c.MaxNeighbors || c.EfConstruction > MaxEfConstruction {
		return ErrInvalidBuildConfig
	}
	return nil
}

func (c BuildConfig) validateVectorAllocation(vectorCount int) (int, error) {
	if c.MaxVectorBytes == 0 {
		return 0, ErrInvalidBuildConfig
	}
	components, componentsOK := checkedMultiply(uint64(vectorCount), uint64(c.Dimensions))
	vectorBytes, bytesOK := checkedMultiply(components, 4)
	if !componentsOK || !bytesOK || components > uint64(math.MaxInt) ||
		vectorBytes > uint64(math.MaxInt) || vectorBytes > c.MaxVectorBytes {
		return 0, ErrInvalidBuildConfig
	}
	return int(components), nil
}

func checkedMultiply(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func (c BuildConfig) info() BuildInfo {
	return BuildInfo{
		BuildVersion: BuildVersion, LevelGeneratorVersion: LevelGeneratorVersion,
		MaxNeighbors: c.MaxNeighbors, LevelZeroMaxNeighbors: c.MaxNeighbors * 2,
		EfConstruction: c.EfConstruction, Seed: c.Seed,
	}
}

func (i BuildInfo) neighborLimit(level int) int {
	if level == 0 {
		return i.LevelZeroMaxNeighbors
	}
	return i.MaxNeighbors
}

type searchCandidate struct {
	node          NodeOrdinal
	vectorOrdinal vector.Ordinal
	distance      float64
	accepted      bool
}
