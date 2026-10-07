package hnsw

import (
	"math"
)

// BuildConfig controls deterministic single-writer graph construction.
type BuildConfig struct {
	// MaxNeighbors is the standard HNSW M parameter. A new node selects at
	// most this many neighbors per level; level-0 reverse lists may grow to twice it.
	MaxNeighbors int
	// EfConstruction bounds the best candidate set retained while traversing
	// one level to choose MaxNeighbors links. It does not bound direct node degree.
	EfConstruction int
	// Seed makes level assignment deterministic for a fixed insertion order.
	Seed uint64
}

// BuildLimits bounds source cardinality and the builder's prepared-vector copy.
type BuildLimits struct {
	MaxVectors     int
	MaxVectorBytes uint64
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
	Build    BuildConfig
	Limits   BuildLimits
	Search   SearchConfig
	Progress func(BuildProgress)
}

// Validate checks graph/search parameters and row-count limits. Build validates
// the byte limit after deriving dimensions from its PreparedVectorStore.
func (o BuildOptions) Validate(vectorCount int) error {
	if err := o.Build.validate(); err != nil {
		return err
	}
	if err := o.Limits.validateCount(vectorCount); err != nil {
		return err
	}
	return o.Search.validate()
}

func (c BuildConfig) validate() error {
	return c.validateConstructionParameters()
}

func (l BuildLimits) validateCount(vectorCount int) error {
	if l.MaxVectors <= 0 || uint64(l.MaxVectors) >= math.MaxUint32 || vectorCount < 0 || vectorCount > l.MaxVectors || l.MaxVectorBytes == 0 {
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

func (l BuildLimits) validateVectorAllocation(vectorCount, dimensions int) (int, error) {
	if err := l.validateCount(vectorCount); err != nil || dimensions <= 0 {
		return 0, ErrInvalidBuildConfig
	}
	components, componentsOK := checkedMultiply(uint64(vectorCount), uint64(dimensions))
	vectorBytes, bytesOK := checkedMultiply(components, 4)
	if !componentsOK || !bytesOK || components > uint64(math.MaxInt) ||
		vectorBytes > uint64(math.MaxInt) || vectorBytes > l.MaxVectorBytes {
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
