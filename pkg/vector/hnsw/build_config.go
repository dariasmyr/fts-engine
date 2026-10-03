package hnsw

import (
	"math"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

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
	Build    BuildConfig
	Search   SearchConfig
	Progress func(BuildProgress)
}

// Validate checks a complete build configuration for vectorCount source rows.
func (o BuildOptions) Validate(vectorCount int) error {
	if _, _, err := o.Build.validate(vectorCount); err != nil {
		return err
	}
	return o.Search.validate()
}

func (c BuildConfig) validate(vectorCount int) (vector.Calculator, int, error) {
	calculator, err := vector.NewCalculator(c.Dimensions, c.Metric)
	if err != nil {
		return vector.Calculator{}, 0, err
	}
	if err := c.validateCapacity(vectorCount); err != nil {
		return vector.Calculator{}, 0, err
	}
	if err := c.validateConstructionParameters(); err != nil {
		return vector.Calculator{}, 0, err
	}
	components, err := c.validateVectorAllocation(vectorCount)
	if err != nil {
		return vector.Calculator{}, 0, err
	}
	return calculator, components, nil
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
