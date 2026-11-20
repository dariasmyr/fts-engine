package semanticpersist

import (
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/hnswformat"
)

// Limits bounds persisted files and decoded data for one operation.
// Zero fields are replaced by DefaultLimits.
type Limits struct {
	MaxFileBytes         uint64
	MaxVectorBytes       uint64
	MaxGraphBytes        uint64
	MaxGraphLinks        uint64
	MaxOpenBytes         uint64
	MaxEfSearch          int
	MaxVisitLimit        int
	MaxDimensions        int
	MaxVectors           int
	MaxDocuments         int
	MaxStringBytes       int
	MaxChunksPerDocument int
	MaxQueryChunks       int
	MaxK                 int
}

// SaveOptions configures one Save operation.
// ExpectedGeneration is required for optimistic concurrency control.
type SaveOptions struct {
	// Limits bounds files and decoded data. Zero fields use DefaultLimits.
	Limits Limits
	// ExpectedGeneration must match CURRENT. Use a zero Generation for the first
	// save and the value returned by Load or the previous save thereafter.
	ExpectedGeneration *Generation
}

// LoadOptions configures one Load operation.
type LoadOptions struct {
	// Limits bounds files and decoded data. Zero fields use DefaultLimits.
	Limits Limits
}

// PruneOptions configures one Prune operation.
type PruneOptions struct {
	// RetainGenerations is the total number of valid committed generations to
	// retain, including the generation named by CURRENT. It must be positive.
	RetainGenerations int
	Limits            Limits
}

// RecoverOptions configures one RecoverGeneration operation.
type RecoverOptions struct {
	Limits     Limits
	Generation Generation
}

type settings struct {
	limits      Limits
	checkpoints saveCheckpoints
}

func newSettings(limits Limits) (settings, error) {
	limits, err := limits.normalized()
	if err != nil {
		return settings{}, err
	}
	return settings{limits: limits}, nil
}

// DefaultLimits returns the validation limits used for zero-valued fields.
func DefaultLimits() Limits {
	maxWireInt := int(^uint(0) >> 1)
	if uint64(maxWireInt) > math.MaxUint32 {
		maxWireInt = int(math.MaxUint32)
	}
	maxVectorCount := maxWireInt
	if uint64(maxVectorCount) == math.MaxUint32 {
		maxVectorCount--
	}
	return Limits{
		MaxFileBytes: 512 << 20, MaxVectorBytes: math.MaxUint64 - 128, MaxGraphBytes: 512 << 20,
		MaxGraphLinks: math.MaxUint64, MaxOpenBytes: 1 << 30, MaxEfSearch: maxWireInt,
		MaxVisitLimit: maxWireInt, MaxDimensions: maxWireInt, MaxVectors: maxVectorCount,
		MaxDocuments: maxWireInt, MaxStringBytes: maxWireInt,
		MaxChunksPerDocument: maxWireInt, MaxQueryChunks: maxWireInt, MaxK: maxWireInt,
	}
}

// Validate checks the effective static persistence limits. Zero fields select
// values from DefaultLimits.
func (c Limits) Validate() error {
	_, err := c.normalized()
	return err
}

func (c Limits) normalized() (Limits, error) {
	defaults := DefaultLimits()
	c.MaxFileBytes = defaultIfZero(c.MaxFileBytes, defaults.MaxFileBytes)
	c.MaxVectorBytes = defaultIfZero(c.MaxVectorBytes, defaults.MaxVectorBytes)
	c.MaxGraphBytes = defaultIfZero(c.MaxGraphBytes, defaults.MaxGraphBytes)
	c.MaxGraphLinks = defaultIfZero(c.MaxGraphLinks, defaults.MaxGraphLinks)
	c.MaxOpenBytes = defaultIfZero(c.MaxOpenBytes, defaults.MaxOpenBytes)
	c.MaxEfSearch = defaultIfZero(c.MaxEfSearch, defaults.MaxEfSearch)
	c.MaxVisitLimit = defaultIfZero(c.MaxVisitLimit, defaults.MaxVisitLimit)
	c.MaxDimensions = defaultIfZero(c.MaxDimensions, defaults.MaxDimensions)
	c.MaxVectors = defaultIfZero(c.MaxVectors, defaults.MaxVectors)
	c.MaxDocuments = defaultIfZero(c.MaxDocuments, defaults.MaxDocuments)
	c.MaxStringBytes = defaultIfZero(c.MaxStringBytes, defaults.MaxStringBytes)
	c.MaxChunksPerDocument = defaultIfZero(c.MaxChunksPerDocument, defaults.MaxChunksPerDocument)
	c.MaxQueryChunks = defaultIfZero(c.MaxQueryChunks, defaults.MaxQueryChunks)
	c.MaxK = defaultIfZero(c.MaxK, defaults.MaxK)
	if err := validateLimits(c); err != nil {
		return Limits{}, err
	}
	return c, nil
}

func defaultIfZero[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}

func validateLimits(limits Limits) error {
	if limits.MaxFileBytes == 0 || limits.MaxFileBytes >= math.MaxInt64 ||
		limits.MaxVectorBytes == 0 || limits.MaxVectorBytes > math.MaxUint64-128 ||
		limits.MaxGraphBytes == 0 || limits.MaxGraphBytes >= math.MaxInt64 || limits.MaxGraphLinks == 0 ||
		limits.MaxOpenBytes < limits.MaxFileBytes || limits.MaxOpenBytes >= math.MaxInt64 ||
		limits.MaxEfSearch <= 0 || limits.MaxVisitLimit <= 0 || limits.MaxDimensions <= 0 ||
		limits.MaxVectors <= 0 || limits.MaxDocuments <= 0 || limits.MaxStringBytes <= 0 ||
		limits.MaxChunksPerDocument <= 0 || limits.MaxQueryChunks <= 0 || limits.MaxK <= 0 ||
		uint64(limits.MaxVectors) >= math.MaxUint32 || uint64(limits.MaxDocuments) > math.MaxUint32 ||
		uint64(limits.MaxStringBytes) > math.MaxUint32 || uint64(limits.MaxChunksPerDocument) > math.MaxUint32 ||
		uint64(limits.MaxQueryChunks) > math.MaxUint32 ||
		uint64(limits.MaxK) > math.MaxUint32 {
		return ErrLimitExceeded
	}
	return nil
}

func generationFormatConfig(limits Limits) generationformat.Config {
	// Every persisted segment contains at least one vector, so MaxVectors is a
	// conservative upper bound for the number of segments without expanding the public API.
	return generationformat.Config{
		MaxFileBytes: limits.MaxFileBytes, MaxStringBytes: limits.MaxStringBytes,
		MaxVectorBytes: limits.MaxVectorBytes, MaxDimensions: limits.MaxDimensions,
		MaxVectors: limits.MaxVectors, MaxSegments: limits.MaxVectors,
		MaxDocuments: limits.MaxDocuments, MaxChunksPerDocument: limits.MaxChunksPerDocument,
		MaxQueryChunks: limits.MaxQueryChunks,
		MaxK:           limits.MaxK, MaxEfSearch: limits.MaxEfSearch, MaxVisitLimit: limits.MaxVisitLimit,
	}
}

func hnswFormatConfig(limits Limits) hnswformat.Config {
	return hnswformat.Config{
		MaxFileBytes: limits.MaxFileBytes, MaxGraphBytes: limits.MaxGraphBytes,
		MaxVectorBytes: limits.MaxVectorBytes, MaxVectors: limits.MaxVectors,
		MaxGraphLinks: limits.MaxGraphLinks, MaxDimensions: limits.MaxDimensions,
		MaxK: limits.MaxK,
	}
}
