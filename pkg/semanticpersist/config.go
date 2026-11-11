package semanticpersist

import (
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/hnswformat"
)

// Config bounds persisted files, decoded data, and restored search settings.
// Zero fields are replaced by DefaultConfig.
type Config struct {
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
	MaxK                 int
}

// SaveConfig configures one Save operation.
// ExpectedGeneration is required for optimistic concurrency control.
type SaveConfig struct {
	// Config bounds files and decoded data. Zero fields use DefaultConfig.
	Config Config
	// ExpectedGeneration must match CURRENT. Use a zero Generation for the first
	// save and the value returned by Load or the previous save thereafter.
	ExpectedGeneration *Generation
}

// LoadConfig configures one Load operation.
type LoadConfig struct {
	// Config bounds files and decoded data. Zero fields use DefaultConfig.
	Config Config
}

// PruneConfig configures one Prune operation.
type PruneConfig struct {
	// RetainGenerations is the total number of valid committed generations to
	// retain, including the generation named by CURRENT. It must be positive.
	RetainGenerations int
	Config            Config
}

// RecoverConfig configures one RecoverGeneration operation.
type RecoverConfig struct {
	Config     Config
	Generation Generation
}

// DefaultConfig returns the validation limits used for zero-valued fields.
func DefaultConfig() Config {
	return Config{
		MaxFileBytes: 512 << 20, MaxVectorBytes: 512 << 20, MaxGraphBytes: 512 << 20,
		MaxGraphLinks: 100_000_000, MaxOpenBytes: 1 << 30, MaxEfSearch: 1_000_000,
		MaxVisitLimit: 10_000_000, MaxDimensions: 65_536, MaxVectors: 10_000_000,
		MaxDocuments: 10_000_000, MaxStringBytes: 1 << 20,
		MaxChunksPerDocument: 1_000_000, MaxK: 1_000_000,
	}
}

// Validate checks the effective static persistence configuration. Zero fields
// select values from DefaultConfig.
func (c Config) Validate() error {
	_, err := c.normalized()
	return err
}

func (c Config) normalized() (Config, error) {
	defaults := DefaultConfig()
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
	c.MaxK = defaultIfZero(c.MaxK, defaults.MaxK)
	if err := validateConfig(c); err != nil {
		return Config{}, err
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

func validateConfig(limits Config) error {
	if limits.MaxFileBytes == 0 || limits.MaxFileBytes >= math.MaxInt64 ||
		limits.MaxVectorBytes == 0 || limits.MaxVectorBytes > math.MaxUint64-128 ||
		limits.MaxGraphBytes == 0 || limits.MaxGraphBytes >= math.MaxInt64 || limits.MaxGraphLinks == 0 ||
		limits.MaxOpenBytes < limits.MaxFileBytes || limits.MaxOpenBytes >= math.MaxInt64 ||
		limits.MaxEfSearch <= 0 || limits.MaxVisitLimit <= 0 || limits.MaxDimensions <= 0 ||
		limits.MaxVectors <= 0 || limits.MaxDocuments <= 0 || limits.MaxStringBytes <= 0 ||
		limits.MaxChunksPerDocument <= 0 || limits.MaxK <= 0 ||
		uint64(limits.MaxVectors) >= math.MaxUint32 || uint64(limits.MaxDocuments) > math.MaxUint32 ||
		uint64(limits.MaxStringBytes) > math.MaxUint32 || uint64(limits.MaxChunksPerDocument) > math.MaxUint32 ||
		uint64(limits.MaxK) > math.MaxUint32 {
		return ErrLimitExceeded
	}
	return nil
}

func generationFormatConfig(config Config) generationformat.Config {
	// Every persisted segment contains at least one vector, so MaxVectors is a
	// conservative upper bound for the number of segments without expanding the public API.
	return generationformat.Config{
		MaxFileBytes: config.MaxFileBytes, MaxStringBytes: config.MaxStringBytes,
		MaxVectorBytes: config.MaxVectorBytes, MaxDimensions: config.MaxDimensions,
		MaxVectors: config.MaxVectors, MaxSegments: config.MaxVectors,
		MaxDocuments: config.MaxDocuments, MaxChunksPerDocument: config.MaxChunksPerDocument,
		MaxK: config.MaxK, MaxEfSearch: config.MaxEfSearch, MaxVisitLimit: config.MaxVisitLimit,
	}
}

func hnswFormatConfig(config Config) hnswformat.Config {
	return hnswformat.Config{
		MaxFileBytes: config.MaxFileBytes, MaxGraphBytes: config.MaxGraphBytes,
		MaxVectorBytes: config.MaxVectorBytes, MaxVectors: config.MaxVectors,
		MaxGraphLinks: config.MaxGraphLinks, MaxDimensions: config.MaxDimensions,
		MaxK: config.MaxK,
	}
}
