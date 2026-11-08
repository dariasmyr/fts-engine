package semanticpersist

import (
	"math"

	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/hnswformat"
)

// Limits bounds persisted files, decoded data, and restored search settings.
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
	MaxK                 int
}

// DefaultLimits returns the validation limits used for zero-valued fields.
func DefaultLimits() Limits {
	return Limits{
		MaxFileBytes: 512 << 20, MaxVectorBytes: 512 << 20, MaxGraphBytes: 512 << 20,
		MaxGraphLinks: 100_000_000, MaxOpenBytes: 1 << 30, MaxEfSearch: 1_000_000,
		MaxVisitLimit: 10_000_000, MaxDimensions: 65_536, MaxVectors: 10_000_000,
		MaxDocuments: 10_000_000, MaxStringBytes: 1 << 20,
		MaxChunksPerDocument: 1_000_000, MaxK: 1_000_000,
	}
}

func normalizeLimits(limits Limits) Limits {
	defaults := DefaultLimits()
	limits.MaxFileBytes = defaultIfZero(limits.MaxFileBytes, defaults.MaxFileBytes)
	limits.MaxVectorBytes = defaultIfZero(limits.MaxVectorBytes, defaults.MaxVectorBytes)
	limits.MaxGraphBytes = defaultIfZero(limits.MaxGraphBytes, defaults.MaxGraphBytes)
	limits.MaxGraphLinks = defaultIfZero(limits.MaxGraphLinks, defaults.MaxGraphLinks)
	limits.MaxOpenBytes = defaultIfZero(limits.MaxOpenBytes, defaults.MaxOpenBytes)
	limits.MaxEfSearch = defaultIfZero(limits.MaxEfSearch, defaults.MaxEfSearch)
	limits.MaxVisitLimit = defaultIfZero(limits.MaxVisitLimit, defaults.MaxVisitLimit)
	limits.MaxDimensions = defaultIfZero(limits.MaxDimensions, defaults.MaxDimensions)
	limits.MaxVectors = defaultIfZero(limits.MaxVectors, defaults.MaxVectors)
	limits.MaxDocuments = defaultIfZero(limits.MaxDocuments, defaults.MaxDocuments)
	limits.MaxStringBytes = defaultIfZero(limits.MaxStringBytes, defaults.MaxStringBytes)
	limits.MaxChunksPerDocument = defaultIfZero(limits.MaxChunksPerDocument, defaults.MaxChunksPerDocument)
	limits.MaxK = defaultIfZero(limits.MaxK, defaults.MaxK)
	return limits
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
		limits.MaxChunksPerDocument <= 0 || limits.MaxK <= 0 ||
		uint64(limits.MaxVectors) >= math.MaxUint32 || uint64(limits.MaxDocuments) > math.MaxUint32 ||
		uint64(limits.MaxStringBytes) > math.MaxUint32 || uint64(limits.MaxChunksPerDocument) > math.MaxUint32 ||
		uint64(limits.MaxK) > math.MaxUint32 {
		return ErrLimitExceeded
	}
	return nil
}

func fileFormatLimits(limits Limits) generationformat.FileLimits {
	return generationformat.FileLimits{MaxFileBytes: limits.MaxFileBytes, MaxStringBytes: limits.MaxStringBytes}
}

func manifestFormatLimits(limits Limits) generationformat.ManifestLimits {
	// Every persisted segment contains at least one vector, so MaxVectors is a
	// conservative upper bound for the number of segments without expanding the public API.
	return generationformat.ManifestLimits{FileLimits: fileFormatLimits(limits), MaxSegments: limits.MaxVectors}
}

func stateFormatLimits(limits Limits) generationformat.StateLimits {
	return generationformat.StateLimits{
		FileLimits: fileFormatLimits(limits), MaxVectorBytes: limits.MaxVectorBytes,
		MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxSegments: limits.MaxVectors,
		MaxDocuments: limits.MaxDocuments, MaxChunksPerDocument: limits.MaxChunksPerDocument,
		MaxK: limits.MaxK, MaxEfSearch: limits.MaxEfSearch, MaxVisitLimit: limits.MaxVisitLimit,
	}
}

func vectorFormatLimits(limits Limits) hnswformat.VectorLimits {
	return hnswformat.VectorLimits{MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxVectorBytes: limits.MaxVectorBytes, MaxK: limits.MaxK}
}

func hnswSegmentLimits(limits Limits) hnswformat.SegmentLimits {
	return hnswformat.SegmentLimits{
		MaxFileBytes: limits.MaxFileBytes, MaxGraphBytes: limits.MaxGraphBytes,
		MaxVectorBytes: limits.MaxVectorBytes, MaxVectors: limits.MaxVectors,
		MaxGraphLinks: limits.MaxGraphLinks, MaxDimensions: limits.MaxDimensions,
	}
}
