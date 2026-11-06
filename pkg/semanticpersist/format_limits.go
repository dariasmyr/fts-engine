package semanticpersist

import (
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/hnswformat"
)

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
