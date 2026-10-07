package semanticpersist

import "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"

func fileFormatLimits(limits Limits) semanticformat.FileLimits {
	return semanticformat.FileLimits{MaxFileBytes: limits.MaxFileBytes, MaxStringBytes: limits.MaxStringBytes}
}
func manifestFormatLimits(limits Limits) semanticformat.ManifestLimits {
	// Every persisted segment contains at least one vector, so MaxVectors is a
	// conservative upper bound for the number of segments without expanding the public API.
	return semanticformat.ManifestLimits{FileLimits: fileFormatLimits(limits), MaxSegments: limits.MaxVectors}
}
func stateFormatLimits(limits Limits) semanticformat.StateLimits {
	return semanticformat.StateLimits{
		FileLimits: fileFormatLimits(limits), MaxVectorBytes: limits.MaxVectorBytes,
		MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxSegments: limits.MaxVectors,
		MaxDocuments: limits.MaxDocuments, MaxChunksPerDocument: limits.MaxChunksPerDocument,
		MaxK: limits.MaxK, MaxEfSearch: limits.MaxEfSearch, MaxVisitLimit: limits.MaxVisitLimit,
	}
}
func vectorFormatLimits(limits Limits) semanticformat.VectorLimits {
	return semanticformat.VectorLimits{MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxVectorBytes: limits.MaxVectorBytes, MaxK: limits.MaxK}
}
