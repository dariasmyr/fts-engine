package semanticpersist

import (
	"errors"

	semanticformat "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/format"
)

func codecLimits(limits Limits) semanticformat.Limits {
	return semanticformat.Limits{MaxFileBytes: limits.MaxFileBytes, MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxDocuments: limits.MaxDocuments, MaxStringBytes: limits.MaxStringBytes, MaxChunksPerDocument: limits.MaxChunksPerDocument, MaxK: limits.MaxK}
}

func mapCodecError(err error) error {
	switch {
	case errors.Is(err, semanticformat.ErrCorrupt):
		return ErrCorrupt
	case errors.Is(err, semanticformat.ErrUnsupportedVersion):
		return ErrUnsupportedVersion
	case errors.Is(err, semanticformat.ErrLimitExceeded):
		return ErrLimitExceeded
	default:
		return err
	}
}

func formatReference(value fileReference) semanticformat.FileReference {
	return semanticformat.FileReference{Size: value.Size, SHA256: value.SHA256}
}
func persistReference(value semanticformat.FileReference) fileReference {
	return fileReference{Size: value.Size, SHA256: value.SHA256}
}
