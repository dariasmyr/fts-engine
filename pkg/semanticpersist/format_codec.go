package semanticpersist

import (
	"errors"
	"fmt"

	semanticformat "github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/format"
)

func codecLimits(limits Limits) semanticformat.Limits {
	return semanticformat.Limits{MaxFileBytes: limits.MaxFileBytes, MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxDocuments: limits.MaxDocuments, MaxStringBytes: limits.MaxStringBytes, MaxChunksPerDocument: limits.MaxChunksPerDocument, MaxK: limits.MaxK, MaxVectorBytes: limits.MaxVectorBytes, MaxEfSearch: limits.MaxEfSearch, MaxVisitLimit: limits.MaxVisitLimit}
}

func mapCodecError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, semanticformat.ErrCorrupt):
		return mapCodecErrorCategory(err, semanticformat.ErrCorrupt, ErrCorrupt)
	case errors.Is(err, semanticformat.ErrUnsupportedVersion):
		return mapCodecErrorCategory(err, semanticformat.ErrUnsupportedVersion, ErrUnsupportedVersion)
	case errors.Is(err, semanticformat.ErrLimitExceeded):
		return mapCodecErrorCategory(err, semanticformat.ErrLimitExceeded, ErrLimitExceeded)
	default:
		return err
	}
}

func mapCodecErrorCategory(err, internalCategory, publicCategory error) error {
	if err == internalCategory {
		return publicCategory
	}
	return fmt.Errorf("%w: %s", publicCategory, err)
}

func formatReference(value fileReference) semanticformat.FileReference {
	return semanticformat.FileReference{Size: value.Size, SHA256: value.SHA256}
}
func persistReference(value semanticformat.FileReference) fileReference {
	return fileReference{Size: value.Size, SHA256: value.SHA256}
}
