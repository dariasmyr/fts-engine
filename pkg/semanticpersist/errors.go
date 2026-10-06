package semanticpersist

import (
	"errors"
	"fmt"
)

var (
	ErrCorrupt            = errors.New("semanticpersist: corrupt data")
	ErrUnsupportedVersion = errors.New("semanticpersist: unsupported version")
	ErrLimitExceeded      = errors.New("semanticpersist: configured limit exceeded")
	ErrCurrentMissing     = errors.New("semanticpersist: CURRENT is missing")
	ErrPathEscape         = errors.New("semanticpersist: path escapes store root")
	ErrSymlink            = errors.New("semanticpersist: symlink is not allowed")
	ErrGenerationExists   = errors.New("semanticpersist: generation already exists")
	ErrIndeterminate      = errors.New("semanticpersist: publication outcome is indeterminate")
	ErrStoreLocked        = errors.New("semanticpersist: store is locked by another writer")
	ErrStoreClosed        = errors.New("semanticpersist: store is closed")
	ErrLockUnsupported    = errors.New("semanticpersist: writable store locking is unsupported on this platform")
	ErrStaleGeneration    = errors.New("semanticpersist: expected generation does not match CURRENT")
	ErrEmbeddingMismatch  = errors.New("semanticpersist: embedding descriptor mismatch")
	ErrChunkingMismatch   = errors.New("semanticpersist: chunking descriptor mismatch")
)

func codecLimits(limits Limits) Limits {
	return Limits{MaxFileBytes: limits.MaxFileBytes, MaxDimensions: limits.MaxDimensions, MaxVectors: limits.MaxVectors, MaxDocuments: limits.MaxDocuments, MaxStringBytes: limits.MaxStringBytes, MaxChunksPerDocument: limits.MaxChunksPerDocument, MaxK: limits.MaxK, MaxVectorBytes: limits.MaxVectorBytes, MaxEfSearch: limits.MaxEfSearch, MaxVisitLimit: limits.MaxVisitLimit}
}

func mapCodecError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrCorrupt):
		return mapCodecErrorCategory(err, ErrCorrupt, ErrCorrupt)
	case errors.Is(err, ErrUnsupportedVersion):
		return mapCodecErrorCategory(err, ErrUnsupportedVersion, ErrUnsupportedVersion)
	case errors.Is(err, ErrLimitExceeded):
		return mapCodecErrorCategory(err, ErrLimitExceeded, ErrLimitExceeded)
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

func formatReference(value fileReference) FileReference {
	return FileReference{Size: value.Size, SHA256: value.SHA256}
}
func persistReference(value FileReference) fileReference {
	return fileReference{Size: value.Size, SHA256: value.SHA256}
}
