package semanticpersist

import (
	"errors"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/internal/persistfs"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/hnswformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
)

var (
	ErrCorrupt                    = errors.New("semanticpersist: corrupt data")
	ErrUnsupportedVersion         = errors.New("semanticpersist: unsupported version")
	ErrLimitExceeded              = errors.New("semanticpersist: configured limit exceeded")
	ErrCurrentMissing             = errors.New("semanticpersist: CURRENT is missing")
	ErrPathEscape                 = errors.New("semanticpersist: path escapes persistence root")
	ErrSymlink                    = storage.ErrSymlink
	ErrGenerationExists           = errors.New("semanticpersist: generation already exists")
	ErrGenerationExhausted        = errors.New("semanticpersist: generation ID exhausted")
	ErrIndeterminate              = errors.New("semanticpersist: save outcome is indeterminate")
	ErrLocked                     = storage.ErrLocked
	ErrLockUnsupported            = storage.ErrLockUnsupported
	ErrExpectedGenerationRequired = errors.New("semanticpersist: expected generation is required")
	ErrStaleGeneration            = errors.New("semanticpersist: expected generation does not match CURRENT")
	ErrIndexIdentityMismatch      = errors.New("semanticpersist: index identity does not match persisted lineage")
	ErrImmutableConfigMismatch    = errors.New("semanticpersist: immutable configuration does not match persisted lineage")
	ErrPersistenceModeMismatch    = errors.New("semanticpersist: persistence mode does not match persisted lineage")
)

func indeterminateError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrIndeterminate, err)
}

func mapFormatError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, generationformat.ErrCorrupt), errors.Is(err, hnswformat.ErrCorrupt):
		if err == generationformat.ErrCorrupt || err == hnswformat.ErrCorrupt {
			return ErrCorrupt
		}
		return fmt.Errorf("%w: %s", ErrCorrupt, err)
	case errors.Is(err, generationformat.ErrUnsupportedVersion), errors.Is(err, hnswformat.ErrUnsupportedVersion):
		if err == generationformat.ErrUnsupportedVersion || err == hnswformat.ErrUnsupportedVersion {
			return ErrUnsupportedVersion
		}
		return fmt.Errorf("%w: %s", ErrUnsupportedVersion, err)
	case errors.Is(err, generationformat.ErrLimitExceeded), errors.Is(err, hnswformat.ErrLimitExceeded):
		if err == generationformat.ErrLimitExceeded || err == hnswformat.ErrLimitExceeded {
			return ErrLimitExceeded
		}
		return fmt.Errorf("%w: %s", ErrLimitExceeded, err)
	default:
		return err
	}
}

func mapPersistFSError(err, invalidType error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, persistfs.ErrSymlink):
		return ErrSymlink
	case errors.Is(err, persistfs.ErrLimitExceeded):
		return ErrLimitExceeded
	case errors.Is(err, persistfs.ErrNotDirectory), errors.Is(err, persistfs.ErrNotRegular):
		return invalidType
	default:
		return err
	}
}
