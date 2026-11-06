package semanticpersist

import (
	"errors"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/hnswformat"
)

func mapFormatError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, generationformat.ErrCorrupt), errors.Is(err, hnswformat.ErrCorrupt), errors.Is(err, hnswformat.ErrCorruptData):
		if err == generationformat.ErrCorrupt || err == hnswformat.ErrCorrupt || err == hnswformat.ErrCorruptData {
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
