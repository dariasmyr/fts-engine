package semanticpersist

import (
	"errors"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/semanticpersist/internal/semanticformat"
)

func mapFormatError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, semanticformat.ErrCorrupt):
		if err == semanticformat.ErrCorrupt {
			return ErrCorrupt
		}
		return fmt.Errorf("%w: %s", ErrCorrupt, err)
	case errors.Is(err, semanticformat.ErrUnsupportedVersion):
		if err == semanticformat.ErrUnsupportedVersion {
			return ErrUnsupportedVersion
		}
		return fmt.Errorf("%w: %s", ErrUnsupportedVersion, err)
	case errors.Is(err, semanticformat.ErrLimitExceeded):
		if err == semanticformat.ErrLimitExceeded {
			return ErrLimitExceeded
		}
		return fmt.Errorf("%w: %s", ErrLimitExceeded, err)
	default:
		return err
	}
}
