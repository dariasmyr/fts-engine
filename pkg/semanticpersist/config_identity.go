package semanticpersist

import "github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/generationformat"

func validateImmutableLineage(current, candidate generationformat.IndexState) error {
	if current.ID != candidate.ID {
		return ErrIndexIdentityMismatch
	}
	if current.Config != candidate.Config {
		return ErrImmutableConfigMismatch
	}
	if (current.Pipeline == nil) != (candidate.Pipeline == nil) {
		return ErrPersistenceModeMismatch
	}
	if current.Pipeline != nil && *current.Pipeline != *candidate.Pipeline {
		return ErrImmutableConfigMismatch
	}
	return nil
}
