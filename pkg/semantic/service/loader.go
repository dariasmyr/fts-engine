package service

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

// RestoreMetadata is the immutable identity and configuration snapshot checked
// before a Loader opens the index's segment objects.
type RestoreMetadata struct {
	IndexID      semantic.IndexID
	GenerationID uint64
	IndexConfig  semantic.IndexConfig
	Pipeline     PipelineConfig
}

// RestorePreflight validates persisted metadata before a Loader opens the
// index's segment objects.
type RestorePreflight func(RestoreMetadata) error

// LoadedState is the result of one consistent service restore operation.
type LoadedState struct {
	Index        *semantic.Index
	Pipeline     PipelineConfig
	GenerationID uint64
}

// Loader restores a model-backed service state. Implementations must invoke
// preflight exactly once for the same metadata snapshot used to restore Index.
type Loader interface {
	LoadServiceState(context.Context, RestorePreflight) (LoadedState, error)
}
