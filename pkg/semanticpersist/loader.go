package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
)

// Loader restores model-backed service state from a filesystem persistence
// root. One LoadServiceState call reads and restores one generation while
// holding the root's operation lock.
type Loader struct {
	root    string
	options LoadOptions
}

var _ service.Loader = (*Loader)(nil)

// NewLoader validates and captures one filesystem load configuration. It does
// not access root; filesystem I/O starts in LoadServiceState.
func NewLoader(root string, options LoadOptions) (*Loader, error) {
	if root == "" {
		return nil, ErrCorrupt
	}
	if err := options.Limits.Validate(); err != nil {
		return nil, err
	}
	return &Loader{root: root, options: options}, nil
}

// LoadServiceState restores one metadata and segment snapshot under one
// operation lock. Preflight runs after metadata validation and before segment
// objects are opened.
func (l *Loader) LoadServiceState(
	ctx context.Context,
	preflight service.RestorePreflight,
) (service.LoadedState, error) {
	if l == nil {
		return service.LoadedState{}, service.ErrInvalidLoadedState
	}
	cfg, err := prepareLoad(ctx, l.root, l.options)
	if err != nil {
		return service.LoadedState{}, err
	}
	if preflight == nil {
		return service.LoadedState{}, service.ErrInvalidLoadedState
	}

	return withStorageOperationResult(l.root, operationLayoutExisting, func(operation *storageOperation) (service.LoadedState, error) {
		persisted, generation, err := openCurrentGeneration(ctx, operation.layout, cfg)
		if err != nil {
			return service.LoadedState{}, err
		}
		indexConfig, err := indexConfigFromFormat(persisted.state.Config)
		if err != nil {
			return service.LoadedState{}, mapFormatError(err)
		}
		pipeline, err := optionalPipelineFromFormat(persisted.state.Pipeline, indexConfig)
		if err != nil {
			return service.LoadedState{}, mapFormatError(err)
		}
		if pipeline == nil {
			return service.LoadedState{}, service.ErrModelServiceUnavailable
		}
		metadata := service.RestoreMetadata{
			IndexID:      semantic.IndexID(persisted.state.ID),
			GenerationID: generation.ID,
			IndexConfig:  indexConfig,
			Pipeline:     *pipeline,
		}
		if err := preflight(metadata); err != nil {
			return service.LoadedState{}, err
		}

		index, err := restoreIndex(ctx, persisted, newSegmentStore(operation.layout, cfg))
		if err != nil {
			return service.LoadedState{}, corruptMissingReference(err)
		}
		if index.ID() != metadata.IndexID || index.Config() != indexConfig {
			return service.LoadedState{}, service.ErrInvalidLoadedState
		}
		return service.LoadedState{
			Index:        index,
			Pipeline:     *pipeline,
			GenerationID: generation.ID,
		}, nil
	})
}
