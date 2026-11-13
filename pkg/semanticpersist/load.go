package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Load restores the current generation fully into memory and returns its token
// for the next optimistic Save.
func Load(
	ctx context.Context,
	root string,
	options LoadOptions,
) (*semantic.Index, Generation, error) {
	index, generation, _, err := load(ctx, root, options)
	return index, generation, err
}

func load(
	ctx context.Context,
	root string,
	options LoadOptions,
) (*semantic.Index, Generation, persistedGeneration, error) {
	cfg, err := prepareLoad(ctx, root, options)
	if err != nil {
		return nil, Generation{}, persistedGeneration{}, err
	}

	loaded, err := withStorageOperationResult(root, operationLayoutExisting, func(operation *storageOperation) (loadOperationResult, error) {
		persisted, generation, err := openCurrentGeneration(ctx, operation.layout, cfg)
		if err != nil {
			return loadOperationResult{}, err
		}
		index, err := restoreIndex(
			ctx,
			persisted,
			newSegmentStore(operation.layout, cfg),
		)
		if err != nil {
			return loadOperationResult{}, corruptMissingReference(err)
		}
		return loadOperationResult{
			index:      index,
			generation: generation,
			persisted:  persisted,
		}, nil
	})
	if err != nil {
		return nil, Generation{}, persistedGeneration{}, err
	}
	return loaded.index, loaded.generation, loaded.persisted, nil
}

func prepareLoad(ctx context.Context, root string, options LoadOptions) (settings, error) {
	if ctx == nil {
		return settings{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return settings{}, err
	}
	if root == "" {
		return settings{}, ErrCorrupt
	}
	return newSettings(options.Limits)
}

func openCurrentGeneration(
	ctx context.Context,
	l layout,
	cfg settings,
) (persistedGeneration, Generation, error) {
	head, err := newCurrentFile(l, cfg).read(ctx)
	if err != nil {
		return persistedGeneration{}, Generation{}, err
	}
	persisted, err := newGenerationFiles(l, cfg).open(
		ctx,
		head.GenerationID,
		&head.ManifestHash,
	)
	if err != nil {
		return persistedGeneration{}, Generation{}, corruptMissingReference(err)
	}
	return persisted, Generation{ID: head.GenerationID}, nil
}

type loadOperationResult struct {
	index      *semantic.Index
	generation Generation
	persisted  persistedGeneration
}

// LoadService restores the saved model-backed service configuration and binds
// client after verifying its model contract. It returns the generation token
// for the next optimistic save. External encoders use Load and NewService.
func LoadService(
	ctx context.Context,
	root string,
	client embedding.Client,
	options LoadOptions,
) (*service.Service, Generation, error) {
	cfg, err := prepareLoad(ctx, root, options)
	if err != nil {
		return nil, Generation{}, err
	}
	loaded, err := withStorageOperationResult(root, operationLayoutExisting, func(operation *storageOperation) (loadServiceOperationResult, error) {
		persisted, generation, err := openCurrentGeneration(ctx, operation.layout, cfg)
		if err != nil {
			return loadServiceOperationResult{}, err
		}
		indexConfig, err := indexConfigFromFormat(persisted.state.Config)
		if err != nil {
			return loadServiceOperationResult{}, mapFormatError(err)
		}
		pipeline, err := optionalPipelineFromFormat(persisted.state.Pipeline, indexConfig)
		if err != nil {
			return loadServiceOperationResult{}, mapFormatError(err)
		}
		if err := validatePersistedServiceClient(pipeline, client); err != nil {
			return loadServiceOperationResult{}, err
		}
		index, err := restoreIndex(ctx, persisted, newSegmentStore(operation.layout, cfg))
		if err != nil {
			return loadServiceOperationResult{}, corruptMissingReference(err)
		}
		semanticService, err := service.BindRestored(index, client, *pipeline)
		if err != nil {
			return loadServiceOperationResult{}, err
		}
		return loadServiceOperationResult{service: semanticService, generation: generation}, nil
	})
	if err != nil {
		return nil, Generation{}, err
	}
	return loaded.service, loaded.generation, nil
}

type loadServiceOperationResult struct {
	service    *service.Service
	generation Generation
}

func validatePersistedServiceClient(config *service.PipelineConfig, client embedding.Client) error {
	if config == nil || client == nil {
		return service.ErrModelServiceUnavailable
	}
	model := client.Model()
	if err := model.Validate(); err != nil {
		return service.ErrModelServiceUnavailable
	}
	if model != config.Model {
		return service.ErrModelServiceUnavailable
	}
	return nil
}
