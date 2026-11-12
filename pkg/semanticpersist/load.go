package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Load restores the current generation fully into memory and returns its token
// for the next optimistic Save.
func Load(
	ctx context.Context,
	root string,
	config LoadConfig,
) (*semantic.Index, Generation, error) {
	index, generation, _, err := load(ctx, root, config)
	return index, generation, err
}

func load(
	ctx context.Context,
	root string,
	config LoadConfig,
) (*semantic.Index, Generation, persistedGeneration, error) {
	cfg, err := prepareLoad(ctx, root, config)
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

func prepareLoad(ctx context.Context, root string, config LoadConfig) (settings, error) {
	if ctx == nil {
		return settings{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return settings{}, err
	}
	if root == "" {
		return settings{}, ErrCorrupt
	}
	return newSettings(config.Config)
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
	loadConfig LoadConfig,
) (*semantic.Service, Generation, error) {
	cfg, err := prepareLoad(ctx, root, loadConfig)
	if err != nil {
		return nil, Generation{}, err
	}
	loaded, err := withStorageOperationResult(root, operationLayoutExisting, func(operation *storageOperation) (loadServiceOperationResult, error) {
		persisted, generation, err := openCurrentGeneration(ctx, operation.layout, cfg)
		if err != nil {
			return loadServiceOperationResult{}, err
		}
		if err := validatePersistedServiceClient(persisted.state.ServiceConfig, client); err != nil {
			return loadServiceOperationResult{}, err
		}
		index, err := restoreIndex(ctx, persisted, newSegmentStore(operation.layout, cfg))
		if err != nil {
			return loadServiceOperationResult{}, corruptMissingReference(err)
		}
		service, err := semantic.NewFromIndex(client, index, *persisted.state.ServiceConfig)
		if err != nil {
			return loadServiceOperationResult{}, err
		}
		return loadServiceOperationResult{service: service, generation: generation}, nil
	})
	if err != nil {
		return nil, Generation{}, err
	}
	return loaded.service, loaded.generation, nil
}

type loadServiceOperationResult struct {
	service    *semantic.Service
	generation Generation
}

func validatePersistedServiceClient(config *semantic.ServiceConfig, client embedding.Client) error {
	if config == nil || client == nil {
		return semantic.ErrModelServiceUnavailable
	}
	model := client.Model()
	if err := model.Validate(); err != nil {
		return semantic.ErrModelServiceUnavailable
	}
	clientConfig := semantic.ModelConfig{
		ID:             model.ID,
		Revision:       model.Revision,
		Dimensions:     model.Dimensions,
		ContextTokens:  model.ContextTokens,
		DocumentPrefix: model.DocumentPrefix,
		QueryPrefix:    model.QueryPrefix,
	}
	if clientConfig != config.Model {
		return semantic.ErrModelServiceUnavailable
	}
	return nil
}
