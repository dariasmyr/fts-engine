package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist/internal/storage"
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
	if ctx == nil {
		return nil, Generation{}, persistedGeneration{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, Generation{}, persistedGeneration{}, err
	}
	if root == "" {
		return nil, Generation{}, persistedGeneration{}, ErrCorrupt
	}
	cfg, err := newSettings(config.Config)
	if err != nil {
		return nil, Generation{}, persistedGeneration{}, err
	}

	l := newLayout(root)
	if err := l.validate(); err != nil {
		return nil, Generation{}, persistedGeneration{}, err
	}
	lock, err := storage.AcquireOperationLock(l.lock)
	if err != nil {
		return nil, Generation{}, persistedGeneration{}, err
	}
	defer lock.Close()

	head, err := newCurrentFile(l, cfg).read(ctx)
	if err != nil {
		return nil, Generation{}, persistedGeneration{}, err
	}
	persisted, err := newGenerationFiles(l, cfg).open(
		ctx,
		head.GenerationID,
		&head.ManifestHash,
	)
	if err != nil {
		return nil, Generation{}, persistedGeneration{}, corruptMissingReference(err)
	}
	index, err := restoreIndex(
		ctx,
		persisted,
		newSegmentStore(l, cfg),
	)
	if err != nil {
		return nil, Generation{}, persistedGeneration{}, corruptMissingReference(err)
	}
	return index, Generation{ID: head.GenerationID}, persisted, nil
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
	index, generation, persisted, err := load(ctx, root, loadConfig)
	if err != nil {
		return nil, Generation{}, err
	}
	if persisted.state.ServiceConfig == nil {
		return nil, Generation{}, semantic.ErrModelServiceUnavailable
	}
	service, err := semantic.NewFromIndex(client, index, *persisted.state.ServiceConfig)
	if err != nil {
		return nil, Generation{}, err
	}
	return service, generation, nil
}
