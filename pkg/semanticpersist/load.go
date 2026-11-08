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
	options LoadOptions,
) (*semantic.Index, Generation, error) {
	if ctx == nil {
		return nil, Generation{}, vector.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, Generation{}, err
	}
	if root == "" {
		return nil, Generation{}, ErrCorrupt
	}
	cfg, err := newSettings(options.Limits)
	if err != nil {
		return nil, Generation{}, err
	}

	l := newLayout(root)
	if err := l.validate(); err != nil {
		return nil, Generation{}, err
	}
	lock, err := storage.AcquireOperationLock(l.lock)
	if err != nil {
		return nil, Generation{}, err
	}
	defer lock.Close()

	head, err := newCurrentFile(l, cfg).read(ctx)
	if err != nil {
		return nil, Generation{}, err
	}
	persisted, err := newGenerationFiles(l, cfg).open(
		ctx,
		head.GenerationID,
		&head.ManifestHash,
	)
	if err != nil {
		return nil, Generation{}, corruptMissingReference(err)
	}
	index, err := restoreIndex(
		ctx,
		persisted,
		newSegmentStore(l, cfg),
	)
	if err != nil {
		return nil, Generation{}, corruptMissingReference(err)
	}
	return index, Generation{ID: head.GenerationID}, nil
}

// LoadService loads the current generation and binds its index to a
// model-derived semantic service. It returns the generation token for the next
// optimistic Save.
func LoadService(
	ctx context.Context,
	root string,
	client embedding.Client,
	options LoadOptions,
	semanticOptions ...semantic.Option,
) (*semantic.Service, Generation, error) {
	index, generation, err := Load(ctx, root, options)
	if err != nil {
		return nil, Generation{}, err
	}

	service, err := semantic.NewFromIndex(client, index, semanticOptions...)
	if err != nil {
		return nil, Generation{}, err
	}
	return service, generation, nil
}
