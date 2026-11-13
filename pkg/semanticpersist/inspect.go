package semanticpersist

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
)

// StoredInfo is a detached copy of the immutable contract stored in CURRENT.
// Inspect validates metadata but does not open vector or HNSW segment objects.
type StoredInfo struct {
	Generation  Generation
	IndexID     semantic.IndexID
	IndexConfig semantic.IndexConfig
	Pipeline    service.PipelineConfig
	HasPipeline bool
}

// Inspect reads and validates the current generation metadata without restoring
// the index or requiring an embedding client.
func Inspect(ctx context.Context, root string, options LoadOptions) (StoredInfo, error) {
	cfg, err := prepareLoad(ctx, root, options)
	if err != nil {
		return StoredInfo{}, err
	}
	return withStorageOperationResult(root, operationLayoutExisting, func(operation *storageOperation) (StoredInfo, error) {
		persisted, generation, err := openCurrentGeneration(ctx, operation.layout, cfg)
		if err != nil {
			return StoredInfo{}, err
		}
		info := StoredInfo{
			Generation:  generation,
			IndexID:     persisted.state.ID,
			IndexConfig: persisted.state.Config,
		}
		if persisted.state.Pipeline != nil {
			info.Pipeline = *persisted.state.Pipeline
			info.HasPipeline = true
		}
		return info, nil
	})
}
