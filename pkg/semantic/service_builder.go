package semantic

import (
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
)

const (
	defaultMaxFields      = 16
	defaultMaxSourceBytes = uint64(1 << 20)
)

// ChunkSplitter splits document fields into byte ranges.
type ChunkSplitter interface {
	Split(text string) ([]chunk.Range, error)
}

func newConfiguredChunkSplitter(config chunk.Config) (ChunkSplitter, error) {
	splitter, err := chunk.NewSplitter(config)
	if err != nil {
		return nil, fmt.Errorf("%w: chunking config: %w", ErrInvalidConfig, err)
	}
	return splitter, nil
}

func defaultInputLimits(dimensions, maxChunks int) (modelInputLimits, error) {
	if dimensions <= 0 || uint64(dimensions) > ^uint64(0)/4 ||
		maxChunks <= 0 || uint64(maxChunks) > ^uint64(0)/(uint64(dimensions)*4) {
		return modelInputLimits{}, ErrInvalidConfig
	}
	return modelInputLimits{
		maxFields:         defaultMaxFields,
		maxSourceBytes:    defaultMaxSourceBytes,
		maxChunks:         maxChunks,
		maxEmbeddingBytes: uint64(maxChunks) * uint64(dimensions) * 4,
	}, nil
}

type modelServiceComponents struct {
	config    SemanticConfig
	documents *modelEncoder
	queries   *modelEncoder
}

func prepareStandardModelService(
	client embedding.Client,
	config SemanticConfig,
) (modelServiceComponents, error) {
	if config.Chunking.Kind != ChunkingKindByteSplitter {
		return modelServiceComponents{}, ErrInvalidConfig
	}
	splitter, err := newConfiguredChunkSplitter(config.Chunking.Builtin)
	if err != nil {
		return modelServiceComponents{}, err
	}
	return prepareModelService(client, config, splitter)
}

func prepareCustomModelService(
	client embedding.Client,
	config SemanticConfig,
	splitter ChunkSplitter,
) (modelServiceComponents, error) {
	if config.Chunking.Kind != ChunkingKindCustom || splitter == nil {
		return modelServiceComponents{}, ErrInvalidConfig
	}
	return prepareModelService(client, config, splitter)
}

func prepareModelService(
	client embedding.Client,
	config SemanticConfig,
	splitter ChunkSplitter,
) (modelServiceComponents, error) {
	if client == nil || splitter == nil || newModelConfig(client.Model()) != config.Model {
		return modelServiceComponents{}, ErrInvalidConfig
	}
	config, err := config.normalized()
	if err != nil {
		return modelServiceComponents{}, err
	}
	return buildModelService(client, config, splitter)
}

func buildModelService(
	client embedding.Client,
	config SemanticConfig,
	splitter ChunkSplitter,
) (modelServiceComponents, error) {
	inputLimits, err := defaultInputLimits(
		config.Model.Dimensions,
		config.Index.Limits.MaxChunksPerDocument,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	documents, err := newModelEncoder(
		client,
		modelInputDocument,
		splitter,
		config.Model,
		inputLimits,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	queries, err := newModelEncoder(
		client,
		modelInputQuery,
		splitter,
		config.Model,
		inputLimits,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	return modelServiceComponents{config: config, documents: documents, queries: queries}, nil
}
