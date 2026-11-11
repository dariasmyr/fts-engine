package semantic

import (
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
)

func newConfiguredChunkSplitter(config chunk.Config) (*chunk.Splitter, error) {
	splitter, err := chunk.NewSplitter(config)
	if err != nil {
		return nil, fmt.Errorf("%w: chunking config: %w", ErrInvalidConfig, err)
	}
	return splitter, nil
}

func modelLimits(
	config EncodingLimits,
	dimensions int,
	maxChunks int,
) (modelInputLimits, error) {
	if dimensions <= 0 || uint64(dimensions) > ^uint64(0)/4 ||
		maxChunks <= 0 || config.MaxFields <= 0 || config.MaxSourceBytes == 0 ||
		config.MaxEmbeddingBytes == 0 || uint64(maxChunks) > ^uint64(0)/(uint64(dimensions)*4) {
		return modelInputLimits{}, ErrInvalidConfig
	}
	return modelInputLimits{
		maxFields:         config.MaxFields,
		maxSourceBytes:    config.MaxSourceBytes,
		maxChunks:         maxChunks,
		maxEmbeddingBytes: config.MaxEmbeddingBytes,
	}, nil
}

type modelServiceComponents struct {
	config    ServiceConfig
	documents *modelEncoder
	queries   *modelEncoder
}

func prepareStandardModelService(
	client embedding.Client,
	config ServiceConfig,
) (modelServiceComponents, error) {
	splitter, err := newConfiguredChunkSplitter(config.Chunking)
	if err != nil {
		return modelServiceComponents{}, err
	}
	return prepareModelService(client, config, splitter)
}

func prepareModelService(
	client embedding.Client,
	config ServiceConfig,
	splitter *chunk.Splitter,
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
	config ServiceConfig,
	splitter *chunk.Splitter,
) (modelServiceComponents, error) {
	documentLimits, err := modelLimits(
		config.Encoding,
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
		documentLimits,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	queryLimits, err := modelLimits(
		config.Encoding,
		config.Model.Dimensions,
		config.Index.Search.MaxQueryChunks,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	queries, err := newModelEncoder(
		client,
		modelInputQuery,
		splitter,
		config.Model,
		queryLimits,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	return modelServiceComponents{config: config, documents: documents, queries: queries}, nil
}
