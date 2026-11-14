package service

import (
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/encoding"
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
) (encoding.InputLimits, error) {
	if dimensions <= 0 || uint64(dimensions) > ^uint64(0)/4 ||
		maxChunks <= 0 || config.MaxFields <= 0 || config.MaxSourceBytes == 0 ||
		config.MaxEmbeddingBytes == 0 || uint64(maxChunks) > ^uint64(0)/(uint64(dimensions)*4) {
		return encoding.InputLimits{}, ErrInvalidConfig
	}
	return encoding.InputLimits{
		MaxFields:         config.MaxFields,
		MaxSourceBytes:    config.MaxSourceBytes,
		MaxChunks:         maxChunks,
		MaxEmbeddingBytes: config.MaxEmbeddingBytes,
	}, nil
}

type modelServiceComponents struct {
	config  Config
	encoder *encoding.Encoder
}

func prepareStandardModelService(
	client embedding.Client,
	config Config,
) (modelServiceComponents, error) {
	splitter, err := newConfiguredChunkSplitter(config.Pipeline.Chunking)
	if err != nil {
		return modelServiceComponents{}, err
	}
	return prepareModelService(client, config, splitter)
}

func prepareRestoredModelService(
	client embedding.Client,
	config Config,
) (modelServiceComponents, error) {
	if client == nil || config.validateCanonical() != nil || client.Model() != config.Pipeline.Model {
		return modelServiceComponents{}, ErrModelServiceUnavailable
	}
	splitter, err := newConfiguredChunkSplitter(config.Pipeline.Chunking)
	if err != nil {
		return modelServiceComponents{}, err
	}
	return buildModelService(client, config, splitter)
}

func prepareModelService(
	client embedding.Client,
	config Config,
	splitter *chunk.Splitter,
) (modelServiceComponents, error) {
	if client == nil || splitter == nil || client.Model() != config.Pipeline.Model {
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
	config Config,
	splitter *chunk.Splitter,
) (modelServiceComponents, error) {
	documentLimits, err := modelLimits(
		config.Pipeline.Operational.Encoding,
		config.Pipeline.Model.Dimensions,
		config.Index.Operational.Ingestion.MaxChunksPerDocument,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	queryLimits, err := modelLimits(
		config.Pipeline.Operational.Encoding,
		config.Pipeline.Model.Dimensions,
		config.Index.Operational.Search.MaxQueryChunks,
	)
	if err != nil {
		return modelServiceComponents{}, err
	}
	encoder, err := encoding.NewEncoder(encoding.EncoderConfig{
		Client:         client,
		Splitter:       splitter,
		DocumentLimits: documentLimits,
		QueryLimits:    queryLimits,
	})
	if err != nil {
		return modelServiceComponents{}, err
	}
	return modelServiceComponents{config: config, encoder: encoder}, nil
}
