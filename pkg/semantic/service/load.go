package service

import (
	"context"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// Load restores a model-backed service through loader and verifies that the
// loaded state matches both the preflight snapshot and client's model.
func Load(
	ctx context.Context,
	loader Loader,
	client embedding.Client,
) (*Service, uint64, error) {
	if ctx == nil {
		return nil, 0, vector.ErrNilContext
	}
	if loader == nil {
		return nil, 0, ErrInvalidConfig
	}
	if client == nil {
		return nil, 0, ErrModelServiceUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	model := client.Model()
	if err := model.Validate(); err != nil {
		return nil, 0, ErrModelServiceUnavailable
	}

	var (
		preflightCalls    int
		preflightIndex    semantic.IndexConfig
		preflightPipeline PipelineConfig
		preflightErr      error
	)
	preflight := func(index semantic.IndexConfig, pipeline PipelineConfig) error {
		preflightCalls++
		if preflightCalls != 1 {
			preflightErr = ErrInvalidLoadedState
			return preflightErr
		}
		if index.ValidateCanonical() != nil || pipeline.ValidateCanonical(index) != nil {
			preflightErr = ErrInvalidLoadedState
			return preflightErr
		}
		if pipeline.Model != model {
			preflightErr = ErrModelServiceUnavailable
			return preflightErr
		}
		preflightIndex = index
		preflightPipeline = pipeline
		return nil
	}

	loaded, err := loader.LoadServiceState(ctx, preflight)
	if err != nil {
		return nil, 0, err
	}
	if preflightErr != nil {
		return nil, 0, preflightErr
	}
	if preflightCalls != 1 || loaded.Index == nil || loaded.GenerationID == 0 ||
		loaded.Index.Config() != preflightIndex || loaded.Pipeline != preflightPipeline ||
		loaded.Index.Config().ValidateCanonical() != nil ||
		loaded.Pipeline.ValidateCanonical(loaded.Index.Config()) != nil {
		return nil, 0, ErrInvalidLoadedState
	}

	service, err := bindRestored(loaded.Index, client, loaded.Pipeline)
	if err != nil {
		return nil, 0, err
	}
	return service, loaded.GenerationID, nil
}

func bindRestored(
	index *semantic.Index,
	client embedding.Client,
	saved PipelineConfig,
) (*Service, error) {
	if index == nil || client == nil || index.Config().ValidateCanonical() != nil ||
		saved.ValidateCanonical(index.Config()) != nil || client.Model() != saved.Model {
		return nil, ErrModelServiceUnavailable
	}

	config := Config{Index: index.Config(), Pipeline: saved}
	components, err := prepareRestoredModelService(client, config)
	if err != nil {
		return nil, err
	}
	if components.config != config {
		return nil, ErrModelServiceUnavailable
	}
	service, err := NewService(index, components.encoder, components.encoder)
	if err != nil {
		return nil, err
	}
	pipeline := saved
	service.pipeline = &pipeline
	return service, nil
}
