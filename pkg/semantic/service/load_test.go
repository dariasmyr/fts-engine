package service

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type loaderFunc func(context.Context, RestorePreflight) (LoadedState, error)

func (f loaderFunc) LoadServiceState(
	ctx context.Context,
	preflight RestorePreflight,
) (LoadedState, error) {
	return f(ctx, preflight)
}

type loadTestClient struct{ model embedding.Model }

func (c loadTestClient) Model() embedding.Model { return c.model }

func (loadTestClient) Embed(context.Context, []string) ([][]float32, error) {
	return nil, nil
}

func TestLoadRestoresExactState(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelConfig(t, client)
	config.Index.Metric = vector.MetricL2Squared
	config.Pipeline.Chunking.TargetBytes--
	original, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	preflightCalls := 0
	loader := loaderFunc(func(_ context.Context, preflight RestorePreflight) (LoadedState, error) {
		preflightCalls++
		if err := preflight(config.Index, config.Pipeline); err != nil {
			return LoadedState{}, err
		}
		return LoadedState{Index: original.index, Pipeline: config.Pipeline, GenerationID: 42}, nil
	})

	loaded, generationID, err := Load(t.Context(), loader, client)
	if err != nil {
		t.Fatal(err)
	}
	if preflightCalls != 1 {
		t.Fatalf("preflight calls = %d, want 1", preflightCalls)
	}
	if generationID != 42 {
		t.Fatalf("generation ID = %d, want 42", generationID)
	}
	if loaded.index != original.index {
		t.Fatal("Load() replaced the restored index")
	}
	loadedConfig, ok := loaded.Config()
	if !ok || loadedConfig != config {
		t.Fatalf("Config() = %+v, %v; want %+v, true", loadedConfig, ok, config)
	}
}

func TestLoadRejectsInvalidArgumentsBeforeLoader(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	called := false
	loader := loaderFunc(func(context.Context, RestorePreflight) (LoadedState, error) {
		called = true
		return LoadedState{}, nil
	})

	tests := []struct {
		name   string
		ctx    context.Context
		loader Loader
		client embedding.Client
		want   error
	}{
		{name: "nil context", loader: loader, client: client, want: vector.ErrNilContext},
		{name: "nil loader", ctx: t.Context(), client: client, want: ErrInvalidConfig},
		{name: "nil client", ctx: t.Context(), loader: loader, want: ErrModelServiceUnavailable},
		{
			name:   "invalid client model",
			ctx:    t.Context(),
			loader: loader,
			client: loadTestClient{},
			want:   ErrModelServiceUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			_, _, err := Load(tt.ctx, tt.loader, tt.client)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Load() error = %v, want %v", err, tt.want)
			}
			if called {
				t.Fatal("Load() called loader for invalid arguments")
			}
		})
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	called = false
	if _, _, err := Load(ctx, loader, client); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load() canceled error = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("Load() called loader with canceled context")
	}
}

func TestLoadPropagatesLoaderFailureBeforePreflight(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	want := errors.New("load failed")
	loader := loaderFunc(func(context.Context, RestorePreflight) (LoadedState, error) {
		return LoadedState{}, want
	})

	_, _, err := Load(t.Context(), loader, client)
	if !errors.Is(err, want) {
		t.Fatalf("Load() error = %v, want %v", err, want)
	}
}

func TestLoadRejectsInvalidLoaderResults(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelConfig(t, client)
	original, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	otherIndexConfig := config.Index
	otherIndexConfig.Metric = vector.MetricL2Squared
	otherIndex, err := semantic.NewIndex(otherIndexConfig)
	if err != nil {
		t.Fatal(err)
	}
	otherPipeline := config.Pipeline
	otherPipeline.Model.DocumentPrefix = "other: "

	tests := []struct {
		name   string
		loader Loader
	}{
		{
			name: "missing preflight",
			loader: loaderFunc(func(context.Context, RestorePreflight) (LoadedState, error) {
				return LoadedState{Index: original.index, Pipeline: config.Pipeline, GenerationID: 1}, nil
			}),
		},
		{
			name: "preflight called twice",
			loader: loaderFunc(func(_ context.Context, preflight RestorePreflight) (LoadedState, error) {
				_ = preflight(config.Index, config.Pipeline)
				_ = preflight(config.Index, config.Pipeline)
				return LoadedState{Index: original.index, Pipeline: config.Pipeline, GenerationID: 1}, nil
			}),
		},
		{
			name: "returned pipeline differs",
			loader: loaderFunc(func(_ context.Context, preflight RestorePreflight) (LoadedState, error) {
				if err := preflight(config.Index, config.Pipeline); err != nil {
					return LoadedState{}, err
				}
				return LoadedState{Index: original.index, Pipeline: otherPipeline, GenerationID: 1}, nil
			}),
		},
		{
			name: "returned index config differs",
			loader: loaderFunc(func(_ context.Context, preflight RestorePreflight) (LoadedState, error) {
				if err := preflight(config.Index, config.Pipeline); err != nil {
					return LoadedState{}, err
				}
				return LoadedState{Index: otherIndex, Pipeline: config.Pipeline, GenerationID: 1}, nil
			}),
		},
		{
			name: "nil index",
			loader: loaderFunc(func(_ context.Context, preflight RestorePreflight) (LoadedState, error) {
				if err := preflight(config.Index, config.Pipeline); err != nil {
					return LoadedState{}, err
				}
				return LoadedState{Pipeline: config.Pipeline, GenerationID: 1}, nil
			}),
		},
		{
			name: "zero generation ID",
			loader: loaderFunc(func(_ context.Context, preflight RestorePreflight) (LoadedState, error) {
				if err := preflight(config.Index, config.Pipeline); err != nil {
					return LoadedState{}, err
				}
				return LoadedState{Index: original.index, Pipeline: config.Pipeline}, nil
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Load(t.Context(), tt.loader, client)
			if !errors.Is(err, ErrInvalidLoadedState) {
				t.Fatalf("Load() error = %v, want ErrInvalidLoadedState", err)
			}
		})
	}
}

func TestLoadRejectsNonCanonicalPreflightWithoutDefaults(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelConfig(t, client)
	config.Pipeline.Operational.Encoding = EncodingLimits{}
	loader := loaderFunc(func(_ context.Context, preflight RestorePreflight) (LoadedState, error) {
		return LoadedState{}, preflight(config.Index, config.Pipeline)
	})

	_, _, err := Load(t.Context(), loader, client)
	if !errors.Is(err, ErrInvalidLoadedState) {
		t.Fatalf("Load() error = %v, want ErrInvalidLoadedState", err)
	}
}

func TestLoadRejectsModelMismatchBeforeReturningState(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelConfig(t, client)
	config.Pipeline.Model.DocumentPrefix = "other: "
	loader := loaderFunc(func(_ context.Context, preflight RestorePreflight) (LoadedState, error) {
		return LoadedState{}, preflight(config.Index, config.Pipeline)
	})

	_, _, err := Load(t.Context(), loader, client)
	if !errors.Is(err, ErrModelServiceUnavailable) {
		t.Fatalf("Load() error = %v, want ErrModelServiceUnavailable", err)
	}
}
