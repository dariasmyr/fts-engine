package service_test

import (
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
)

func TestDefaultConfigReturnsCanonicalConfig(t *testing.T) {
	t.Parallel()

	model := embedding.Model{ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 32}
	config, err := service.DefaultConfig(model)
	if err != nil {
		t.Fatal(err)
	}
	if config.Pipeline.Model != model {
		t.Fatalf("pipeline model = %+v, want %+v", config.Pipeline.Model, model)
	}
	if err := config.Index.ValidateCanonical(); err != nil {
		t.Fatalf("index config is not canonical: %v", err)
	}
	if err := config.Pipeline.ValidateCanonical(config.Index); err != nil {
		t.Fatalf("pipeline config is not canonical: %v", err)
	}
}
