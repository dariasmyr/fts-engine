package semantic

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestConfigRejectsIncompatibleHNSWLimits(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"dimensions", func(config *Config) { config.HNSWBuild.Dimensions = config.Embedding.Dimensions + 1 }},
		{"metric", func(config *Config) { config.HNSWBuild.Metric = vector.MetricCosine }},
		{"vector capacity", func(config *Config) { config.HNSWBuild.MaxVectors = config.MaxVectors - 1 }},
		{"vector bytes", func(config *Config) { config.HNSWBuild.MaxVectorBytes = 1 }},
		{"search max k", func(config *Config) { config.HNSWSearch.MaxK = config.MaxChunkCandidates - 1 }},
		{"search max ef", func(config *Config) { config.HNSWSearch.MaxEfSearch = config.MaxChunkCandidates - 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testConfig(10, 100)
			test.mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate error = %v", err)
			}
			if _, err := New(config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New error = %v", err)
			}
		})
	}
}
