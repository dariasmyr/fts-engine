package semantic

import (
	"errors"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

func TestConfigRejectsInvalidLimitsAndHNSWTuning(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"live vector capacity", func(config *Config) { config.Limits.MaxLiveVectors = 0 }},
		{"chunks per document", func(config *Config) { config.Limits.MaxChunksPerDocument = 0 }},
		{"documents per search", func(config *Config) { config.Limits.MaxDocumentsPerSearch = 0 }},
		{"candidate budget below documents", func(config *Config) { config.Limits.MaxChunkCandidates = 9 }},
		{"candidate budget above capacity", func(config *Config) { config.Limits.MaxChunkCandidates = 201 }},
		{"document chunks above capacity", func(config *Config) { config.Limits.MaxChunksPerDocument = 201 }},
		{"chunks per hit", func(config *Config) { config.Limits.MaxChunksPerDocumentHit = 0 }},
		{"chunks per hit above document limit", func(config *Config) { config.Limits.MaxChunksPerDocumentHit = 21 }},
		{"neighbors below minimum", func(config *Config) { config.HNSW.MaxNeighbors = 1 }},
		{"construction ef below neighbors", func(config *Config) {
			config.HNSW.MaxNeighbors = 32
			config.HNSW.EfConstruction = 16
		}},
		{"default search ef above maximum", func(config *Config) {
			config.HNSW.DefaultEfSearch = 101
			config.HNSW.MaxEfSearch = 100
		}},
		{"default visit limit above maximum", func(config *Config) {
			config.HNSW.DefaultVisitLimit = 201
			config.HNSW.MaxVisitLimit = 200
		}},
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

func TestConfigDerivesHNSWOptions(t *testing.T) {
	config, err := testConfig(10, 100).normalized()
	if err != nil {
		t.Fatal(err)
	}

	wantTuning := HNSWTuning{
		MaxNeighbors:      16,
		EfConstruction:    64,
		DefaultEfSearch:   64,
		MaxEfSearch:       100,
		DefaultVisitLimit: 200,
		MaxVisitLimit:     200,
	}
	if config.HNSW != wantTuning {
		t.Fatalf("normalized HNSW tuning = %+v, want %+v", config.HNSW, wantTuning)
	}

	wantOptions := hnsw.BuildOptions{
		Build: hnsw.BuildConfig{
			Dimensions:     2,
			Metric:         config.Embedding.Metric,
			MaxVectors:     200,
			MaxVectorBytes: 1600,
			MaxNeighbors:   16,
			EfConstruction: 64,
		},
		Search: hnsw.SearchConfig{
			DefaultEfSearch:   64,
			MaxEfSearch:       100,
			DefaultVisitLimit: 200,
			MaxVisitLimit:     200,
			MaxK:              100,
		},
	}
	if got, _ := config.hnswOptions(); !reflect.DeepEqual(got, wantOptions) {
		t.Fatalf("derived HNSW options = %+v, want %+v", got, wantOptions)
	}
}
