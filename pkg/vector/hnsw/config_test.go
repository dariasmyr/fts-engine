package hnsw_test

import (
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/vector/hnsw"
)

func TestBuildConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		config hnsw.BuildConfig
		valid  bool
	}{
		{name: "valid", config: hnsw.BuildConfig{MaxNeighbors: 16, EfConstruction: 64}, valid: true},
		{name: "maximum", config: hnsw.BuildConfig{MaxNeighbors: 1024, EfConstruction: 1_000_000}, valid: true},
		{name: "too few neighbors", config: hnsw.BuildConfig{MaxNeighbors: 1, EfConstruction: 64}},
		{name: "too many neighbors", config: hnsw.BuildConfig{MaxNeighbors: 1025, EfConstruction: 1025}},
		{name: "construction below neighbors", config: hnsw.BuildConfig{MaxNeighbors: 16, EfConstruction: 15}},
		{name: "construction too large", config: hnsw.BuildConfig{MaxNeighbors: 16, EfConstruction: 1_000_001}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.valid && !errors.Is(err, hnsw.ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestSearchConfigValidate(t *testing.T) {
	tests := []struct {
		name   string
		config hnsw.SearchConfig
		valid  bool
	}{
		{name: "valid", config: hnsw.SearchConfig{EfSearch: 1, VisitLimit: 1}, valid: true},
		{name: "missing ef search", config: hnsw.SearchConfig{VisitLimit: 1}},
		{name: "missing visit limit", config: hnsw.SearchConfig{EfSearch: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.valid && !errors.Is(err, hnsw.ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}
