package main

import (
	"fts-hw/config"
	"testing"
)

func TestResolveIndexerType(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.Config
		want string
	}{
		{
			name: "prefers indexer type",
			cfg: &config.Config{
				FTS: config.FTSConfig{
					Indexer: config.IndexerConfig{Type: "hamt"},
				},
			},
			want: "hamt",
		},
		{
			name: "uses default when nothing set",
			cfg: &config.Config{
				FTS: config.FTSConfig{},
			},
			want: "radix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveIndexerType(tt.cfg)
			if got != tt.want {
				t.Fatalf("resolveIndexerType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveFilterType(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.Config
		want string
	}{
		{
			name: "explicit filter type",
			cfg: &config.Config{
				FTS: config.FTSConfig{
					Filter: config.FilterConfig{Type: "cuckoo"},
				},
			},
			want: "cuckoo",
		},
		{
			name: "default filter type",
			cfg: &config.Config{
				FTS: config.FTSConfig{},
			},
			want: "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveFilterType(tt.cfg)
			if got != tt.want {
				t.Fatalf("resolveFilterType() = %q, want %q", got, tt.want)
			}
		})
	}
}
