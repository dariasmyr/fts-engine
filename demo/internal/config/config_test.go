package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidateConfigAppliesFallbacksFromDefaultConfig(t *testing.T) {
	cfg := defaultConfig()
	cfg.Env = ""
	cfg.DumpPath = ""
	cfg.FTS.Index = ""
	cfg.FTS.KeyGen = ""
	cfg.FTS.Scorer = ""
	cfg.FTS.Filter = ""
	cfg.FTS.Persistence.Format = ""
	cfg.FTS.Persistence.Access = ""
	cfg.FTS.Persistence.Path = ""
	cfg.FTS.Persistence.BufferSize = 0
	cfg.FTS.Persistence.FlushThreshold = 0
	cfg.FTS.Pipeline.MinLength = 0
	cfg.Mode.Type = ""

	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}

	if !reflect.DeepEqual(cfg, defaultConfig()) {
		t.Fatalf("validateConfig() defaults mismatch\n got: %+v\nwant: %+v", cfg, defaultConfig())
	}
}

func TestValidateConfigRejectsSnapshotMmap(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Persistence.Format = "snapshot"
	cfg.FTS.Persistence.Access = "mmap"

	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("validateConfig() error = nil, want error for snapshot+mmap")
	}
	if !strings.Contains(err.Error(), "snapshot persistence supports only file access") {
		t.Fatalf("validateConfig() error = %q, want snapshot persistence error", err)
	}
}

func TestValidateConfigAllowsSegmentMmap(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Persistence.Format = "segment"
	cfg.FTS.Persistence.Access = "mmap"

	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}

	if got, want := cfg.FTS.Persistence.Format, "segment"; got != want {
		t.Fatalf("Persistence.Format = %q, want %q", got, want)
	}
	if got, want := cfg.FTS.Persistence.Access, "mmap"; got != want {
		t.Fatalf("Persistence.Access = %q, want %q", got, want)
	}
}

func TestConfigStructsDoNotUseEnvDefaults(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(Config{}),
		reflect.TypeOf(FTSConfig{}),
		reflect.TypeOf(RankProfileConfig{}),
		reflect.TypeOf(CompactionConfig{}),
		reflect.TypeOf(PersistenceConfig{}),
		reflect.TypeOf(BloomConfig{}),
		reflect.TypeOf(CuckooConfig{}),
		reflect.TypeOf(RibbonConfig{}),
		reflect.TypeOf(ModeConfig{}),
		reflect.TypeOf(PipelineConfig{}),
	} {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if tag := field.Tag.Get("env-default"); tag != "" {
				t.Fatalf("field %s.%s still uses env-default tag %q", typ.Name(), field.Name, tag)
			}
		}
	}
}

func TestValidateConfigAllowsRankProfileWithScorer(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Scorer = "bm25"
	cfg.FTS.RankProfile = RankProfileConfig{FieldWeights: map[string]float64{"title": 1.5}}

	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
	if cfg.FTS.RankProfile.Name != "demo" {
		t.Fatalf("RankProfile.Name = %q, want demo", cfg.FTS.RankProfile.Name)
	}
}

func TestValidateConfigRejectsRankProfileWithoutScorer(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Scorer = "none"
	cfg.FTS.RankProfile = RankProfileConfig{FieldWeights: map[string]float64{"title": 1.5}}

	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("validateConfig() error = nil, want rank_profile scorer error")
	}
	if !strings.Contains(err.Error(), "rank_profile requires") {
		t.Fatalf("validateConfig() error = %q, want rank_profile error", err)
	}
}

func TestValidateConfigRejectsInvalidRankProfileWeight(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Scorer = "bm25"
	cfg.FTS.RankProfile = RankProfileConfig{FieldWeights: map[string]float64{"title": -1}}

	err := validateConfig(&cfg)
	if err == nil {
		t.Fatal("validateConfig() error = nil, want invalid weight error")
	}
	if !strings.Contains(err.Error(), "invalid weight") {
		t.Fatalf("validateConfig() error = %q, want invalid weight error", err)
	}
}
