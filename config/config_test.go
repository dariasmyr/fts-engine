package config

import (
	"reflect"
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

	validateConfig(&cfg)

	if !reflect.DeepEqual(cfg, defaultConfig()) {
		t.Fatalf("validateConfig() defaults mismatch\n got: %+v\nwant: %+v", cfg, defaultConfig())
	}
}

func TestValidateConfigRejectsSnapshotMmap(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Persistence.Format = "snapshot"
	cfg.FTS.Persistence.Access = "mmap"

	defer func() {
		if recover() == nil {
			t.Fatal("validateConfig() panic = nil, want panic for snapshot+mmap")
		}
	}()

	validateConfig(&cfg)
}

func TestValidateConfigAllowsSegmentMmap(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Persistence.Format = "segment"
	cfg.FTS.Persistence.Access = "mmap"

	validateConfig(&cfg)

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
