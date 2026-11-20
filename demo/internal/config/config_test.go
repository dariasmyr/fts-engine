package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadReadsQueryTypeWeights(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`fts:
  scorer: bm25
  rank_profile:
    name: query-types
    query_type_weights:
      term: 1.5
      prefix: 0.5
      phrase: 1.7
      near_phrase: 2.0
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := QueryTypeWeightsConfig{Term: 1.5, Prefix: 0.5, Phrase: 1.7, NearPhrase: 2}
	if got := cfg.FTS.RankProfile.QueryTypeWeights; got != want {
		t.Fatalf("QueryTypeWeights = %+v, want %+v", got, want)
	}
}

func TestLoadReadsSemanticSearchConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`semantic:
  search:
    ef_search: 12
    candidate_chunks: 18
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := SemanticSearchConfig{EfSearch: 12, CandidateChunks: 18}
	if cfg.Semantic.Search != want {
		t.Fatalf("Semantic.Search = %+v, want %+v", cfg.Semantic.Search, want)
	}
}

func TestLoadReadsMaxDocuments(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("max_documents: 25000\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.MaxDocuments != 25_000 {
		t.Fatalf("MaxDocuments = %d, want 25000", cfg.MaxDocuments)
	}
}

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
	cfg.FTS.Pipeline.Preset = ""
	cfg.FTS.Pipeline.MinLength = 0
	cfg.Semantic.Workers = 0
	cfg.Semantic.CheckpointDocuments = 0
	cfg.Semantic.MaxLiveVectors = 0
	cfg.Semantic.Ollama = SemanticOllamaConfig{}
	cfg.Semantic.Persistence.Path = ""
	cfg.Semantic.Persistence.MaxFileBytes = 0
	cfg.Semantic.Persistence.MaxOpenBytes = 0
	cfg.Mode.Type = ""

	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}

	if !reflect.DeepEqual(cfg, defaultConfig()) {
		t.Fatalf("validateConfig() defaults mismatch\n got: %+v\nwant: %+v", cfg, defaultConfig())
	}
}

func TestValidateConfigAllowsFlatObservability(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Index = "flat"
	cfg.FTS.Pipeline.Preset = "observability"

	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
}

func TestValidateConfigRejectsNegativeMaxDocuments(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	cfg.MaxDocuments = -1

	err := validateConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), "max_documents must be >= 0") {
		t.Fatalf("validateConfig() error = %v, want max_documents error", err)
	}
}

func TestValidateConfigRejectsUnknownPipelinePreset(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Pipeline.Preset = "unknown"

	err := validateConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), "unknown pipeline preset") {
		t.Fatalf("validateConfig() error = %v, want pipeline preset error", err)
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
		reflect.TypeOf(QueryTypeWeightsConfig{}),
		reflect.TypeOf(CompactionConfig{}),
		reflect.TypeOf(PersistenceConfig{}),
		reflect.TypeOf(BloomConfig{}),
		reflect.TypeOf(CuckooConfig{}),
		reflect.TypeOf(RibbonConfig{}),
		reflect.TypeOf(ModeConfig{}),
		reflect.TypeOf(PipelineConfig{}),
		reflect.TypeOf(SemanticConfig{}),
		reflect.TypeOf(SemanticSearchConfig{}),
		reflect.TypeOf(SemanticOllamaConfig{}),
		reflect.TypeOf(SemanticPersistenceConfig{}),
		reflect.TypeOf(SemanticCompactionConfig{}),
	} {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if tag := field.Tag.Get("env-default"); tag != "" {
				t.Fatalf("field %s.%s still uses env-default tag %q", typ.Name(), field.Name, tag)
			}
		}
	}
}

func TestValidateConfigAllowsSemanticCompaction(t *testing.T) {
	cfg := defaultConfig()
	cfg.Semantic.Enabled = true
	cfg.Semantic.Compaction = SemanticCompactionConfig{
		Enabled: true,
		Prune:   true,
	}

	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}
}

func TestValidateConfigRejectsSemanticPersistenceOpenLimitBelowFileLimit(t *testing.T) {
	cfg := defaultConfig()
	cfg.Semantic.Persistence.MaxFileBytes = 2 << 30
	cfg.Semantic.Persistence.MaxOpenBytes = 1 << 30

	err := validateConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), "max_open_bytes must be at least max_file_bytes") {
		t.Fatalf("validateConfig() error = %v, want semantic persistence limit error", err)
	}
}

func TestValidateConfigRejectsUnsafeSemanticCompaction(t *testing.T) {
	tests := []struct {
		name        string
		configure   func(*Config)
		wantErrPart string
	}{
		{
			name: "without persistence",
			configure: func(cfg *Config) {
				cfg.Semantic.Persistence.Enabled = false
			},
			wantErrPart: "requires persistence",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			cfg.Semantic.Enabled = true
			cfg.Semantic.Compaction = SemanticCompactionConfig{
				Enabled: true,
			}
			tt.configure(&cfg)

			err := validateConfig(&cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrPart) {
				t.Fatalf("validateConfig() error = %v, want %q", err, tt.wantErrPart)
			}
		})
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

func TestValidateConfigAllowsQueryTypeWeightsWithoutFieldWeights(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Scorer = "bm25"
	cfg.FTS.RankProfile = RankProfileConfig{
		QueryTypeWeights: QueryTypeWeightsConfig{Phrase: 1.7},
	}

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

func TestValidateConfigRejectsQueryTypeWeightsWithoutScorer(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Scorer = "none"
	cfg.FTS.RankProfile = RankProfileConfig{
		QueryTypeWeights: QueryTypeWeightsConfig{Term: 1.5},
	}

	err := validateConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), "rank_profile requires") {
		t.Fatalf("validateConfig() error = %v, want rank_profile scorer error", err)
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

func TestValidateConfigRejectsUnsupportedRankProfileField(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Scorer = "bm25"
	cfg.FTS.RankProfile = RankProfileConfig{FieldWeights: map[string]float64{"body": 1}}

	err := validateConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), `unsupported field "body"`) {
		t.Fatalf("validateConfig() error = %v, want unsupported field error", err)
	}
}

func TestValidateConfigRejectsInvalidQueryTypeWeight(t *testing.T) {
	cfg := defaultConfig()
	cfg.FTS.Scorer = "bm25"
	cfg.FTS.RankProfile = RankProfileConfig{
		QueryTypeWeights: QueryTypeWeightsConfig{Prefix: -1},
	}

	err := validateConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), `query type "prefix" has invalid weight`) {
		t.Fatalf("validateConfig() error = %v, want invalid query type weight error", err)
	}
}
