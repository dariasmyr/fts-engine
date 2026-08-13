package config

import (
	"fmt"
	"math"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env      string     `yaml:"env"`
	DumpPath string     `yaml:"dump_path"`
	FTS      FTSConfig  `yaml:"fts"`
	Mode     ModeConfig `yaml:"mode"`
}

type FTSConfig struct {
	Index       string            `yaml:"index"`
	KeyGen      string            `yaml:"keygen"`
	Scorer      string            `yaml:"scorer"`
	RankProfile RankProfileConfig `yaml:"rank_profile"`
	Filter      string            `yaml:"filter"`
	Persistence PersistenceConfig `yaml:"persistence"`
	Compaction  CompactionConfig  `yaml:"compaction"`
	Bloom       BloomConfig       `yaml:"bloom"`
	Cuckoo      CuckooConfig      `yaml:"cuckoo"`
	Ribbon      RibbonConfig      `yaml:"ribbon"`
	Pipeline    PipelineConfig    `yaml:"pipeline"`
}

type RankProfileConfig struct {
	Name             string                 `yaml:"name"`
	FieldWeights     map[string]float64     `yaml:"field_weights"`
	QueryTypeWeights QueryTypeWeightsConfig `yaml:"query_type_weights"`
}

type QueryTypeWeightsConfig struct {
	Term       float64 `yaml:"term"`
	Prefix     float64 `yaml:"prefix"`
	Phrase     float64 `yaml:"phrase"`
	NearPhrase float64 `yaml:"near_phrase"`
}

type CompactionConfig struct {
	LoadFactor float64 `yaml:"load_factor"`
	AutoCheck  bool    `yaml:"auto_check"`
}

type PersistenceConfig struct {
	Enabled        bool   `yaml:"enabled"`
	Format         string `yaml:"format"`
	Access         string `yaml:"access"`
	Path           string `yaml:"path"`
	LoadOnStart    bool   `yaml:"load_on_start"`
	SaveOnBuild    bool   `yaml:"save_on_build"`
	BufferSize     int    `yaml:"buffer_size"`
	FlushThreshold int    `yaml:"flush_threshold"`
	SyncFile       bool   `yaml:"sync_file"`
}

type BloomConfig struct {
	ExpectedItems uint64 `yaml:"expected_items"`
	BitsPerItem   uint64 `yaml:"bits_per_item"`
	K             uint64 `yaml:"k"`
}

type CuckooConfig struct {
	BucketCount int `yaml:"bucket_count"`
	BucketSize  int `yaml:"bucket_size"`
	MaxKicks    int `yaml:"max_kicks"`
}

type RibbonConfig struct {
	ExpectedItems uint32 `yaml:"expected_items"`
	ExtraCells    uint32 `yaml:"extra_cells"`
	WindowSize    uint32 `yaml:"window_size"`
	Seed          uint64 `yaml:"seed"`
	MaxAttempts   uint32 `yaml:"max_attempts"`
}

type ModeConfig struct {
	Type string `yaml:"type"`
}

type PipelineConfig struct {
	Preset      string `yaml:"preset"`
	Lowercase   bool   `yaml:"lowercase"`
	StopwordsEN bool   `yaml:"stopwords_en"`
	StopwordsRU bool   `yaml:"stopwords_ru"`
	StemEN      bool   `yaml:"stem_en"`
	StemRU      bool   `yaml:"stem_ru"`
	MinLength   int    `yaml:"min_length"`
}

func Load(configPath string) (*Config, string, error) {
	cfg := defaultConfig()
	if configPath == "" {
		configPath = fetchConfigPath()
	}

	if configPath != "" {
		if _, err := os.Stat(configPath); err == nil {
			if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
				return nil, "", fmt.Errorf("read config %q: %w", configPath, err)
			}
			if err := validateConfig(&cfg); err != nil {
				return nil, "", err
			}
			return &cfg, configPath, nil
		}
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, "", err
	}
	return &cfg, "defaults", nil
}

// fetchConfigPath fetches domain path from environment variable or default if it was not set in command line flag.
// Priority: flag > env > default.
// Default value is empty string.
func fetchConfigPath() string {
	if res := os.Getenv("CONFIG_PATH"); res != "" {
		return res
	}

	return "./config/config_local.yaml"
}

func defaultConfig() Config {
	return Config{
		Env:      "local",
		DumpPath: "./data/enwiki-latest-abstract1.xml.gz",
		FTS: FTSConfig{
			Index:  "slicedradix",
			KeyGen: "word",
			Scorer: "none",
			Filter: "ribbon",
			Persistence: PersistenceConfig{
				Enabled:        true,
				Format:         "snapshot",
				Access:         "file",
				Path:           "./data/fts/local",
				LoadOnStart:    false,
				SaveOnBuild:    true,
				BufferSize:     1048576,
				FlushThreshold: 262144,
				SyncFile:       true,
			},
			Compaction: CompactionConfig{
				LoadFactor: 0,
				AutoCheck:  true,
			},
			Bloom: BloomConfig{
				ExpectedItems: 1000000,
				BitsPerItem:   20,
				K:             14,
			},
			Cuckoo: CuckooConfig{
				BucketCount: 327680,
				BucketSize:  4,
				MaxKicks:    500,
			},
			Ribbon: RibbonConfig{
				ExpectedItems: 1000000,
				ExtraCells:    250000,
				WindowSize:    16,
				Seed:          0,
				MaxAttempts:   50,
			},
			Pipeline: PipelineConfig{
				Preset:      "custom",
				Lowercase:   true,
				StopwordsEN: true,
				StopwordsRU: false,
				StemEN:      true,
				StemRU:      false,
				MinLength:   3,
			},
		},
		Mode: ModeConfig{Type: "prod"},
	}
}

func validateConfig(cfg *Config) error {
	defaults := defaultConfig()

	if cfg.Env == "" {
		cfg.Env = defaults.Env
	}

	if cfg.DumpPath == "" {
		cfg.DumpPath = defaults.DumpPath
	}

	if cfg.FTS.Index == "" {
		cfg.FTS.Index = defaults.FTS.Index
	}

	if cfg.FTS.KeyGen == "" {
		cfg.FTS.KeyGen = defaults.FTS.KeyGen
	}

	if cfg.FTS.Scorer == "" {
		cfg.FTS.Scorer = defaults.FTS.Scorer
	}
	if cfg.FTS.RankProfile.configured() && cfg.FTS.RankProfile.Name == "" {
		cfg.FTS.RankProfile.Name = "demo"
	}

	if cfg.FTS.Filter == "" {
		cfg.FTS.Filter = defaults.FTS.Filter
	}

	if cfg.FTS.Persistence.Format == "" {
		cfg.FTS.Persistence.Format = defaults.FTS.Persistence.Format
	}

	if cfg.FTS.Persistence.Access == "" {
		cfg.FTS.Persistence.Access = defaults.FTS.Persistence.Access
	}

	if cfg.FTS.Persistence.Path == "" {
		cfg.FTS.Persistence.Path = defaults.FTS.Persistence.Path
	}

	if cfg.FTS.Persistence.BufferSize <= 0 {
		cfg.FTS.Persistence.BufferSize = defaults.FTS.Persistence.BufferSize
	}

	if cfg.FTS.Persistence.FlushThreshold <= 0 {
		cfg.FTS.Persistence.FlushThreshold = defaults.FTS.Persistence.FlushThreshold
	}

	if cfg.FTS.Pipeline.MinLength <= 0 {
		cfg.FTS.Pipeline.MinLength = defaults.FTS.Pipeline.MinLength
	}
	if cfg.FTS.Pipeline.Preset == "" {
		cfg.FTS.Pipeline.Preset = defaults.FTS.Pipeline.Preset
	}

	if cfg.Mode.Type == "" {
		cfg.Mode.Type = defaults.Mode.Type
	}

	if cfg.FTS.Compaction.LoadFactor < 0 || cfg.FTS.Compaction.LoadFactor > 1 {
		return fmt.Errorf("compaction load_factor must be in range [0..1]")
	}

	switch cfg.FTS.Index {
	case "slicedradix", "hamt", "flat":
	default:
		return fmt.Errorf("unknown index type: %s", cfg.FTS.Index)
	}

	switch cfg.FTS.KeyGen {
	case "word":
	default:
		return fmt.Errorf("unknown keygen type: %s", cfg.FTS.KeyGen)
	}

	switch cfg.FTS.Scorer {
	case "none", "bm25", "tfidf":
	default:
		return fmt.Errorf("unknown scorer type: %s", cfg.FTS.Scorer)
	}

	if err := validateRankProfile(cfg.FTS.RankProfile, cfg.FTS.Scorer); err != nil {
		return err
	}

	switch cfg.FTS.Pipeline.Preset {
	case "custom", "observability":
	default:
		return fmt.Errorf("unknown pipeline preset: %s", cfg.FTS.Pipeline.Preset)
	}

	switch cfg.FTS.Filter {
	case "none", "bloom", "cuckoo", "ribbon":
	default:
		return fmt.Errorf("unknown filter type: %s", cfg.FTS.Filter)
	}

	switch cfg.FTS.Persistence.Format {
	case "snapshot", "segment":
	default:
		return fmt.Errorf("unknown persistence format: %s", cfg.FTS.Persistence.Format)
	}

	switch cfg.FTS.Persistence.Access {
	case "file", "mmap":
	default:
		return fmt.Errorf("unknown persistence access: %s", cfg.FTS.Persistence.Access)
	}

	if cfg.FTS.Persistence.Format == "snapshot" && cfg.FTS.Persistence.Access != "file" {
		return fmt.Errorf("snapshot persistence supports only file access")
	}

	if cfg.FTS.Cuckoo.BucketCount <= 0 {
		return fmt.Errorf("cuckoo bucket_count must be > 0")
	}

	if cfg.FTS.Cuckoo.BucketSize <= 0 {
		return fmt.Errorf("cuckoo bucket_size must be > 0")
	}

	if cfg.FTS.Cuckoo.MaxKicks < 0 {
		return fmt.Errorf("cuckoo max_kicks must be >= 0")
	}

	if cfg.FTS.Filter == "ribbon" {
		if cfg.FTS.Ribbon.ExpectedItems == 0 {
			return fmt.Errorf("ribbon expected_items must be > 0")
		}

		if cfg.FTS.Ribbon.WindowSize == 0 || cfg.FTS.Ribbon.WindowSize > 32 {
			return fmt.Errorf("ribbon window_size must be in range [1..32]")
		}

		if cfg.FTS.Ribbon.MaxAttempts == 0 {
			return fmt.Errorf("ribbon max_attempts must be > 0")
		}
	}

	switch cfg.Mode.Type {
	case "prod", "experiment":
	default:
		return fmt.Errorf("unknown mode type: %s", cfg.Mode.Type)
	}

	return nil
}

func validateRankProfile(profile RankProfileConfig, scorer string) error {
	if !profile.configured() {
		return nil
	}
	if scorer == "none" {
		return fmt.Errorf("rank_profile requires fts.scorer to be bm25 or tfidf")
	}
	if err := validateFieldWeights(profile.FieldWeights); err != nil {
		return err
	}
	return validateQueryTypeWeights(profile.QueryTypeWeights)
}

func validateFieldWeights(weights map[string]float64) error {
	for field, weight := range weights {
		if field == "" {
			return fmt.Errorf("rank_profile field_weights contains an empty field name")
		}
		switch field {
		case "title", "abstract":
		default:
			return fmt.Errorf("rank_profile field_weights contains unsupported field %q", field)
		}
		if weight < 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			return fmt.Errorf("rank_profile field %q has invalid weight %v", field, weight)
		}
	}
	return nil
}

func validateQueryTypeWeights(weights QueryTypeWeightsConfig) error {
	queryTypeWeights := []struct {
		name   string
		weight float64
	}{
		{name: "term", weight: weights.Term},
		{name: "prefix", weight: weights.Prefix},
		{name: "phrase", weight: weights.Phrase},
		{name: "near_phrase", weight: weights.NearPhrase},
	}
	for _, queryType := range queryTypeWeights {
		if queryType.weight < 0 || math.IsNaN(queryType.weight) || math.IsInf(queryType.weight, 0) {
			return fmt.Errorf("rank_profile query type %q has invalid weight %v", queryType.name, queryType.weight)
		}
	}
	return nil
}

func (p RankProfileConfig) configured() bool {
	return len(p.FieldWeights) > 0 || p.QueryTypeWeights != (QueryTypeWeightsConfig{})
}
