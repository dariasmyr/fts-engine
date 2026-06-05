package config

import (
	"flag"
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
	Engine      string            `yaml:"engine"`
	Index       string            `yaml:"index"`
	KeyGen      string            `yaml:"keygen"`
	Scorer      string            `yaml:"scorer"`
	Filter      string            `yaml:"filter"`
	Persistence PersistenceConfig `yaml:"persistence"`
	Compaction  CompactionConfig  `yaml:"compaction"`
	Bloom       BloomConfig       `yaml:"bloom"`
	Cuckoo      CuckooConfig      `yaml:"cuckoo"`
	Ribbon      RibbonConfig      `yaml:"ribbon"`
	Pipeline    PipelineConfig    `yaml:"pipeline"`
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
	Lowercase   bool `yaml:"lowercase"`
	StopwordsEN bool `yaml:"stopwords_en"`
	StopwordsRU bool `yaml:"stopwords_ru"`
	StemEN      bool `yaml:"stem_en"`
	StemRU      bool `yaml:"stem_ru"`
	MinLength   int  `yaml:"min_length"`
}

func MustLoad() (*Config, string) {
	return mustLoad()
}

func mustLoad() (*Config, string) {
	configPathFlag := flag.String("config", "", "Path to the config file")
	flag.Parse()

	cfg := defaultConfig()
	configPath := *configPathFlag
	if configPath == "" {
		configPath = fetchConfigPath()
	}

	if configPath != "" {
		if _, err := os.Stat(configPath); err == nil {
			if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
				panic("error loading config file: " + err.Error())
			}
			validateConfig(&cfg)
			return &cfg, configPath
		}
	}

	validateConfig(&cfg)
	return &cfg, "defaults"
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
			Engine: "trie",
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

func validateConfig(cfg *Config) {
	defaults := defaultConfig()

	if cfg.Env == "" {
		cfg.Env = defaults.Env
	}

	if cfg.DumpPath == "" {
		cfg.DumpPath = defaults.DumpPath
	}

	if cfg.FTS.Engine == "" {
		cfg.FTS.Engine = defaults.FTS.Engine
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

	if cfg.Mode.Type == "" {
		cfg.Mode.Type = defaults.Mode.Type
	}

	if cfg.FTS.Compaction.LoadFactor < 0 || cfg.FTS.Compaction.LoadFactor > 1 {
		panic("compaction load_factor must be in range [0..1]")
	}

	switch cfg.FTS.Engine {
	case "trie":
		switch cfg.FTS.Index {
		case "radix", "slicedradix", "hamt", "hamtpointered":
		default:
			panic("unknown index type: " + cfg.FTS.Index)
		}
	default:
		panic("unknown fts engine: " + cfg.FTS.Engine)
	}

	switch cfg.FTS.KeyGen {
	case "word":
	default:
		panic("unknown keygen type: " + cfg.FTS.KeyGen)
	}

	switch cfg.FTS.Scorer {
	case "none", "bm25", "tfidf":
	default:
		panic("unknown scorer type: " + cfg.FTS.Scorer)
	}

	switch cfg.FTS.Filter {
	case "none", "bloom", "cuckoo", "ribbon":
	default:
		panic("unknown filter type: " + cfg.FTS.Filter)
	}

	switch cfg.FTS.Persistence.Format {
	case "snapshot", "segment":
	default:
		panic("unknown persistence format: " + cfg.FTS.Persistence.Format)
	}

	switch cfg.FTS.Persistence.Access {
	case "file", "mmap":
	default:
		panic("unknown persistence access: " + cfg.FTS.Persistence.Access)
	}

	if cfg.FTS.Persistence.Format == "snapshot" && cfg.FTS.Persistence.Access != "file" {
		panic("snapshot persistence supports only file access")
	}

	if cfg.FTS.Cuckoo.BucketCount <= 0 {
		panic("cuckoo bucket_count must be > 0")
	}

	if cfg.FTS.Cuckoo.BucketSize <= 0 {
		panic("cuckoo bucket_size must be > 0")
	}

	if cfg.FTS.Cuckoo.MaxKicks < 0 {
		panic("cuckoo max_kicks must be >= 0")
	}

	if cfg.FTS.Filter == "ribbon" {
		if cfg.FTS.Ribbon.ExpectedItems == 0 {
			panic("ribbon expected_items must be > 0")
		}

		if cfg.FTS.Ribbon.WindowSize == 0 || cfg.FTS.Ribbon.WindowSize > 32 {
			panic("ribbon window_size must be in range [1..32]")
		}

		if cfg.FTS.Ribbon.MaxAttempts == 0 {
			panic("ribbon max_attempts must be > 0")
		}
	}

	switch cfg.Mode.Type {
	case "prod", "experiment":
	default:
		panic("unknown mode type: " + cfg.Mode.Type)
	}
}
