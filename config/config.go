package config

import (
	"flag"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env      string     `yaml:"env" env-default:"local"`
	DumpPath string     `yaml:"dump_path" env-default:"./data/enwiki-latest-abstract10.xml.gz"`
	FTS      FTSConfig  `yaml:"fts"`
	Mode     ModeConfig `yaml:"mode"`
}

type FTSConfig struct {
	Engine   string         `yaml:"engine" env-default:"trie"`
	Index    string         `yaml:"index"`
	KeyGen   string         `yaml:"keygen"`
	Filter   string         `yaml:"filter" env-default:"none"`
	Snapshot SnapshotConfig `yaml:"snapshot"`
	Bloom    BloomConfig    `yaml:"bloom"`
	Cuckoo   CuckooConfig   `yaml:"cuckoo"`
	Ribbon   RibbonConfig   `yaml:"ribbon"`
	Pipeline PipelineConfig `yaml:"pipeline"`
}

type SnapshotConfig struct {
	Enabled        bool   `yaml:"enabled" env-default:"false"`
	Path           string `yaml:"path" env-default:"./data/segments/default.fidx"`
	IndexPath      string `yaml:"index_path" env-default:""`
	FilterPath     string `yaml:"filter_path" env-default:""`
	LoadOnStart    bool   `yaml:"load_on_start" env-default:"true"`
	SaveOnBuild    bool   `yaml:"save_on_build" env-default:"true"`
	BufferSize     int    `yaml:"buffer_size" env-default:"1048576"`
	FlushThreshold int    `yaml:"flush_threshold" env-default:"262144"`
	SyncFile       bool   `yaml:"sync_file" env-default:"true"`
}

type BloomConfig struct {
	ExpectedItems uint64 `yaml:"expected_items" env-default:"1000000"`
	BitsPerItem   uint64 `yaml:"bits_per_item" env-default:"10"`
	K             uint64 `yaml:"k" env-default:"7"`
}

type CuckooConfig struct {
	BucketCount int `yaml:"bucket_count" env-default:"262144"`
	BucketSize  int `yaml:"bucket_size" env-default:"4"`
	MaxKicks    int `yaml:"max_kicks" env-default:"500"`
}

type RibbonConfig struct {
	ExpectedItems uint32 `yaml:"expected_items" env-default:"1000000"`
	ExtraCells    uint32 `yaml:"extra_cells" env-default:"250000"`
	WindowSize    uint32 `yaml:"window_size" env-default:"24"`
	Seed          uint64 `yaml:"seed" env-default:"0"`
	MaxAttempts   uint32 `yaml:"max_attempts" env-default:"5"`
}

type ModeConfig struct {
	Type string `yaml:"type" env-default:"prod"`
}

type PipelineConfig struct {
	Lowercase   bool `yaml:"lowercase" env-default:"true"`
	StopwordsEN bool `yaml:"stopwords_en" env-default:"true"`
	StopwordsRU bool `yaml:"stopwords_ru" env-default:"false"`
	StemEN      bool `yaml:"stem_en" env-default:"true"`
	StemRU      bool `yaml:"stem_ru" env-default:"false"`
	MinLength   int  `yaml:"min_length" env-default:"3"`
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
			Filter: "ribbon",
			Snapshot: SnapshotConfig{
				Enabled:        true,
				Path:           "./data/segments/local.fidx",
				IndexPath:      "./data/segments/local.index.fidx",
				FilterPath:     "./data/segments/local.filter.fidx",
				LoadOnStart:    false,
				SaveOnBuild:    true,
				BufferSize:     1048576,
				FlushThreshold: 262144,
				SyncFile:       true,
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
	if cfg.FTS.Index == "" {
		cfg.FTS.Index = "radix"
	}

	if cfg.FTS.KeyGen == "" {
		cfg.FTS.KeyGen = "word"
	}

	if cfg.FTS.Filter == "" {
		cfg.FTS.Filter = "none"
	}

	if cfg.FTS.Snapshot.Path == "" {
		cfg.FTS.Snapshot.Path = "./data/segments/default.fidx"
	}

	if cfg.FTS.Snapshot.BufferSize <= 0 {
		cfg.FTS.Snapshot.BufferSize = 1048576
	}

	if cfg.FTS.Snapshot.FlushThreshold <= 0 {
		cfg.FTS.Snapshot.FlushThreshold = 262144
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

	switch cfg.FTS.Filter {
	case "none", "bloom", "cuckoo", "ribbon":
	default:
		panic("unknown filter type: " + cfg.FTS.Filter)
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
