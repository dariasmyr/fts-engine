package config

import (
	"flag"
	"fmt"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env         string     `yaml:"env" env-default:"local"`
	StoragePath string     `yaml:"storage_path" env-required:"true"`
	DumpPath    string     `yaml:"dump_path" env-default:"./data/enwiki-latest-abstract10.xml.gz"`
	FTS         FTSConfig  `yaml:"fts"`
	Mode        ModeConfig `yaml:"mode"`
}

type FTSConfig struct {
	Engine   string         `yaml:"engine" env-default:"trie"`
	Index    string         `yaml:"index"`
	KeyGen   string         `yaml:"keygen"`
	Filter   string         `yaml:"filter" env-default:"none"`
	Snapshot SnapshotConfig `yaml:"snapshot"`
	Bloom    BloomConfig    `yaml:"bloom"`
	Cuckoo   CuckooConfig   `yaml:"cuckoo"`
	Pipeline PipelineConfig `yaml:"pipeline"`
}

type SnapshotConfig struct {
	Enabled        bool   `yaml:"enabled" env-default:"false"`
	Path           string `yaml:"path" env-default:"./data/segments/default.fidx"`
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

func MustLoad() *Config {
	configPathFlag := flag.String("config", "", "Path to the config file")
	storagePathFlag := flag.String("storage-path", "", "Path to the storage file")
	flag.Parse()

	configPath := *configPathFlag
	if configPath == "" {
		configPath = fetchConfigPath() // fallback to default method
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		panic("config file does not exist: " + configPath)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		panic("error loading config file: " + err.Error())
	}

	if *storagePathFlag != "" {
		cfg.StoragePath = *storagePathFlag
	}

	if _, err := os.Stat(cfg.DumpPath); os.IsNotExist(err) {
		fmt.Printf("Error: DumpPath does not exist: %s", cfg.DumpPath)
	}

	validateConfig(&cfg)

	return &cfg
}

// fetchConfigPath fetches domain path from environment variable or default if it was not set in command line flag.
// Priority: flag > env > default.
// Default value is empty string.
func fetchConfigPath() string {
	var res string

	res = os.Getenv("CONFIG_PATH")
	if res == "" {
		cwd, _ := os.Getwd()
		fmt.Println("Current working directory:", cwd)
	}

	if res == "" {
		res = "./config/config_local.yaml" // default path
	}

	fmt.Println("Config path:", res)
	return res
}

func validateConfig(cfg *Config) {
	if cfg.FTS.Index == "" {
		cfg.FTS.Index = "radix"
	}

	if cfg.FTS.KeyGen == "" {
		if cfg.FTS.Index == "trigram" {
			cfg.FTS.KeyGen = "trigram"
		} else {
			cfg.FTS.KeyGen = "word"
		}
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
		case "radix", "slicedradix", "hamt", "hamtpointered", "trigram":
		default:
			panic("unknown index type: " + cfg.FTS.Index)
		}
	case "kv":
	default:
		panic("unknown fts engine: " + cfg.FTS.Engine)
	}

	switch cfg.FTS.KeyGen {
	case "word", "trigram":
	default:
		panic("unknown keygen type: " + cfg.FTS.KeyGen)
	}

	switch cfg.FTS.Filter {
	case "none", "bloom", "cuckoo":
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

	switch cfg.Mode.Type {
	case "prod", "experiment":
	default:
		panic("unknown mode type: " + cfg.Mode.Type)
	}
}
