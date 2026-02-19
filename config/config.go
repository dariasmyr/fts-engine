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
	Engine  string        `yaml:"engine" env-default:"trie"`
	Indexer IndexerConfig `yaml:"indexer"`
	Filter  FilterConfig  `yaml:"filter"`
}

type ModeConfig struct {
	Type string `yaml:"type" env-default:"prod"`
}

type IndexerConfig struct {
	Type string `yaml:"type" env-default:"radix"`
}

type FilterConfig struct {
	Type   string       `yaml:"type" env-default:"none"`
	Bloom  BloomConfig  `yaml:"bloom"`
	Cuckoo CuckooConfig `yaml:"cuckoo"`
	Ribbon RibbonConfig `yaml:"ribbon"`
}

type BloomConfig struct {
	Capacity uint64 `yaml:"capacity" env-default:"1000000"`
	Hashes   uint64 `yaml:"hashes" env-default:"7"`
}

type CuckooConfig struct {
	Capacity uint64 `yaml:"capacity" env-default:"1000000"`
	BucketSz int    `yaml:"bucket_size" env-default:"4"`
	MaxKicks int    `yaml:"max_kicks" env-default:"500"`
}

type RibbonConfig struct {
	Bits  uint64 `yaml:"bits" env-default:"1048576"`
	Width uint64 `yaml:"width" env-default:"8"`
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
	indexerType := cfg.FTS.Indexer.Type
	if indexerType == "" {
		indexerType = "radix"
	}
	filterType := cfg.FTS.Filter.Type
	if filterType == "" {
		filterType = "none"
	}

	switch cfg.FTS.Engine {
	case "trie":
		switch indexerType {
		case "radix", "slicedradix", "hamt", "hamtpointered", "trigram":
		default:
			panic("unknown indexer type: " + indexerType)
		}

		switch filterType {
		case "none", "bloom", "cuckoo", "ribbon":
		default:
			panic("unknown filter type: " + filterType)
		}
	case "kv":
	default:
		panic("unknown fts engine: " + cfg.FTS.Engine)
	}
}
