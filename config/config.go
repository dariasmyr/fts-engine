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
	Trie     TrieConfig     `yaml:"trie"`
	Pipeline PipelineConfig `yaml:"pipeline"`
}

type ModeConfig struct {
	Type string `yaml:"type" env-default:"prod"`
}

type TrieConfig struct {
	Type string `yaml:"type"`
}

type PipelineConfig struct {
	Lowercase   bool `yaml:"lowercase" env-default:"true"`
	StopwordsEN bool `yaml:"stopwords_en" env-default:"true"`
	StemEN      bool `yaml:"stem_en" env-default:"true"`
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
	if cfg.FTS.Index != "" && cfg.FTS.Trie.Type != "" && cfg.FTS.Index != cfg.FTS.Trie.Type {
		panic("fts.index and fts.trie.type conflict: " + cfg.FTS.Index + " != " + cfg.FTS.Trie.Type)
	}

	if cfg.FTS.Index == "" && cfg.FTS.Trie.Type != "" {
		cfg.FTS.Index = cfg.FTS.Trie.Type
	}

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

	switch cfg.Mode.Type {
	case "prod", "test", "experiment":
	default:
		panic("unknown mode type: " + cfg.Mode.Type)
	}
}
