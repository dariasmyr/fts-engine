package config

import (
	"flag"
	"fmt"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env         string `yaml:"env" env-default:"local"`
	StoragePath string `yaml:"storage_path" env-required:"true"`
	DumpPath    string `yaml:"dump_path" env-default:"./data/enwiki-latest-abstract10.xml.gz"`
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
