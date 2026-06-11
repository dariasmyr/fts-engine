package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

type config struct {
	Engines []string
	Dataset string
	K       int
	Out     string
	Work    string
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		engines = fs.String("engines", "fts-engine", "comma-separated engine list")
		dataset = fs.String("dataset", "synthetic", "dataset name")
		k       = fs.Int("k", 10, "top-k results")
		out     = fs.String("out", "", "JSON output path")
		work    = fs.String("work", "./work", "work directory for engine state")
	)

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := config{
		Engines: splitCSV(*engines),
		Dataset: strings.TrimSpace(*dataset),
		K:       *k,
		Out:     strings.TrimSpace(*out),
		Work:    strings.TrimSpace(*work),
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func (c config) validate() error {
	if len(c.Engines) == 0 {
		return errors.New("bench: at least one engine must be provided via -engines")
	}
	if c.Dataset == "" {
		return errors.New("bench: -dataset must not be empty")
	}
	if c.K <= 0 {
		return fmt.Errorf("bench: -k must be > 0, got %d", c.K)
	}
	if c.Work == "" {
		return errors.New("bench: -work must not be empty")
	}
	return nil
}

func run(cfg config) error {
	_ = cfg
	return nil
}

func splitCSV(input string) []string {
	parts := strings.Split(input, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}
