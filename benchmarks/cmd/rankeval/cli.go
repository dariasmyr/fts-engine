package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

type config struct {
	Dataset  string
	Profiles []string
	Baseline string
	Queries  int
	K        int
	Out      string
	WikiDump string
	MaxDocs  int
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("rankeval", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	dataset := fs.String("dataset", "multifield-synthetic", "dataset name: multifield-synthetic|wiki-multifield")
	profiles := fs.String("profiles", "", "comma-separated rank profile JSON files")
	baseline := fs.String("baseline", "bm25", "baseline scorer: bm25|tfidf")
	queries := fs.Int("queries", 100, "number of generated synthetic queries")
	k := fs.Int("k", 10, "top-k results")
	out := fs.String("out", "", "optional JSON output path")
	wikiDump := fs.String("wiki-dump", "", "path to wiki XML dump (.xml|.xml.gz|.xml.bz2) for -dataset=wiki-multifield")
	maxDocs := fs.Int("max-docs", 50000, "maximum wiki documents to load for -dataset=wiki-multifield")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	cfg := config{
		Dataset:  strings.TrimSpace(*dataset),
		Profiles: splitCSV(*profiles),
		Baseline: normalizedScorerName(*baseline),
		Queries:  *queries,
		K:        *k,
		Out:      strings.TrimSpace(*out),
		WikiDump: strings.TrimSpace(*wikiDump),
		MaxDocs:  *maxDocs,
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (c config) validate() error {
	switch c.Dataset {
	case "multifield-synthetic":
	case "wiki-multifield":
		if c.WikiDump == "" {
			return errors.New("rankeval: -wiki-dump is required when -dataset=wiki-multifield")
		}
		if c.MaxDocs <= 0 {
			return fmt.Errorf("rankeval: -max-docs must be > 0 for wiki-multifield, got %d", c.MaxDocs)
		}
	default:
		return fmt.Errorf("rankeval: unsupported -dataset %q", c.Dataset)
	}
	if len(c.Profiles) == 0 {
		return errors.New("rankeval: at least one profile is required via -profiles")
	}
	if _, err := scorerByName(c.Baseline); err != nil {
		return fmt.Errorf("rankeval: %w", err)
	}
	if c.Queries <= 0 {
		return fmt.Errorf("rankeval: -queries must be > 0, got %d", c.Queries)
	}
	if c.K <= 0 {
		return fmt.Errorf("rankeval: -k must be > 0, got %d", c.K)
	}
	return nil
}

func splitCSV(input string) []string {
	parts := strings.Split(input, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}
