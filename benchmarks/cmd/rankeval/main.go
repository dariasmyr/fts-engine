package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/fts"
)

const schemaVersion = "rankeval.v1alpha1"

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(context.Background(), cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, stdout io.Writer) error {
	docs, queries, datasetMeta, err := loadDataset(cfg)
	if err != nil {
		return err
	}
	baseScorer, err := scorerByName(cfg.Baseline)
	if err != nil {
		return err
	}

	baselineReport, err := evaluate(ctx, docs, queries, cfg.K, fts.WithScorer(baseScorer))
	if err != nil {
		return fmt.Errorf("evaluate baseline: %w", err)
	}
	baselineMetrics := metricsFromReport(baselineReport)
	runs := []evalRun{{
		Name:    cfg.Baseline,
		Kind:    "baseline",
		Base:    cfg.Baseline,
		Metrics: baselineMetrics,
	}}

	for _, path := range cfg.Profiles {
		profile, err := loadRankProfile(path)
		if err != nil {
			return err
		}
		base, err := scorerByName(profile.Base)
		if err != nil {
			return err
		}
		rep, err := evaluate(ctx, docs, queries, cfg.K, fts.WithRankProfile(fts.RankProfile{
			Name:         profile.Name,
			Base:         base,
			FieldWeights: profile.FieldWeights,
		}))
		if err != nil {
			return fmt.Errorf("evaluate profile %q: %w", profile.Name, err)
		}
		m := metricsFromReport(rep)
		runs = append(runs, evalRun{
			Name:    profile.Name,
			Kind:    "profile",
			Base:    profile.Base,
			File:    path,
			Profile: profile,
			Metrics: m,
			Delta:   deltaMetrics(m, baselineMetrics),
			Queries: compareQueries(rep, baselineReport),
		})
	}

	report := reportFile{
		SchemaVersion: schemaVersion,
		Dataset:       datasetMeta,
		Runs:          runs,
	}
	if cfg.Out != "" {
		if err := writeJSON(cfg.Out, report); err != nil {
			return err
		}
	}
	return writeTable(stdout, runs)
}
