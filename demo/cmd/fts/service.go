package main

import (
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/dariasmyr/fts-engine/demo/internal/config"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func selectScorer(kind string) (pkgfts.Scorer, error) {
	switch kind {
	case "", "none":
		return nil, nil
	case "bm25":
		return pkgfts.BM25(), nil
	case "tfidf":
		return pkgfts.TFIDF(), nil
	default:
		return nil, fmt.Errorf("unknown scorer %q", kind)
	}
}

func selectScoringOption(cfg config.FTSConfig) (pkgfts.Option, error) {
	scorer, err := selectScorer(cfg.Scorer)
	if err != nil {
		return nil, err
	}
	if rankProfileConfigured(cfg.RankProfile) {
		if scorer == nil {
			return nil, fmt.Errorf("rank profile requires scorer")
		}
		return pkgfts.WithRankProfile(pkgfts.RankProfile{
			Name:         cfg.RankProfile.Name,
			Base:         scorer,
			FieldWeights: pkgfts.FieldWeights(maps.Clone(cfg.RankProfile.FieldWeights)),
			QueryTypeWeights: pkgfts.QueryTypeWeights{
				Term:       cfg.RankProfile.QueryTypeWeights.Term,
				Prefix:     cfg.RankProfile.QueryTypeWeights.Prefix,
				Phrase:     cfg.RankProfile.QueryTypeWeights.Phrase,
				NearPhrase: cfg.RankProfile.QueryTypeWeights.NearPhrase,
			},
		}), nil
	}
	if scorer == nil {
		return nil, nil
	}
	return pkgfts.WithScorer(scorer), nil
}

func rankProfileLabel(profile config.RankProfileConfig) string {
	if !rankProfileConfigured(profile) {
		return ""
	}
	if profile.Name != "" {
		return profile.Name
	}
	return "custom"
}

func rankProfileConfigured(profile config.RankProfileConfig) bool {
	return len(profile.FieldWeights) > 0 || profile.QueryTypeWeights != (config.QueryTypeWeightsConfig{})
}

func buildService(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, pipeline textproc.Pipeline) (*pkgfts.Service, bool, error) {
	if cfg == nil {
		return nil, false, fmt.Errorf("nil config")
	}

	scorerOpt, err := selectScoringOption(cfg.FTS)
	if err != nil {
		return nil, false, err
	}

	serviceOpts := []pkgfts.Option{pkgfts.WithPipeline(pipeline)}
	if scorerOpt != nil {
		serviceOpts = append(serviceOpts, scorerOpt)
	}
	serviceOpts = append(serviceOpts,
		pkgfts.WithCompactionLoadFactor(cfg.FTS.Compaction.LoadFactor),
		pkgfts.WithAutoCompactionCheck(cfg.FTS.Compaction.AutoCheck),
		pkgfts.WithCompactionCallback(func(stats pkgfts.CompactionStats) {
			log.Warn("FTS compaction threshold reached",
				"load_factor", stats.TombstoneLoadFactor,
				"threshold", cfg.FTS.Compaction.LoadFactor,
				"tombstoned_docs", stats.TombstonedDocs,
				"live_docs", stats.LiveDocs,
				"total_assigned_docs", stats.TotalAssignedDocs,
			)
		}),
	)

	if cfg.Mode.Type == "prod" && cfg.FTS.Persistence.Enabled && cfg.FTS.Persistence.LoadOnStart {
		svc, ok, err := tryLoadPersistence(log, cfg, keyGen, serviceOpts)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return svc, true, nil
		}
	}

	searchFilter, err := selectFilter(cfg)
	if err != nil {
		return nil, false, err
	}

	if searchFilter != nil {
		serviceOpts = append(serviceOpts, pkgfts.WithFilter(searchFilter))
	}

	svc := pkgfts.NewMultiField(func(string) (pkgfts.Index, error) {
		return selectIndex(cfg.FTS.Index)
	}, keyGen, serviceOpts...)
	return svc, false, nil
}

func selectKeyGenerator(kind string) (pkgfts.KeyGenerator, error) {
	switch kind {
	case "word":
		return keygen.Word, nil
	default:
		return nil, fmt.Errorf("unknown keygen %q", kind)
	}
}

func selectIndex(name string) (pkgfts.Index, error) {
	return ftsbuiltin.BuildIndex(name)
}

func selectFilter(cfg *config.Config) (pkgfts.Filter, error) {
	return ftsbuiltin.BuildFilter(cfg.FTS.Filter, buildFilterOptions(cfg))
}

func buildFilterOptions(cfg *config.Config) ftsbuiltin.FilterOptions {
	if cfg == nil {
		return ftsbuiltin.FilterOptions{}
	}

	return ftsbuiltin.FilterOptions{
		BloomExpectedItems:  cfg.FTS.Bloom.ExpectedItems,
		BloomBitsPerItem:    cfg.FTS.Bloom.BitsPerItem,
		BloomK:              cfg.FTS.Bloom.K,
		CuckooBucketCount:   cfg.FTS.Cuckoo.BucketCount,
		CuckooBucketSize:    cfg.FTS.Cuckoo.BucketSize,
		CuckooMaxKicks:      cfg.FTS.Cuckoo.MaxKicks,
		RibbonExpectedItems: cfg.FTS.Ribbon.ExpectedItems,
		RibbonExtraCells:    cfg.FTS.Ribbon.ExtraCells,
		RibbonWindowSize:    cfg.FTS.Ribbon.WindowSize,
		RibbonSeed:          cfg.FTS.Ribbon.Seed,
		RibbonMaxAttempts:   cfg.FTS.Ribbon.MaxAttempts,
	}
}

func buildFilterIfNeeded(log *slog.Logger, svc *pkgfts.Service) error {
	if svc == nil {
		return nil
	}

	startedAt := time.Now()
	if err := svc.BuildFilter(); err != nil {
		return fmt.Errorf("build search filter: %w", err)
	}

	log.Info("Search filter finalized", "duration", time.Since(startedAt))
	return nil
}

func buildPipeline(cfg *config.Config) textproc.Pipeline {
	if cfg.FTS.Pipeline.Preset == "observability" {
		return textproc.ObservabilityPipeline()
	}

	filters := make([]textproc.Filter, 0, 4)

	if cfg.FTS.Pipeline.Lowercase {
		filters = append(filters, textproc.LowercaseFilter{})
	}

	if cfg.FTS.Pipeline.MinLength > 0 {
		filters = append(filters, textproc.MinLengthOrNumericFilter{MinLength: cfg.FTS.Pipeline.MinLength})
	}

	if cfg.FTS.Pipeline.StopwordsEN {
		filters = append(filters, textproc.EnglishStopwordFilter{})
	}

	if cfg.FTS.Pipeline.StopwordsRU {
		filters = append(filters, textproc.RussianStopwordFilter{})
	}

	if cfg.FTS.Pipeline.StemEN {
		filters = append(filters, textproc.EnglishStemFilter{})
	}

	if cfg.FTS.Pipeline.StemRU {
		filters = append(filters, textproc.RussianStemFilter{})
	}

	name := fmt.Sprintf(
		"demo-custom/lowercase=%t/min=%d/stopwords-en=%t/stopwords-ru=%t/stem-en=%t/stem-ru=%t",
		cfg.FTS.Pipeline.Lowercase,
		cfg.FTS.Pipeline.MinLength,
		cfg.FTS.Pipeline.StopwordsEN,
		cfg.FTS.Pipeline.StopwordsRU,
		cfg.FTS.Pipeline.StemEN,
		cfg.FTS.Pipeline.StemRU,
	)
	return textproc.NewNamedPipeline(name, 1, textproc.AlnumTokenizer{}, filters...)
}
