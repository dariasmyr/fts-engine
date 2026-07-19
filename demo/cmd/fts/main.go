package main

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/dariasmyr/fts-engine/demo/internal/adapters/loader/wiki"
	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
	"github.com/dariasmyr/fts-engine/demo/internal/lib/logger/sl"
	"github.com/dariasmyr/fts-engine/pkg/ftsbuiltin"
	"github.com/dariasmyr/fts-engine/pkg/ftsstats"
)

const (
	_readinessDrainDelay = 5 * time.Second
)

var version = ""

func buildVersion() string {
	if version != "" {
		return version
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}

	var rev string
	var dirty bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return "dev"
	}
	if len(rev) > 7 {
		rev = rev[:7]
	}
	v := "dev-" + rev
	if dirty {
		v += "+dirty"
	}
	return v
}

func main() {
	cfg, cfgSource := mustLoadConfig()

	prepareAppDirs()

	log := setupLogger(cfg.Env)
	rootCtx, stop, ctx, cancel := newShutdownContext(log)
	defer stop()
	defer cancel()

	if cfgSource == "defaults" {
		log.Warn("No config file found; using built-in defaults", "dump_path", cfg.DumpPath, "persistence_path", cfg.FTS.Persistence.Path)
	} else {
		log.Info("Loaded configuration", "source", cfgSource)
	}
	log.Info("fts", "env", cfg.Env)
	log.Info("fts", "index", cfg.FTS.Index)
	log.Info("fts", "keygen", cfg.FTS.KeyGen)
	log.Info("fts", "scorer", cfg.FTS.Scorer)
	log.Info("fts", "rank_profile", rankProfileLabel(cfg.FTS.RankProfile), "field_weights", cfg.FTS.RankProfile.FieldWeights)
	log.Info("fts", "filter", cfg.FTS.Filter)
	log.Info("fts", "compaction_load_factor", cfg.FTS.Compaction.LoadFactor)
	log.Info("fts", "compaction_auto_check", cfg.FTS.Compaction.AutoCheck)
	log.Info("fts", "mode", cfg.Mode.Type)

	if err := ftsbuiltin.RegisterSnapshotCodecs(); err != nil {
		panic(err)
	}

	documentsByID := make(map[string]models.Document)

	keyGen, err := selectKeyGenerator(cfg.FTS.KeyGen)
	if err != nil {
		log.Error("Failed to select keygen", "error", sl.Err(err))
		return
	}

	pipeline := buildPipeline(cfg)
	svc, loadedFromSnapshot, err := buildService(log, cfg, keyGen, pipeline)
	if err != nil {
		log.Error("Failed to initialize search service", "error", sl.Err(err))
		return
	}
	ftsEngine := &serviceAdapter{service: svc, snapshotLoaded: loadedFromSnapshot, searchStats: ftsstats.NewSearchStats(64)}

	log.Info("FTS engine initialised")

	dumpLoader := wiki.New(log, cfg.DumpPath)
	log.Info("Loader initialised")

	startTime := time.Now()
	documents, err := dumpLoader.LoadDocuments(ctx)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Warn("Dump file not found", "path", cfg.DumpPath)
			documents = nil
		} else {
			log.Error("Failed to load documents", "error", sl.Err(err))
			return
		}
	}
	if err := validateStartupCorpus(cfg, ftsEngine.snapshotLoaded, err); err != nil {
		log.Error("Cannot start CUI", "error", sl.Err(err))
		return
	}

	duration := time.Since(startTime)
	log.Info(fmt.Sprintf("Unpacked & parsed %d documents in %v", len(documents), duration))

	startPprofServer(log)

	if cfg.Mode.Type == "experiment" {
		runExperimentMode(ctx, log, cfg, ftsEngine, ftsEngine.service, documents)
		return
	}

	if ftsEngine.snapshotLoaded {
		log.Info("Skipping re-indexing: persisted state loaded", "path", cfg.FTS.Persistence.Path)
	}

	if interrupted := populateDocuments(rootCtx, ctx, log, ftsEngine.service, documents, documentsByID, ftsEngine.snapshotLoaded); interrupted {
		return
	}

	if !ftsEngine.snapshotLoaded {
		if err := buildFilterIfNeeded(log, ftsEngine.service); err != nil {
			log.Error("Failed to finalize search filter", "error", sl.Err(err))
			return
		}

		if err := savePersistenceIfEnabled(log, cfg, ftsEngine.service); err != nil {
			log.Error("Failed to persist state", "error", sl.Err(err))
			return
		}
	}

	if err := runCUI(ctx, log, cfg.FTS.Index, cfg.FTS.Scorer, rankProfileLabel(cfg.FTS.RankProfile), cfg.FTS.Filter, ftsEngine, documentsByID); err != nil {
		log.Error("Failed to start appCUI", "error", sl.Err(err))
		return
	}
}
