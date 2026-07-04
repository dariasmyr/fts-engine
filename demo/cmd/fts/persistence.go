package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/dariasmyr/fts-engine/demo/internal/config"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/ftspersist"
)

func tryLoadPersistence(log *slog.Logger, cfg *config.Config, keyGen pkgfts.KeyGenerator, serviceOpts []pkgfts.Option) (*pkgfts.Service, bool, error) {
	if cfg == nil || cfg.FTS.Persistence.Path == "" {
		return nil, false, nil
	}

	expectedFilter := cfg.FTS.Filter
	if expectedFilter == "none" {
		expectedFilter = ""
	}

	switch cfg.FTS.Persistence.Format {
	case "snapshot":
		indexPath := persistenceSnapshotIndexPath(cfg)
		filterPath := persistenceSnapshotFilterPath(cfg)
		if expectedFilter == "" {
			if _, err := os.Stat(filterPath); errors.Is(err, os.ErrNotExist) {
				filterPath = ""
			} else if err != nil && filterPath != "" {
				return nil, false, fmt.Errorf("check persistence filter path: %w", err)
			}
		}
		loaded, err := ftspersist.LoadSnapshot(ftspersist.SnapshotPaths{IndexPath: indexPath, FilterPath: filterPath}, keyGen, serviceOpts...)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, false, nil
			}
			return nil, false, err
		}
		if loaded.IndexName != "" && loaded.IndexName != cfg.FTS.Index {
			log.Warn("Snapshot index type differs from config",
				"snapshot_index", loaded.IndexName,
				"config_index", cfg.FTS.Index,
				"path", indexPath,
			)
		}
		if loaded.FilterName != expectedFilter {
			log.Warn("Snapshot filter type differs from config",
				"snapshot_filter", loaded.FilterName,
				"config_filter", cfg.FTS.Filter,
				"path", filterPath,
			)
		}
		log.Info("Loaded snapshot persistence", "index_path", indexPath, "filter_path", filterPath)
		return loaded.Service, true, nil
	case "segment":
		loaded, err := ftspersist.LoadSegment(
			ftspersist.SegmentPaths{Dir: cfg.FTS.Persistence.Path},
			keyGen,
			ftspersist.SegmentLoadOptions{Access: persistenceAccessMode(cfg)},
			serviceOpts...,
		)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, false, nil
			}
			return nil, false, err
		}
		if loaded.FilterName != expectedFilter {
			log.Warn("Segment filter type differs from config",
				"segment_filter", loaded.FilterName,
				"config_filter", cfg.FTS.Filter,
				"path", cfg.FTS.Persistence.Path,
			)
		}
		log.Info("Loaded segment persistence", "dir_path", cfg.FTS.Persistence.Path, "access", cfg.FTS.Persistence.Access)
		return loaded.Service, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported persistence format %q", cfg.FTS.Persistence.Format)
	}
}

func savePersistenceIfEnabled(log *slog.Logger, cfg *config.Config, svc *pkgfts.Service) error {
	if cfg == nil || svc == nil {
		return nil
	}

	if !cfg.FTS.Persistence.Enabled || !cfg.FTS.Persistence.SaveOnBuild {
		return nil
	}

	filterName := cfg.FTS.Filter
	if filterName == "none" {
		filterName = ""
	}
	opts := ftspersist.SaveOptions{
		BufferSize:     cfg.FTS.Persistence.BufferSize,
		FlushThreshold: cfg.FTS.Persistence.FlushThreshold,
		SyncFile:       cfg.FTS.Persistence.SyncFile,
	}

	switch cfg.FTS.Persistence.Format {
	case "snapshot":
		indexPath := persistenceSnapshotIndexPath(cfg)
		filterPath := persistenceSnapshotFilterPath(cfg)
		if err := ftspersist.SaveSnapshot(ftspersist.SnapshotPaths{IndexPath: indexPath, FilterPath: filterPath}, svc, cfg.FTS.Index, filterName, opts); err != nil {
			return err
		}
		log.Info("FTS snapshot persisted", "index_path", indexPath, "filter_path", filterPath)
		return nil
	case "segment":
		if err := ftspersist.SaveSegment(ftspersist.SegmentPaths{Dir: cfg.FTS.Persistence.Path}, svc, filterName, opts); err != nil {
			return err
		}
		log.Info("FTS segment persisted", "dir_path", cfg.FTS.Persistence.Path)
		return nil
	default:
		return fmt.Errorf("unsupported persistence format %q", cfg.FTS.Persistence.Format)
	}
}

func persistenceSnapshotIndexPath(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return filepath.Join(cfg.FTS.Persistence.Path, "index.fidx")
}

func persistenceSnapshotFilterPath(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return filepath.Join(cfg.FTS.Persistence.Path, "filter.fidx")
}

func persistenceAccessMode(cfg *config.Config) ftspersist.AccessMode {
	if cfg == nil {
		return ftspersist.AccessFile
	}
	switch cfg.FTS.Persistence.Access {
	case "mmap":
		return ftspersist.AccessMmap
	default:
		return ftspersist.AccessFile
	}
}

func validateStartupCorpus(cfg *config.Config, snapshotLoaded bool, dumpErr error) error {
	if cfg == nil || cfg.Mode.Type != "prod" {
		return nil
	}
	if snapshotLoaded || !errors.Is(dumpErr, os.ErrNotExist) {
		return nil
	}

	hints := []string{
		fmt.Sprintf("set dump_path to an existing dump file (current: %s)", cfg.DumpPath),
	}
	if hasPersistedState(cfg) {
		hints = append(hints, fmt.Sprintf("or enable fts.persistence.load_on_start=true to load the existing persisted index from %s", cfg.FTS.Persistence.Path))
	} else if cfg.FTS.Persistence.Enabled {
		hints = append(hints, fmt.Sprintf("or create persistence first; current persistence path is %s", cfg.FTS.Persistence.Path))
	}

	return fmt.Errorf("no search corpus available: dump file is missing and no persisted index was loaded; %s", strings.Join(hints, "; "))
}

func hasPersistedState(cfg *config.Config) bool {
	if cfg == nil || !cfg.FTS.Persistence.Enabled || cfg.FTS.Persistence.Path == "" {
		return false
	}

	switch cfg.FTS.Persistence.Format {
	case "snapshot":
		_, err := os.Stat(persistenceSnapshotIndexPath(cfg))
		return err == nil
	case "segment":
		_, err := os.Stat(cfg.FTS.Persistence.Path)
		return err == nil
	default:
		return false
	}
}
