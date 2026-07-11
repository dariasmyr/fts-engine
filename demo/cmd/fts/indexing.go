package main

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/dariasmyr/fts-engine/demo/internal/adapters/cui"
	"github.com/dariasmyr/fts-engine/demo/internal/config"
	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
	"github.com/dariasmyr/fts-engine/demo/internal/utils"
	pkgfts "github.com/dariasmyr/fts-engine/pkg/fts"
)

func runExperimentMode(ctx context.Context, log *slog.Logger, cfg *config.Config, engine cui.SearchEngine, service *pkgfts.Service, documents []models.Document) {
	startedAt := time.Now()
	memStats := utils.MeasureMemory(func() {
		for _, doc := range documents {
			_ = indexDocument(ctx, service, doc)
		}
	})
	duration := time.Since(startedAt)
	log.Info(fmt.Sprintf("Indexed %d documents in %v", len(documents), duration))

	analyzeIndex(cfg, engine, memStats, log)
}

func populateDocuments(rootCtx context.Context, ctx context.Context, log *slog.Logger, service *pkgfts.Service, documents []models.Document, documentsByID map[string]models.Document, skipIndexing bool) bool {
	for i := range documents {
		doc := documents[i]
		documentsByID[doc.ID] = doc
	}
	if skipIndexing {
		return false
	}

	for i := range documents {
		doc := documents[i]
		select {
		case <-rootCtx.Done():
			log.Info("Received shutdown signal, shutting down...")
			return true
		default:
			if err := indexDocument(ctx, service, doc); err != nil {
				log.Error("could not index document:", "error", err)
			}
		}
	}

	return false
}

func indexDocument(ctx context.Context, service *pkgfts.Service, doc models.Document) error {
	if service == nil {
		return fmt.Errorf("nil service")
	}
	fields := make(map[string]pkgfts.Field, 3)
	if doc.Title != "" {
		fields["title"] = pkgfts.Field{Value: doc.Title}
	}
	if doc.Abstract != "" {
		fields["abstract"] = pkgfts.Field{Value: doc.Abstract}
	}
	if doc.Extract != "" {
		fields["extract"] = pkgfts.Field{Value: doc.Extract}
	}
	if len(fields) == 0 {
		return nil
	}
	return service.Index(ctx, pkgfts.Document{
		ID:     pkgfts.DocID(doc.ID),
		Fields: fields,
	})
}

func analyzeIndex(
	cfg *config.Config,
	engine cui.SearchEngine,
	memStats runtime.MemStats,
	log *slog.Logger,
) {
	statsProvider, ok := engine.(interface {
		AnalyzeStats() (pkgfts.Stats, bool)
	})
	if !ok {
		log.Warn("analyzeIndex: engine does not support analysis")
		return
	}

	stats, ok := statsProvider.AnalyzeStats()
	if !ok {
		log.Warn("analyzeIndex: engine does not support analysis")
		return
	}

	log.Info("FTS analysis result",
		"index", cfg.FTS.Index,
		"nodes", stats.Nodes,
		"leafNodes", stats.Leaves,
		"maxDepth", stats.MaxDepth,
		"avgDepth", stats.AvgDepth,
		"totalDocs", stats.TotalDocs,
		"totalChildren", stats.TotalChildren,
		"heapMB", memStats.HeapAlloc/1024/1024,
		"heapObjects", memStats.HeapObjects,
		"totalAllocMB", memStats.TotalAlloc/1024/1024,
	)

	for level, avg := range stats.AvgChildrenPerLevel {
		log.Info(fmt.Sprintf("Level %d: avg children = %.2f", level, avg))
	}
}
