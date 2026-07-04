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
			_ = indexAbstractDocument(ctx, service, doc.ID, doc.Abstract)
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
			if err := indexAbstractDocument(ctx, service, doc.ID, doc.Abstract); err != nil {
				log.Error("could not index document:", "error", err)
			}
		}
	}

	return false
}

func indexAbstractDocument(ctx context.Context, service *pkgfts.Service, docID string, content string) error {
	if service == nil {
		return fmt.Errorf("nil service")
	}
	return service.Index(ctx, pkgfts.Document{
		ID: pkgfts.DocID(docID),
		Fields: map[string]pkgfts.Field{
			pkgfts.DefaultField: {Value: content},
		},
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
