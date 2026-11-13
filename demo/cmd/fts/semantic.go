package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dariasmyr/fts-engine/demo/internal/config"
	"github.com/dariasmyr/fts-engine/demo/internal/domain/models"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding/ollama"
	pkgfts "github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
)

type semanticRuntime struct {
	service         *service.Service
	generation      semanticpersist.Generation
	loadedFromDisk  bool
	persistencePath string
	persistence     bool
	saveOnBuild     bool
	workers         int
}

func buildSemanticRuntime(ctx context.Context, log *slog.Logger, cfg config.SemanticConfig) (*semanticRuntime, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	client, err := ollama.New(ctx, ollama.Config{
		BaseURL:        cfg.Ollama.BaseURL,
		Model:          cfg.Ollama.Model,
		Dimensions:     cfg.Ollama.Dimensions,
		ContextTokens:  cfg.Ollama.ContextTokens,
		DocumentPrefix: cfg.Ollama.DocumentPrefix,
		QueryPrefix:    cfg.Ollama.QueryPrefix,
		Truncate:       cfg.Ollama.Truncate,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to Ollama embedding model: %w", err)
	}

	runtimeState := &semanticRuntime{
		persistencePath: cfg.Persistence.Path,
		persistence:     cfg.Persistence.Enabled,
		saveOnBuild:     cfg.Persistence.SaveOnBuild,
		workers:         cfg.Workers,
	}
	currentPath := filepath.Join(cfg.Persistence.Path, "CURRENT")
	if cfg.Persistence.Enabled && cfg.Persistence.LoadOnStart {
		if _, statErr := os.Stat(currentPath); statErr == nil {
			loader, loadErr := semanticpersist.NewLoader(cfg.Persistence.Path, semanticpersist.LoadOptions{})
			if loadErr != nil {
				return nil, fmt.Errorf("configure semantic persistence loader: %w", loadErr)
			}
			loaded, generationID, loadErr := service.Load(ctx, loader, client)
			if loadErr != nil {
				return nil, fmt.Errorf("load semantic index: %w", loadErr)
			}
			runtimeState.service = loaded
			runtimeState.generation = semanticpersist.Generation{ID: generationID}
			runtimeState.loadedFromDisk = true
			log.Info("Loaded semantic index", "path", cfg.Persistence.Path, "generation", generationID)
			return runtimeState, nil
		} else if !os.IsNotExist(statErr) {
			return nil, fmt.Errorf("inspect semantic persistence: %w", statErr)
		}
	}

	serviceConfig, err := service.DefaultConfig(client.Model())
	if err != nil {
		return nil, fmt.Errorf("create semantic config: %w", err)
	}
	serviceConfig.Index.Operational.Capacity.MaxLiveVectors = cfg.MaxLiveVectors
	serviceConfig.Index.Operational.Capacity.MaxStaleVectors = cfg.MaxLiveVectors
	semanticService, err := service.New(client, serviceConfig)
	if err != nil {
		return nil, fmt.Errorf("create semantic service: %w", err)
	}
	runtimeState.service = semanticService
	return runtimeState, nil
}

func populateSemanticDocuments(
	ctx context.Context,
	log *slog.Logger,
	runtimeState *semanticRuntime,
	documents []models.Document,
) error {
	if runtimeState == nil || runtimeState.service == nil || runtimeState.loadedFromDisk {
		return nil
	}

	startedAt := time.Now()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan models.Document)
	var indexed atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var workers sync.WaitGroup
	for range runtimeState.workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for document := range jobs {
				if err := addSemanticDocument(workerCtx, runtimeState.service, document); err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
				count := indexed.Add(1)
				if count%10_000 == 0 {
					log.Info("Semantic embedding progress", "documents", count, "elapsed", time.Since(startedAt))
				}
			}
		}()
	}

sendLoop:
	for _, document := range documents {
		select {
		case <-workerCtx.Done():
			break sendLoop
		case jobs <- document:
		}
	}
	close(jobs)
	workers.Wait()
	if firstErr != nil {
		return fmt.Errorf("embed semantic document after %d documents: %w", indexed.Load(), firstErr)
	}
	if err := workerCtx.Err(); err != nil {
		return err
	}

	embeddingDuration := time.Since(startedAt)
	log.Info("Semantic embeddings staged",
		"documents", indexed.Load(),
		"duration", embeddingDuration,
		"documents_per_second", rate(indexed.Load(), embeddingDuration),
	)

	commitStartedAt := time.Now()
	if err := runtimeState.service.Commit(ctx); err != nil {
		return fmt.Errorf("commit semantic HNSW index: %w", err)
	}
	commitDuration := time.Since(commitStartedAt)
	stats := runtimeState.service.Statistics()
	log.Info("Semantic HNSW committed",
		"duration", commitDuration,
		"documents", stats.Documents,
		"live_vectors", stats.LiveVectors,
	)

	var persistenceDuration time.Duration
	var persistedBytes int64
	if runtimeState.persistence && runtimeState.saveOnBuild {
		if err := os.MkdirAll(filepath.Dir(runtimeState.persistencePath), 0o755); err != nil {
			return fmt.Errorf("create semantic persistence parent: %w", err)
		}
		persistStartedAt := time.Now()
		generation, err := semanticpersist.SaveService(ctx, runtimeState.persistencePath, runtimeState.service, semanticpersist.SaveOptions{
			ExpectedGeneration: &runtimeState.generation,
		})
		if err != nil {
			return fmt.Errorf("persist semantic index: %w", err)
		}
		runtimeState.generation = generation
		persistenceDuration = time.Since(persistStartedAt)
		persistedBytes, err = directorySize(runtimeState.persistencePath)
		if err != nil {
			return fmt.Errorf("measure semantic persistence: %w", err)
		}
		log.Info("Semantic index persisted",
			"duration", persistenceDuration,
			"path", runtimeState.persistencePath,
			"bytes", persistedBytes,
			"generation", generation.ID,
		)
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	log.Info("Semantic indexing summary",
		"documents", stats.Documents,
		"vectors", stats.LiveVectors,
		"embedding_duration", embeddingDuration,
		"hnsw_duration", commitDuration,
		"persistence_duration", persistenceDuration,
		"total_duration", time.Since(startedAt),
		"heap_alloc_bytes", after.HeapAlloc,
		"allocated_bytes", after.TotalAlloc-before.TotalAlloc,
		"persisted_bytes", persistedBytes,
	)
	return nil
}

func addSemanticDocument(ctx context.Context, semanticService *service.Service, document models.Document) error {
	parts := make([]string, 0, 3)
	for _, text := range []string{document.Title, document.Abstract, document.Extract} {
		if text = strings.TrimSpace(text); text != "" {
			parts = append(parts, text)
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return semanticService.AddDocument(ctx, pkgfts.Document{
		ID: pkgfts.DocID(document.ID),
		Fields: map[string]pkgfts.FieldData{
			"content": {Text: strings.Join(parts, "\n")},
		},
	})
}

func rate(count int64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(count) / duration.Seconds()
}

func directorySize(root string) (int64, error) {
	var size int64
	err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	})
	return size, err
}
