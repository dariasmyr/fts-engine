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
	service           *service.Service
	generation        semanticpersist.Generation
	loadedFromDisk    bool
	persistencePath   string
	persistence       bool
	saveOnBuild       bool
	workers           int
	checkpointDocs    int
	persistenceLimits semanticpersist.Limits
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
		persistencePath:   cfg.Persistence.Path,
		persistence:       cfg.Persistence.Enabled,
		saveOnBuild:       cfg.Persistence.SaveOnBuild,
		workers:           cfg.Workers,
		checkpointDocs:    cfg.CheckpointDocuments,
		persistenceLimits: semanticpersist.Limits{MaxOpenBytes: cfg.Persistence.MaxOpenBytes},
	}
	currentPath := filepath.Join(cfg.Persistence.Path, "CURRENT")
	if cfg.Persistence.Enabled && cfg.Persistence.LoadOnStart {
		if _, statErr := os.Stat(currentPath); statErr == nil {
			loader, loadErr := semanticpersist.NewLoader(cfg.Persistence.Path, semanticpersist.LoadOptions{
				Limits: runtimeState.persistenceLimits,
			})
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
	if runtimeState == nil || runtimeState.service == nil {
		return nil
	}

	startedAt := time.Now()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	existingIDs, err := semanticDocumentIDs(ctx, runtimeState.service)
	if err != nil {
		return fmt.Errorf("inspect committed semantic documents: %w", err)
	}
	missing := make([]models.Document, 0, len(documents)-min(len(documents), len(existingIDs)))
	for _, document := range documents {
		if _, exists := existingIDs[pkgfts.DocID(document.ID)]; !exists {
			missing = append(missing, document)
		}
	}
	if len(existingIDs) > 0 {
		log.Info("Resuming semantic indexing",
			"committed_documents", len(existingIDs),
			"remaining_documents", len(missing),
			"generation", runtimeState.generation.ID,
		)
	}
	if len(missing) == 0 {
		log.Info("Semantic index already covers the corpus", "documents", len(existingIDs))
		return nil
	}

	checkpointSize := len(missing)
	if runtimeState.persistence && runtimeState.saveOnBuild {
		checkpointSize = runtimeState.checkpointDocs
		if err := os.MkdirAll(filepath.Dir(runtimeState.persistencePath), 0o755); err != nil {
			return fmt.Errorf("create semantic persistence parent: %w", err)
		}
	}

	var indexed atomic.Int64
	var embeddingDuration, commitDuration, persistenceDuration time.Duration
	for offset := 0; offset < len(missing); offset += checkpointSize {
		end := min(offset+checkpointSize, len(missing))
		embedStartedAt := time.Now()
		if err := embedSemanticBatch(
			ctx, log, runtimeState, missing[offset:end], int64(len(existingIDs)), &indexed, startedAt,
		); err != nil {
			return err
		}
		embeddingDuration += time.Since(embedStartedAt)

		commitStartedAt := time.Now()
		if err := runtimeState.service.Commit(ctx); err != nil {
			return fmt.Errorf("commit semantic HNSW checkpoint: %w", err)
		}
		checkpointCommitDuration := time.Since(commitStartedAt)
		commitDuration += checkpointCommitDuration
		stats := runtimeState.service.Statistics()

		var checkpointPersistenceDuration time.Duration
		if runtimeState.persistence && runtimeState.saveOnBuild {
			persistStartedAt := time.Now()
			generation, err := semanticpersist.SaveService(ctx, runtimeState.persistencePath, runtimeState.service, semanticpersist.SaveOptions{
				ExpectedGeneration: &runtimeState.generation,
				Limits:             runtimeState.persistenceLimits,
			})
			if err != nil {
				return fmt.Errorf("persist semantic index: %w", err)
			}
			runtimeState.generation = generation
			if _, pruneErr := semanticpersist.Prune(ctx, runtimeState.persistencePath, semanticpersist.PruneOptions{
				RetainGenerations: 2,
				Limits:            runtimeState.persistenceLimits,
			}); pruneErr != nil {
				log.Warn("Could not prune old semantic generations", "error", pruneErr)
			}
			checkpointPersistenceDuration = time.Since(persistStartedAt)
			persistenceDuration += checkpointPersistenceDuration
		}

		log.Info("Semantic checkpoint committed",
			"documents", stats.Documents,
			"live_vectors", stats.LiveVectors,
			"hnsw_duration", checkpointCommitDuration,
			"persistence_duration", checkpointPersistenceDuration,
			"generation", runtimeState.generation.ID,
		)
	}

	stats := runtimeState.service.Statistics()
	var persistedBytes int64
	if runtimeState.persistence && runtimeState.saveOnBuild {
		persistedBytes, err = directorySize(runtimeState.persistencePath)
		if err != nil {
			return fmt.Errorf("measure semantic persistence: %w", err)
		}
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	log.Info("Semantic indexing summary",
		"documents", stats.Documents,
		"vectors", stats.LiveVectors,
		"embedded_this_run", indexed.Load(),
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

func embedSemanticBatch(
	ctx context.Context,
	log *slog.Logger,
	runtimeState *semanticRuntime,
	documents []models.Document,
	previouslyCommitted int64,
	indexed *atomic.Int64,
	startedAt time.Time,
) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan models.Document)
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
					log.Info("Semantic embedding progress",
						"documents", previouslyCommitted+count,
						"embedded_this_run", count,
						"elapsed", time.Since(startedAt),
					)
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
		return fmt.Errorf("embed semantic document after %d new documents: %w", indexed.Load(), firstErr)
	}
	return workerCtx.Err()
}

func semanticDocumentIDs(ctx context.Context, semanticService *service.Service) (map[pkgfts.DocID]struct{}, error) {
	state, err := semanticService.ExportState(ctx)
	if err != nil {
		return nil, err
	}
	documentIDs := make(map[pkgfts.DocID]struct{})
	for _, segment := range state.Segments {
		for ordinal, row := range segment.Rows {
			if livenessAllows(segment.LivenessWords, ordinal) {
				documentIDs[row.Chunk.DocID] = struct{}{}
			}
		}
	}
	return documentIDs, nil
}

func livenessAllows(words []uint64, ordinal int) bool {
	word := ordinal / 64
	return word >= 0 && word < len(words) && words[word]&(uint64(1)<<uint(ordinal%64)) != 0
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
