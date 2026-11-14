package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
)

type semanticCompactionMetrics struct {
	duration    time.Duration
	allocated   uint64
	mallocs     uint64
	frees       uint64
	heapBefore  uint64
	heapAfter   uint64
	postGCHeap  uint64
	gcCycles    uint32
	gcPause     time.Duration
	statsBefore semantic.Statistics
	statsAfter  semantic.Statistics
}

type semanticCompactionReport struct {
	skipped        bool
	compact        semanticCompactionMetrics
	saveDuration   time.Duration
	verifyDuration time.Duration
	pruneDuration  time.Duration
	totalDuration  time.Duration
	bytesBefore    int64
	bytesAfter     int64
	generation     semanticpersist.Generation
	pruneResult    semanticpersist.PruneResult
}

func runSemanticCompaction(
	ctx context.Context,
	log *slog.Logger,
	runtimeState *semanticRuntime,
) (semanticCompactionReport, error) {
	if runtimeState == nil || !runtimeState.compaction.Enabled {
		return semanticCompactionReport{}, nil
	}
	if runtimeState.service == nil {
		return semanticCompactionReport{}, errors.New("semantic compaction requires an initialized index")
	}

	stats := runtimeState.service.Statistics()
	report := semanticCompactionReport{generation: runtimeState.generation}
	if semanticIndexIsCompacted(stats) {
		report.skipped = true
		log.Info("Skipping semantic compaction: index is already compacted",
			"segments", stats.Segments,
			"live_vectors", stats.LiveVectors,
			"stale_vectors", stats.StaleVectors,
			"generation", runtimeState.generation.ID,
		)
		if !runtimeState.compaction.Prune {
			return report, nil
		}
		startedAt := time.Now()
		var err error
		report.bytesBefore, err = semanticPersistenceSize(runtimeState.persistencePath)
		if err != nil {
			return report, fmt.Errorf("measure semantic persistence before prune: %w", err)
		}
		if err := verifyAndPruneSemanticGenerations(ctx, runtimeState, stats, &report); err != nil {
			return report, err
		}
		report.bytesAfter, err = semanticPersistenceSize(runtimeState.persistencePath)
		if err != nil {
			return report, fmt.Errorf("measure semantic persistence after prune: %w", err)
		}
		report.totalDuration = time.Since(startedAt)
		log.Info("Semantic persistence pruning completed",
			"generation", report.generation.ID,
			"verify_duration", report.verifyDuration,
			"prune_duration", report.pruneDuration,
			"removed_generations", report.pruneResult.RemovedGenerations,
			"removed_segment_objects", report.pruneResult.RemovedSegmentObjects,
			"persisted_bytes_before", report.bytesBefore,
			"persisted_bytes_after", report.bytesAfter,
		)
		return report, nil
	}

	var err error
	report.bytesBefore, err = semanticPersistenceSize(runtimeState.persistencePath)
	if err != nil {
		return report, fmt.Errorf("measure semantic persistence before compaction: %w", err)
	}

	log.Info("Starting semantic compaction",
		"segments", stats.Segments,
		"live_vectors", stats.LiveVectors,
		"physical_vectors", stats.PhysicalVectors,
		"dimensions", runtimeState.service.IndexConfig().Dimensions,
	)
	stopHeartbeat := startSemanticCompactionHeartbeat(log)
	startedAt := time.Now()
	metrics, err := measureSemanticCompaction(
		ctx,
		runtimeState.service.Compact,
		runtimeState.service.Statistics,
		runtimeState.compaction.ProfilePath,
	)
	stopHeartbeat()
	report.compact = metrics
	if err != nil {
		return report, fmt.Errorf("compact semantic index: %w", err)
	}
	log.Info("Semantic compaction completed",
		"duration", metrics.duration,
		"segments_before", metrics.statsBefore.Segments,
		"segments_after", metrics.statsAfter.Segments,
		"allocated_bytes", metrics.allocated,
		"post_gc_heap_bytes", metrics.postGCHeap,
		"gc_cycles", metrics.gcCycles,
	)

	saveStartedAt := time.Now()
	generation, saveErr := semanticpersist.SaveService(
		ctx,
		runtimeState.persistencePath,
		runtimeState.service,
		semanticpersist.SaveOptions{
			ExpectedGeneration: &runtimeState.generation,
			Limits:             runtimeState.persistenceLimits,
		},
	)
	report.saveDuration = time.Since(saveStartedAt)
	if saveErr != nil {
		return report, fmt.Errorf("save compacted semantic index: %w", saveErr)
	}
	runtimeState.generation = generation
	report.generation = generation

	if runtimeState.compaction.Prune {
		if err := verifyAndPruneSemanticGenerations(ctx, runtimeState, metrics.statsAfter, &report); err != nil {
			return report, err
		}
	}

	report.bytesAfter, err = semanticPersistenceSize(runtimeState.persistencePath)
	if err != nil {
		return report, fmt.Errorf("measure semantic persistence after compaction: %w", err)
	}
	report.totalDuration = time.Since(startedAt)
	logSemanticCompactionReport(log, runtimeState, report)
	return report, nil
}

func verifyAndPruneSemanticGenerations(
	ctx context.Context,
	runtimeState *semanticRuntime,
	wantStats semantic.Statistics,
	report *semanticCompactionReport,
) error {
	verifyStartedAt := time.Now()
	verifyErr := verifySemanticGeneration(
		ctx,
		runtimeState.persistencePath,
		runtimeState.generation,
		wantStats,
		runtimeState.persistenceLimits,
	)
	report.verifyDuration = time.Since(verifyStartedAt)
	if verifyErr != nil {
		return fmt.Errorf("verify compacted semantic generation before prune: %w", verifyErr)
	}

	pruneStartedAt := time.Now()
	pruneResult, pruneErr := semanticpersist.Prune(
		ctx,
		runtimeState.persistencePath,
		semanticpersist.PruneOptions{
			RetainGenerations: 1,
			Limits:            runtimeState.persistenceLimits,
		},
	)
	report.pruneDuration = time.Since(pruneStartedAt)
	report.pruneResult = pruneResult
	if pruneErr != nil {
		return fmt.Errorf("prune compacted semantic index: %w", pruneErr)
	}
	return nil
}

func verifySemanticGeneration(
	ctx context.Context,
	path string,
	wantGeneration semanticpersist.Generation,
	wantStats semantic.Statistics,
	limits semanticpersist.Limits,
) error {
	index, generation, err := semanticpersist.Load(ctx, path, semanticpersist.LoadOptions{Limits: limits})
	if err != nil {
		return err
	}
	if generation != wantGeneration {
		return fmt.Errorf("loaded generation %d, want %d", generation.ID, wantGeneration.ID)
	}
	if stats := index.Statistics(); stats != wantStats {
		return fmt.Errorf("loaded statistics %+v, want %+v", stats, wantStats)
	}
	return nil
}

func semanticIndexIsCompacted(stats semantic.Statistics) bool {
	return stats.Segments <= 1 && stats.StaleVectors == 0
}

func semanticPersistenceSize(path string) (int64, error) {
	if path == "" {
		return 0, nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return directorySize(path)
}

func startSemanticCompactionHeartbeat(log *slog.Logger) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	startedAt := time.Now()
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				log.Info("Semantic compaction in progress", "elapsed", time.Since(startedAt))
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func measureSemanticCompaction(
	ctx context.Context,
	compact func(context.Context) error,
	statistics func() semantic.Statistics,
	profilePath string,
) (semanticCompactionMetrics, error) {
	metrics := semanticCompactionMetrics{statsBefore: statistics()}
	if profilePath != "" {
		if err := os.MkdirAll(profilePath, 0o755); err != nil {
			return metrics, fmt.Errorf("create compaction profile directory: %w", err)
		}
	}

	runtime.GC()
	stopCPUProfile, err := startCPUProfile(profilePath)
	if err != nil {
		return metrics, err
	}
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	startedAt := time.Now()
	compactErr := compact(ctx)
	metrics.duration = time.Since(startedAt)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	stopProfileErr := stopCPUProfile()

	metrics.statsAfter = statistics()
	metrics.allocated = after.TotalAlloc - before.TotalAlloc
	metrics.mallocs = after.Mallocs - before.Mallocs
	metrics.frees = after.Frees - before.Frees
	metrics.heapBefore = before.HeapAlloc
	metrics.heapAfter = after.HeapAlloc
	metrics.gcCycles = after.NumGC - before.NumGC
	metrics.gcPause = time.Duration(after.PauseTotalNs - before.PauseTotalNs)

	runtime.GC()
	var postGC runtime.MemStats
	runtime.ReadMemStats(&postGC)
	metrics.postGCHeap = postGC.HeapAlloc
	if err := writeMemoryProfiles(profilePath); err != nil {
		return metrics, errors.Join(stopProfileErr, err)
	}
	return metrics, errors.Join(compactErr, stopProfileErr)
}

func startCPUProfile(profilePath string) (func() error, error) {
	if profilePath == "" {
		return func() error { return nil }, nil
	}
	file, err := os.Create(filepath.Join(profilePath, "cpu.pprof"))
	if err != nil {
		return nil, fmt.Errorf("create compaction CPU profile: %w", err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		return nil, fmt.Errorf("start compaction CPU profile: %w", errors.Join(err, file.Close()))
	}
	return func() error {
		pprof.StopCPUProfile()
		if err := file.Close(); err != nil {
			return fmt.Errorf("close compaction CPU profile: %w", err)
		}
		return nil
	}, nil
}

func writeMemoryProfiles(profilePath string) error {
	if profilePath == "" {
		return nil
	}
	if err := writeProfile(filepath.Join(profilePath, "allocs.pprof"), pprof.Lookup("allocs")); err != nil {
		return err
	}
	return writeProfile(filepath.Join(profilePath, "heap.pprof"), pprof.Lookup("heap"))
}

func writeProfile(path string, profile *pprof.Profile) error {
	if profile == nil {
		return fmt.Errorf("runtime profile for %q is unavailable", filepath.Base(path))
	}
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create runtime profile %q: %w", path, err)
	}
	writeErr := profile.WriteTo(file, 0)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return fmt.Errorf("write runtime profile %q: %w", path, err)
	}
	return nil
}

func logSemanticCompactionReport(
	log *slog.Logger,
	runtimeState *semanticRuntime,
	report semanticCompactionReport,
) {
	metrics := report.compact
	log.Info("Semantic compaction summary",
		"path", runtimeState.persistencePath,
		"compaction_duration", metrics.duration,
		"save_duration", report.saveDuration,
		"verify_duration", report.verifyDuration,
		"prune_duration", report.pruneDuration,
		"total_duration", report.totalDuration,
		"allocated_bytes", metrics.allocated,
		"mallocs", metrics.mallocs,
		"frees", metrics.frees,
		"heap_before_bytes", metrics.heapBefore,
		"heap_after_bytes", metrics.heapAfter,
		"post_gc_heap_bytes", metrics.postGCHeap,
		"gc_cycles", metrics.gcCycles,
		"gc_pause", metrics.gcPause,
		"segments_before", metrics.statsBefore.Segments,
		"segments_after", metrics.statsAfter.Segments,
		"physical_vectors_before", metrics.statsBefore.PhysicalVectors,
		"physical_vectors_after", metrics.statsAfter.PhysicalVectors,
		"live_vectors", metrics.statsAfter.LiveVectors,
		"stale_vectors_before", metrics.statsBefore.StaleVectors,
		"stale_vectors_after", metrics.statsAfter.StaleVectors,
		"persisted_bytes_before", report.bytesBefore,
		"persisted_bytes_after", report.bytesAfter,
		"generation", report.generation.ID,
		"removed_generations", report.pruneResult.RemovedGenerations,
		"removed_segment_objects", report.pruneResult.RemovedSegmentObjects,
		"profile_path", runtimeState.compaction.ProfilePath,
	)
}
