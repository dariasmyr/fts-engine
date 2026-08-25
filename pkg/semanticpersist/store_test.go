package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	vectorflat "github.com/dariasmyr/fts-engine/pkg/vector/flat"
)

func TestPublishOpenRoundTripBothDurabilityModes(t *testing.T) {
	for _, durability := range []DurabilityMode{DurabilitySynchronous, DurabilityAsynchronous} {
		t.Run(durabilityName(durability), func(t *testing.T) {
			checkpoint, wantChunks, wantDocuments := persistenceFixture(t, false)
			root := t.TempDir()
			generation, err := Publish(context.Background(), root, 42, checkpoint, Options{Durability: durability})
			if err != nil {
				t.Fatal(err)
			}
			if generation.ID != 42 || !validObjectID(generation.ObjectID) {
				t.Fatalf("generation = %+v", generation)
			}
			for _, path := range []string{
				filepath.Join(root, currentFileName),
				filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, vectorsFileName),
				filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, segmentMetaFileName),
				filepath.Join(root, generationsDirectory, generationName(42), manifestFileName),
				filepath.Join(root, generationsDirectory, generationName(42), stateFileName),
			} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("missing %s: %v", path, err)
				}
			}
			loaded, err := Open(root, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer loaded.Close()
			gotChunks, err := loaded.Reader.SearchChunks(context.Background(), []float32{0, 0}, 3)
			if err != nil {
				t.Fatal(err)
			}
			gotDocuments, err := loaded.Reader.SearchDocuments(context.Background(), []float32{0, 0}, 2)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(gotChunks.Hits, wantChunks.Hits) || !equalDocumentHits(gotDocuments.Hits, wantDocuments.Hits) {
				t.Fatalf("round-trip search mismatch\nchunks=%+v\ndocuments=%+v", gotChunks, gotDocuments)
			}
			if loaded.Checkpoint.Space != checkpoint.Space || loaded.Checkpoint.Chunking != checkpoint.Chunking || loaded.Checkpoint.HighWatermark != checkpoint.HighWatermark || !equalDuplicateStatistics(loaded.Checkpoint.DuplicateStatistics, checkpoint.DuplicateStatistics) {
				t.Fatal("checkpoint metadata changed during round trip")
			}
			if !slices.Equal(loaded.Checkpoint.VectorIDs, checkpoint.VectorIDs) ||
				!slices.Equal(loaded.Checkpoint.Live.SnapshotWords(), checkpoint.Live.SnapshotWords()) ||
				!slices.Equal(loaded.Checkpoint.Refs, checkpoint.Refs) ||
				!equalDocumentRecords(loaded.Checkpoint.Documents, checkpoint.Documents) {
				t.Fatal("checkpoint mappings changed during round trip")
			}
		})
	}
}

func TestFailuresBeforeCurrentKeepPreviousGeneration(t *testing.T) {
	steps := []PublicationStep{
		StepWriteVectors, StepWriteSegmentMeta, StepSyncSegment, StepRenameSegment,
		StepWriteState, StepWriteManifest, StepSyncGeneration, StepRenameGeneration,
		StepWriteCurrent, StepReplaceCurrent,
	}
	for _, failedStep := range steps {
		t.Run(string(failedStep), func(t *testing.T) {
			root := t.TempDir()
			first, _, _ := persistenceFixture(t, false)
			second, _, _ := persistenceFixture(t, true)
			if _, err := Publish(context.Background(), root, 1, first, Options{Durability: DurabilitySynchronous}); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected failure")
			_, err := Publish(context.Background(), root, 2, second, Options{
				Durability: DurabilitySynchronous, ExpectedGeneration: 1,
				BeforeStep: func(step PublicationStep) error {
					if step == failedStep {
						return injected
					}
					return nil
				},
			})
			if !errors.Is(err, injected) || errors.Is(err, ErrIndeterminate) {
				t.Fatalf("Publish() error = %v", err)
			}
			loaded, err := Open(root, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer loaded.Close()
			if loaded.Generation.ID != 1 {
				t.Fatalf("opened generation %d, want 1", loaded.Generation.ID)
			}
		})
	}
}

func TestFailureAfterCurrentIsIndeterminateAndPublished(t *testing.T) {
	root := t.TempDir()
	first, _, _ := persistenceFixture(t, false)
	second, _, _ := persistenceFixture(t, true)
	if _, err := Publish(context.Background(), root, 1, first, Options{}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("final sync failed")
	_, err := Publish(context.Background(), root, 2, second, Options{
		Durability: DurabilitySynchronous, ExpectedGeneration: 1,
		BeforeStep: func(step PublicationStep) error {
			if step == StepSyncStore {
				return injected
			}
			return nil
		},
	})
	if !errors.Is(err, ErrIndeterminate) {
		t.Fatalf("Publish() error = %v, want ErrIndeterminate", err)
	}
	loaded, err := Open(root, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	if loaded.Generation.ID != 2 {
		t.Fatalf("opened generation %d, want published generation 2", loaded.Generation.ID)
	}
}

func TestFailuresAfterPreCommitStepsKeepPreviousGeneration(t *testing.T) {
	steps := []PublicationStep{
		StepWriteVectors, StepWriteSegmentMeta, StepSyncSegment, StepRenameSegment,
		StepWriteState, StepWriteManifest, StepSyncGeneration, StepRenameGeneration, StepWriteCurrent,
	}
	for _, failedStep := range steps {
		t.Run(string(failedStep), func(t *testing.T) {
			root := t.TempDir()
			first, _, _ := persistenceFixture(t, false)
			second, _, _ := persistenceFixture(t, true)
			if _, err := Publish(context.Background(), root, 1, first, Options{}); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected after step")
			_, err := Publish(context.Background(), root, 2, second, Options{ExpectedGeneration: 1, AfterStep: func(step PublicationStep) error {
				if step == failedStep {
					return injected
				}
				return nil
			}})
			if !errors.Is(err, injected) || errors.Is(err, ErrIndeterminate) {
				t.Fatalf("Publish() error = %v", err)
			}
			loaded, err := Open(root, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer loaded.Close()
			if loaded.Generation.ID != 1 {
				t.Fatalf("opened generation %d, want 1", loaded.Generation.ID)
			}
		})
	}
}

func TestFailureAfterCommittedStepsIsIndeterminate(t *testing.T) {
	for _, failedStep := range []PublicationStep{StepReplaceCurrent, StepSyncStore} {
		t.Run(string(failedStep), func(t *testing.T) {
			root := t.TempDir()
			first, _, _ := persistenceFixture(t, false)
			second, _, _ := persistenceFixture(t, true)
			if _, err := Publish(context.Background(), root, 1, first, Options{}); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("after committed step")
			_, err := Publish(context.Background(), root, 2, second, Options{ExpectedGeneration: 1, AfterStep: func(step PublicationStep) error {
				if step == failedStep {
					return injected
				}
				return nil
			}})
			if !errors.Is(err, ErrIndeterminate) {
				t.Fatalf("Publish() error = %v", err)
			}
			loaded, err := Open(root, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			defer loaded.Close()
			if loaded.Generation.ID != 2 {
				t.Fatalf("opened generation %d, want 2", loaded.Generation.ID)
			}
		})
	}
}

func TestCurrentRecoveryOrphanAndExplicitRepair(t *testing.T) {
	root := t.TempDir()
	first, _, _ := persistenceFixture(t, false)
	second, _, _ := persistenceFixture(t, true)
	if _, err := Publish(context.Background(), root, 1, first, Options{}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("before current")
	if _, err := Publish(context.Background(), root, 2, second, Options{ExpectedGeneration: 1, BeforeStep: func(step PublicationStep) error {
		if step == StepReplaceCurrent {
			return injected
		}
		return nil
	}}); !errors.Is(err, injected) {
		t.Fatalf("orphan publication error = %v", err)
	}
	loaded, err := Open(root, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Generation.ID != 1 {
		t.Fatalf("orphan generation was promoted: %d", loaded.Generation.ID)
	}
	_ = loaded.Close()
	if err := os.Remove(filepath.Join(root, currentFileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, Limits{}); !errors.Is(err, ErrCurrentMissing) {
		t.Fatalf("missing CURRENT error = %v", err)
	}
	if err := RepairCurrent(root, 2, Options{}); err != nil {
		t.Fatal(err)
	}
	loaded, err = Open(root, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	if loaded.Generation.ID != 2 {
		t.Fatalf("repaired generation = %d, want 2", loaded.Generation.ID)
	}
}

func TestOpenRejectsCorruptCurrentAndSymlinkedObject(t *testing.T) {
	root := t.TempDir()
	checkpoint, _, _ := persistenceFixture(t, false)
	generation, err := Publish(context.Background(), root, 1, checkpoint, Options{})
	if err != nil {
		t.Fatal(err)
	}
	currentPath := filepath.Join(root, currentFileName)
	current, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	current[len(current)/2] ^= 0xff
	if err := os.WriteFile(currentPath, current, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, Limits{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt CURRENT error = %v", err)
	}
	if runtime.GOOS == "windows" {
		return
	}
	if err := RepairCurrent(root, 1, Options{}); err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID)
	realPath := objectPath + "-real"
	if err := os.Rename(objectPath, realPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realPath, objectPath); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, Limits{}); !errors.Is(err, ErrSymlink) {
		t.Fatalf("symlinked object error = %v", err)
	}
}

func TestOpenRejectsCorruptReferencedFiles(t *testing.T) {
	files := []func(root string, generation Generation) string{
		func(root string, generation Generation) string {
			return filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, vectorsFileName)
		},
		func(root string, generation Generation) string {
			return filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, segmentMetaFileName)
		},
		func(root string, _ Generation) string {
			return filepath.Join(root, generationsDirectory, generationName(1), stateFileName)
		},
		func(root string, _ Generation) string {
			return filepath.Join(root, generationsDirectory, generationName(1), manifestFileName)
		},
	}
	for index, filePath := range files {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			root := t.TempDir()
			checkpoint, _, _ := persistenceFixture(t, false)
			generation, err := Publish(context.Background(), root, 1, checkpoint, Options{})
			if err != nil {
				t.Fatal(err)
			}
			path := filePath(root, generation)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)/2] ^= 0xff
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(root, Limits{}); err == nil {
				t.Fatalf("Open() accepted corruption in %s", path)
			}
		})
	}
}

func TestOpenEnforcesAllocationLimits(t *testing.T) {
	root := t.TempDir()
	checkpoint, _, _ := persistenceFixture(t, true)
	if _, err := Publish(context.Background(), root, 1, checkpoint, Options{}); err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.MaxVectors = 1
	if _, err := Open(root, limits); !errors.Is(err, ErrLimitExceeded) && !errors.Is(err, vectorflat.ErrSegmentLimit) {
		t.Fatalf("Open() limit error = %v", err)
	}
	limits = DefaultLimits()
	limits.MaxFileBytes = 512
	limits.MaxVectorBytes = 512
	limits.MaxOpenBytes = 512
	if _, err := Open(root, limits); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("Open() aggregate allocation limit error = %v", err)
	}
}

func TestPublishPreflightsLimitsBeforeWriting(t *testing.T) {
	root := t.TempDir()
	checkpoint, _, _ := persistenceFixture(t, true)
	limits := DefaultLimits()
	limits.MaxVectors = 1
	called := false
	_, err := Publish(context.Background(), root, 1, checkpoint, Options{Limits: limits, BeforeStep: func(PublicationStep) error {
		called = true
		return nil
	}})
	if !errors.Is(err, ErrLimitExceeded) || called {
		t.Fatalf("Publish() error = %v, callback called = %v", err, called)
	}
}

func TestPublishDoesNotCommitGenerationThatExceedsOpenBudget(t *testing.T) {
	root := t.TempDir()
	checkpoint, _, _ := persistenceFixture(t, false)
	limits := DefaultLimits()
	limits.MaxFileBytes = 512
	limits.MaxVectorBytes = 512
	limits.MaxOpenBytes = 512
	if _, err := Publish(context.Background(), root, 1, checkpoint, Options{Limits: limits}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("Publish() aggregate allocation limit error = %v", err)
	}
	if _, err := Open(root, limits); !errors.Is(err, ErrCurrentMissing) {
		t.Fatalf("Open() after rejected publication error = %v", err)
	}
}

func TestOpenRejectsReferencedFileLargerThanDeclaredSize(t *testing.T) {
	root := t.TempDir()
	checkpoint, _, _ := persistenceFixture(t, false)
	if _, err := Publish(context.Background(), root, 1, checkpoint, Options{}); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, generationsDirectory, generationName(1), stateFileName)
	file, err := os.OpenFile(statePath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("unexpected trailing bytes")); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, Limits{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open() mismatched referenced size error = %v", err)
	}
}

func TestPublishRejectsLockedAndStaleWriters(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "freebsd" {
		t.Skip("OS-backed writable locking is not implemented on this platform")
	}
	root := t.TempDir()
	checkpoint, _, _ := persistenceFixture(t, false)
	if _, err := Publish(context.Background(), root, 1, checkpoint, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(context.Background(), root, 2, checkpoint, Options{}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale publication error = %v", err)
	}
	firstReader, err := Open(root, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	secondReader, err := Open(root, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(context.Background(), root, 2, checkpoint, Options{ExpectedGeneration: 1}); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("publication with active readers error = %v", err)
	}
	if err := firstReader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := secondReader.Close(); err != nil {
		t.Fatal(err)
	}
	lock, err := acquireStoreLock(filepath.Join(root, "LOCK"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err := Publish(context.Background(), root, 2, checkpoint, Options{ExpectedGeneration: 1}); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("locked publication error = %v", err)
	}
	if _, err := Open(root, Limits{}); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("read during publication error = %v", err)
	}
}

func TestObjectIDValidation(t *testing.T) {
	invalid := []string{"", ".", "..", "seg-../x", "seg-foo/bar", `seg-foo\bar`, "/absolute", "SEG-" + string(make([]byte, 64))}
	for _, id := range invalid {
		if validObjectID(id) {
			t.Fatalf("validObjectID(%q) = true", id)
		}
	}
}

func persistenceFixture(t testing.TB, extra bool) (semantic.Checkpoint, semantic.ChunkSearchResult, semantic.DocumentSearchResult) {
	t.Helper()
	service, err := semantic.New(semantic.Config{
		Space:    semantic.SpaceDescriptor{ID: "persist-space-v1", Dimensions: 2, Metric: vector.MetricL2Squared, Normalization: vector.NormalizationNone, VectorFormatVersion: 1},
		Chunking: semantic.ChunkingDescriptor{ID: "persist-chunks-v1"}, MaxVectors: 100,
		MaxChunksPerDocument: 10, MaxK: 10, MaxChunkCandidates: 100, MaxChunksPerDocumentHit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	add := func(docID fts.DocID, id chunk.ID, value []float32) {
		t.Helper()
		err := service.AddDocument(ctx, []semantic.ChunkVector{{
			Ref: chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, EndByte: 10}, Vector: value,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	add("doc-a", "a", []float32{0, 0})
	add("doc-b", "b", []float32{1, 0})
	if extra {
		add("doc-c", "c", []float32{2, 0})
	}
	checkpoint, err := service.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := service.SearchChunks(ctx, []float32{0, 0}, min(3, checkpoint.Live.AllowedOrdinalCount()))
	if err != nil {
		t.Fatal(err)
	}
	documents, err := service.SearchDocuments(ctx, []float32{0, 0}, min(2, len(checkpoint.Documents)))
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint, chunks, documents
}

func equalDocumentHits(a, b []semantic.DocumentHit) bool {
	return slices.EqualFunc(a, b, func(a, b semantic.DocumentHit) bool {
		return a.DocID == b.DocID && a.Distance == b.Distance && slices.Equal(a.Chunks, b.Chunks)
	})
}

func equalDocumentRecords(a, b []semantic.DocumentRecord) bool {
	return slices.EqualFunc(a, b, func(a, b semantic.DocumentRecord) bool {
		return a.DocID == b.DocID && slices.Equal(a.VectorIDs, b.VectorIDs)
	})
}

func equalDuplicateStatistics(a, b *semantic.DuplicateStatistics) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func durabilityName(mode DurabilityMode) string {
	if mode == DurabilitySynchronous {
		return "sync"
	}
	return "async"
}
