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
	"github.com/dariasmyr/fts-engine/pkg/vector/hnsw"
)

type staticEncoder struct {
	vectors    map[fts.DocID][]semantic.ChunkVector
	descriptor semantic.PipelineDescriptor
}

func testEmbeddingDescriptor(model, fingerprint string) semantic.EmbeddingDescriptor {
	descriptor, err := semantic.NewEmbeddingDescriptor("test-provider", model, "v1", fingerprint, 2, vector.MetricL2Squared, 1)
	if err != nil {
		panic(err)
	}
	return descriptor
}

func (e staticEncoder) Encode(_ context.Context, document semantic.Document) ([]semantic.ChunkVector, error) {
	return e.vectors[document.ID], nil
}

func (e staticEncoder) Descriptor() semantic.PipelineDescriptor {
	if e.descriptor != (semantic.PipelineDescriptor{}) {
		return e.descriptor
	}
	return semantic.PipelineDescriptor{
		Embedding: testEmbeddingDescriptor("test-model", "test-embedding"),
		Chunking:  semantic.ChunkingDescriptor{ID: "test-chunks", Version: 1, Fingerprint: "test-chunks-fp"},
	}
}

func zeroQueryEncoder() staticEncoder {
	return staticEncoder{vectors: map[fts.DocID][]semantic.ChunkVector{
		"query": {{Ref: chunk.Ref{ID: "query", DocID: "query", Field: fts.DefaultField, EndByte: 5}, Vector: []float32{0, 0}}},
	}}
}

func TestPublishOpenRoundTripBothDurabilityModes(t *testing.T) {
	for _, durability := range []DurabilityMode{DurabilitySynchronous, DurabilityAsynchronous} {
		t.Run(durabilityName(durability), func(t *testing.T) {
			checkpoint, _, wantDocuments := persistenceFixture(t, false)
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
				filepath.Join(root, generationsDirectory, generationName(42), manifestFileName),
				filepath.Join(root, generationsDirectory, generationName(42), stateFileName),
			} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("missing %s: %v", path, err)
				}
			}
			loaded, err := Open(root, OpenOptions{Limits: Limits{}})
			if err != nil {
				t.Fatal(err)
			}
			defer loaded.Close()
			view, err := semantic.NewReadView(loaded.Generation.ID, []*semantic.Segment{loaded.Sealed.Segment}, semantic.SearchPolicy{MaxK: 10, MaxChunkCandidates: 20, MaxChunksPerDocumentHit: 3})
			if err != nil {
				t.Fatal(err)
			}
			gotDocuments, err := view.SearchDocuments(context.Background(), zeroQueryEncoder(), semantic.Document{ID: "query"}, 2)
			if err != nil {
				t.Fatal(err)
			}
			if !equalDocumentHits(gotDocuments.Hits, wantDocuments.Hits) {
				t.Fatalf("round-trip document search mismatch\ndocuments=%+v", gotDocuments)
			}
			if loaded.Sealed.Segment.Metadata() != checkpoint.Segment.Metadata() || loaded.Sealed.MaxAllocatedVectorID != checkpoint.MaxAllocatedVectorID {
				t.Fatal("checkpoint metadata changed during round trip")
			}
			if !slices.Equal(loaded.Sealed.Segment.Rows(), checkpoint.Segment.Rows()) {
				t.Fatal("checkpoint mappings changed during round trip")
			}
		})
	}
}

func TestPublishReusesSegmentObjectAndUpgradesDurability(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, false)
	root := t.TempDir()
	first, err := Publish(context.Background(), root, 1, checkpoint, Options{Durability: DurabilityAsynchronous})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.MaxAllocatedVectorID++
	second, err := Publish(context.Background(), root, 2, checkpoint, Options{
		Durability: DurabilitySynchronous, ExpectedGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.ObjectID != first.ObjectID {
		t.Fatalf("segment object was not reused: first %q, second %q", first.ObjectID, second.ObjectID)
	}
	loaded, err := Open(root, OpenOptions{Limits: Limits{}})
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	if loaded.Generation.ID != 2 || loaded.Sealed.MaxAllocatedVectorID != checkpoint.MaxAllocatedVectorID {
		t.Fatalf("opened generation/sealed = %d/%d", loaded.Generation.ID, loaded.Sealed.MaxAllocatedVectorID)
	}
}

func TestPublishOpenChunkHNSWRoundTrip(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, true)
	checkpoint = withHNSW(t, checkpoint, 7)
	root := t.TempDir()
	generation, err := Publish(context.Background(), root, 1, checkpoint, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, graphFileName)); err != nil {
		t.Fatalf("graph was not published: %v", err)
	}
	loaded, err := Open(root, OpenOptions{Limits: Limits{}})
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	if loaded.Sealed.Segment.Kind() != semantic.SegmentKindChunkHNSW || loaded.Sealed.Segment.Searcher() == nil {
		t.Fatalf("opened segment = kind %d, searcher %p", loaded.Sealed.Segment.Kind(), loaded.Sealed.Segment.Searcher())
	}
	view, err := semantic.NewReadView(loaded.Generation.ID, []*semantic.Segment{loaded.Sealed.Segment}, semantic.SearchPolicy{MaxK: 10, MaxChunkCandidates: 20, MaxChunksPerDocumentHit: 3})
	if err != nil {
		t.Fatal(err)
	}
	result, err := view.SearchDocuments(context.Background(), zeroQueryEncoder(), semantic.Document{ID: "query"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.Hits[0].DocID != "doc-a" {
		t.Fatalf("round-trip HNSW result = %+v", result)
	}
}

func TestOpenRejectsMissingCorruptAndSubstitutedGraph(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string, Generation, SealedSegment)
	}{
		{name: "missing", mutate: func(t *testing.T, root string, generation Generation, _ SealedSegment) {
			t.Helper()
			if err := os.Remove(filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, graphFileName)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "corrupt", mutate: func(t *testing.T, root string, generation Generation, _ SealedSegment) {
			t.Helper()
			path := filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, graphFileName)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)/2] ^= 0xff
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "substituted_identity", mutate: substituteGraphAndReferences},
	} {
		t.Run(test.name, func(t *testing.T) {
			checkpoint, _, _ := persistenceFixture(t, true)
			checkpoint = withHNSW(t, checkpoint, 7)
			root := t.TempDir()
			generation, err := Publish(context.Background(), root, 1, checkpoint, Options{})
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, root, generation, checkpoint)
			if _, err := Open(root, OpenOptions{Limits: Limits{}}); err == nil {
				t.Fatal("Open accepted invalid graph publication")
			}
		})
	}
}

func TestGraphPublicationFailureKeepsPreviousGeneration(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[after], func(t *testing.T) {
			root := t.TempDir()
			first, _, _ := persistenceFixture(t, false)
			if _, err := Publish(context.Background(), root, 1, first, Options{}); err != nil {
				t.Fatal(err)
			}
			second, _, _ := persistenceFixture(t, true)
			second = withHNSW(t, second, 9)
			injected := errors.New("graph publication failure")
			options := Options{ExpectedGeneration: 1}
			if after {
				options.AfterStep = func(step PublicationStep) error {
					if step == StepWriteGraph {
						return injected
					}
					return nil
				}
			} else {
				options.BeforeStep = func(step PublicationStep) error {
					if step == StepWriteGraph {
						return injected
					}
					return nil
				}
			}
			if _, err := Publish(context.Background(), root, 2, second, options); !errors.Is(err, injected) {
				t.Fatalf("Publish error = %v", err)
			}
			loaded, err := Open(root, OpenOptions{Limits: Limits{}})
			if err != nil {
				t.Fatal(err)
			}
			defer loaded.Close()
			if loaded.Generation.ID != 1 {
				t.Fatalf("opened generation %d", loaded.Generation.ID)
			}
		})
	}
}

func TestPublishPreflightsGraphLimit(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, true)
	checkpoint = withHNSW(t, checkpoint, 7)
	limits := DefaultLimits()
	limits.MaxGraphBytes = 100
	called := false
	_, err := Publish(context.Background(), t.TempDir(), 1, checkpoint, Options{Limits: limits, BeforeStep: func(PublicationStep) error {
		called = true
		return nil
	}})
	if !errors.Is(err, ErrLimitExceeded) || called {
		t.Fatalf("Publish error/callback = %v/%v", err, called)
	}
}

func TestFailuresBeforeCurrentKeepPreviousGeneration(t *testing.T) {
	steps := []PublicationStep{
		StepWriteVectors, StepSyncSegment, StepRenameSegment,
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
			loaded, err := Open(root, OpenOptions{Limits: Limits{}})
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
	loaded, err := Open(root, OpenOptions{Limits: Limits{}})
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
		StepWriteVectors, StepSyncSegment, StepRenameSegment,
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
			loaded, err := Open(root, OpenOptions{Limits: Limits{}})
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
			loaded, err := Open(root, OpenOptions{Limits: Limits{}})
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

func TestCancellationAfterStepStopsBeforeNextPublicationStage(t *testing.T) {
	root := t.TempDir()
	checkpoint, _, _ := persistenceFixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	_, err := Publish(ctx, root, 1, checkpoint, Options{AfterStep: func(step PublicationStep) error {
		if step == StepWriteVectors {
			cancel()
		}
		return nil
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context cancellation", err)
	}
	if _, err := Open(root, OpenOptions{Limits: Limits{}}); !errors.Is(err, ErrCurrentMissing) {
		t.Fatalf("Open() error = %v, want missing CURRENT", err)
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
	loaded, err := Open(root, OpenOptions{Limits: Limits{}})
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
	if _, err := Open(root, OpenOptions{Limits: Limits{}}); !errors.Is(err, ErrCurrentMissing) {
		t.Fatalf("missing CURRENT error = %v", err)
	}
	if err := RepairCurrent(root, 2, Options{}); err != nil {
		t.Fatal(err)
	}
	loaded, err = Open(root, OpenOptions{Limits: Limits{}})
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
	if _, err := Open(root, OpenOptions{Limits: Limits{}}); !errors.Is(err, ErrCorrupt) {
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
	if _, err := Open(root, OpenOptions{Limits: Limits{}}); !errors.Is(err, ErrSymlink) {
		t.Fatalf("symlinked object error = %v", err)
	}
}

func TestOpenRejectsCorruptReferencedFiles(t *testing.T) {
	files := []func(root string, generation Generation) string{
		func(root string, generation Generation) string {
			return filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, vectorsFileName)
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
			if _, err := Open(root, OpenOptions{Limits: Limits{}}); err == nil {
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
	if _, err := Open(root, OpenOptions{Limits: limits}); !errors.Is(err, ErrLimitExceeded) && !errors.Is(err, ErrSegmentLimit) {
		t.Fatalf("Open() limit error = %v", err)
	}
	limits = DefaultLimits()
	limits.MaxFileBytes = 512
	limits.MaxVectorBytes = 512
	limits.MaxOpenBytes = 512
	if _, err := Open(root, OpenOptions{Limits: limits}); !errors.Is(err, ErrLimitExceeded) {
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

func TestHNSWSearchWorkLimits(t *testing.T) {
	checkpoint, _, _ := persistenceFixture(t, false)
	checkpoint = withHNSW(t, checkpoint, 7)

	for name, limit := range map[string]func(*Limits){
		"ef search": func(limits *Limits) {
			limits.MaxEfSearch = checkpoint.Segment.Searcher().SearchConfig().MaxEfSearch - 1
		},
		"visit limit": func(limits *Limits) {
			limits.MaxVisitLimit = checkpoint.Segment.Searcher().SearchConfig().MaxVisitLimit - 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			limits := DefaultLimits()
			limit(&limits)
			if _, err := Publish(context.Background(), t.TempDir(), 1, checkpoint, Options{Limits: limits}); !errors.Is(err, ErrLimitExceeded) {
				t.Fatalf("Publish() work limit error = %v", err)
			}
		})
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
	if _, err := Open(root, OpenOptions{Limits: limits}); !errors.Is(err, ErrCurrentMissing) {
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
	if _, err := Open(root, OpenOptions{Limits: Limits{}}); !errors.Is(err, ErrCorrupt) {
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
	firstReader, err := Open(root, OpenOptions{Limits: Limits{}})
	if err != nil {
		t.Fatal(err)
	}
	secondReader, err := Open(root, OpenOptions{Limits: Limits{}})
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
	if _, err := Open(root, OpenOptions{Limits: Limits{}}); !errors.Is(err, ErrStoreLocked) {
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

func TestPublicationContainsOnlyLiveRows(t *testing.T) {
	service, err := semantic.New(semantic.Config{
		Embedding: testEmbeddingDescriptor("compact-model", "compact-embedding"),
		Chunking:  semantic.ChunkingDescriptor{ID: "compact-chunks-v1", Version: 1, Fingerprint: "compact-chunks-fp"}, MaxVectors: 10,
		MaxChunksPerDocument: 2, MaxK: 2, MaxChunkCandidates: 10, MaxChunksPerDocumentHit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	encoder := staticEncoder{vectors: map[fts.DocID][]semantic.ChunkVector{
		"doc": {{Ref: chunk.Ref{ID: "old", DocID: "doc", Field: fts.DefaultField, EndByte: 3}, Vector: []float32{0, 0}}},
	}, descriptor: semantic.PipelineDescriptor{Embedding: testEmbeddingDescriptor("compact-model", "compact-embedding"), Chunking: semantic.ChunkingDescriptor{ID: "compact-chunks-v1", Version: 1, Fingerprint: "compact-chunks-fp"}}}
	if err := service.AddDocument(ctx, encoder, semantic.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	encoder.vectors["doc"] = []semantic.ChunkVector{{Ref: chunk.Ref{ID: "new", DocID: "doc", Field: fts.DefaultField, EndByte: 3}, Vector: []float32{1, 0}}}
	if err := service.ReplaceDocument(ctx, encoder, semantic.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	view, err := service.ReadView(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segment := view.Segments()[0]
	sealed := SealedSegment{Segment: segment, MaxAllocatedVectorID: service.Statistics().MaxAllocatedVectorID, MaxK: 2, MaxChunkCandidates: 10, MaxChunksPerDocumentHit: 2}
	if segment.Len() != 1 || len(segment.Rows()) != 1 || segment.Rows()[0].Chunk.ID != "new" {
		t.Fatalf("sealed segment retained stale rows: %+v", sealed)
	}
	root := t.TempDir()
	_, err = Publish(ctx, root, 1, sealed, Options{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Open(root, OpenOptions{Limits: Limits{}})
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	if loaded.Generation.ID != 1 || loaded.Sealed.Segment.Len() != 1 || len(loaded.Sealed.Segment.Rows()) != 1 {
		t.Fatalf("opened dense generation = %+v", loaded.Sealed)
	}
	openedView, err := semantic.NewReadView(1, []*semantic.Segment{loaded.Sealed.Segment}, semantic.SearchPolicy{MaxK: 2, MaxChunkCandidates: 10, MaxChunksPerDocumentHit: 2})
	if err != nil {
		t.Fatal(err)
	}
	result, err := openedView.SearchDocuments(ctx, zeroQueryEncoder(), semantic.Document{ID: "query"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Chunks[0].Ref.ID != "new" || result.Stats.RejectedNodes != 0 {
		t.Fatalf("compacted result = %+v", result)
	}
}

type chunkExpectation struct {
	Hits []semantic.ChunkHit
}

func persistenceFixture(t testing.TB, extra bool) (SealedSegment, chunkExpectation, semantic.DocumentSearchResult) {
	t.Helper()
	config := semantic.Config{
		Embedding: testEmbeddingDescriptor("persist-model", "persist-embedding"),
		Chunking:  semantic.ChunkingDescriptor{ID: "persist-chunks-v1", Version: 1, Fingerprint: "persist-chunks-fp"}, MaxVectors: 100,
		MaxChunksPerDocument: 10, MaxK: 10, MaxChunkCandidates: 100, MaxChunksPerDocumentHit: 3,
	}
	service, err := semantic.New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	encoder := staticEncoder{vectors: make(map[fts.DocID][]semantic.ChunkVector), descriptor: semantic.PipelineDescriptor{Embedding: testEmbeddingDescriptor("persist-model", "persist-embedding"), Chunking: semantic.ChunkingDescriptor{ID: "persist-chunks-v1", Version: 1, Fingerprint: "persist-chunks-fp"}}}
	add := func(docID fts.DocID, id chunk.ID, value []float32) {
		t.Helper()
		encoder.vectors[docID] = []semantic.ChunkVector{{
			Ref: chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, EndByte: 10}, Vector: value,
		}}
		err := service.AddDocument(ctx, encoder, semantic.Document{ID: docID})
		if err != nil {
			t.Fatal(err)
		}
	}
	add("doc-a", "a", []float32{0, 0})
	add("doc-b", "b", []float32{1, 0})
	if extra {
		add("doc-c", "c", []float32{2, 0})
	}
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	view, err := service.ReadView(ctx)
	if err != nil {
		t.Fatal(err)
	}
	segment := view.Segments()[0]
	stats := service.Statistics()
	checkpoint := SealedSegment{Segment: segment, MaxAllocatedVectorID: stats.MaxAllocatedVectorID, MaxK: config.MaxK, MaxChunkCandidates: config.MaxChunkCandidates, MaxChunksPerDocumentHit: config.MaxChunksPerDocumentHit}
	encoder.vectors["query"] = []semantic.ChunkVector{{Ref: chunk.Ref{ID: "query", DocID: "query", Field: fts.DefaultField, EndByte: 5}, Vector: []float32{0, 0}}}
	allDocuments, err := service.SearchDocuments(ctx, encoder, semantic.Document{ID: "query"}, min(3, len(checkpoint.Segment.Rows())))
	if err != nil {
		t.Fatal(err)
	}
	chunks := chunkExpectation{}
	for _, document := range allDocuments.Hits {
		chunks.Hits = append(chunks.Hits, document.Chunks...)
	}
	documents, err := service.SearchDocuments(ctx, encoder, semantic.Document{ID: "query"}, min(2, len(checkpoint.Segment.Rows())))
	if err != nil {
		t.Fatal(err)
	}
	return checkpoint, chunks, documents
}

func withHNSW(t testing.TB, checkpoint SealedSegment, seed uint64) SealedSegment {
	t.Helper()
	maxK := max(checkpoint.MaxK, checkpoint.MaxChunkCandidates)
	graph, err := hnsw.BuildSearcher(context.Background(), checkpoint.Segment.Vectors(), hnsw.BuildOptions{
		BuildConfig: hnsw.BuildConfig{
			Dimensions: checkpoint.Segment.Metadata().Embedding.Dimensions, Metric: checkpoint.Segment.Metadata().Embedding.Metric,
			MaxVectors: checkpoint.Segment.Len(), MaxVectorBytes: uint64(checkpoint.Segment.Len() * checkpoint.Segment.Metadata().Embedding.Dimensions * 4),
			MaxNeighbors: 2, EfConstruction: 8, Seed: seed,
		},
		SearchConfig: hnsw.SearchConfig{
			DefaultEfSearch: maxK, MaxEfSearch: maxK, DefaultVisitLimit: checkpoint.Segment.Len(),
			MaxVisitLimit: checkpoint.Segment.Len(), MaxK: maxK,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	segment, err := semantic.NewSegment(semantic.MutableHeadID, checkpoint.Segment.Metadata(), graph, checkpoint.Segment.Rows())
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.Segment = segment
	return checkpoint
}

func substituteGraphAndReferences(t *testing.T, root string, generation Generation, checkpoint SealedSegment) {
	t.Helper()
	generationPath := filepath.Join(root, generationsDirectory, generationName(generation.ID))
	manifestPath := filepath.Join(generationPath, manifestFileName)
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	value, err := decodeManifest(manifestData, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	replacement := withHNSW(t, checkpoint, 99)
	graphData, _, err := hnsw.MarshalGraph(replacement.Segment.Searcher(), hnsw.VectorFileReference{Size: value.Vectors.Size, SHA256: value.Vectors.SHA256})
	if err != nil {
		t.Fatal(err)
	}
	graphPath := filepath.Join(root, objectsDirectory, segmentsDirectory, generation.ObjectID, graphFileName)
	if err := os.WriteFile(graphPath, graphData, 0o600); err != nil {
		t.Fatal(err)
	}
	value.Graph = fileRef(graphData)
	updatedManifest, updatedRef, err := encodeManifest(value, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, updatedManifest, 0o600); err != nil {
		t.Fatal(err)
	}
	current, _, err := encodeCurrent(currentRecord{GenerationID: generation.ID, ManifestHash: updatedRef.SHA256}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, currentFileName), current, 0o600); err != nil {
		t.Fatal(err)
	}
}

func equalDocumentHits(a, b []semantic.DocumentHit) bool {
	return slices.EqualFunc(a, b, func(a, b semantic.DocumentHit) bool {
		return a.DocID == b.DocID && a.Distance == b.Distance && slices.Equal(a.Chunks, b.Chunks)
	})
}

func durabilityName(mode DurabilityMode) string {
	if mode == DurabilitySynchronous {
		return "sync"
	}
	return "async"
}
