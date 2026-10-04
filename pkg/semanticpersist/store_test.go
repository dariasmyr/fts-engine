package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type staticEncoder struct {
	descriptor semantic.PipelineDescriptor
	vectors    map[fts.DocID][]semantic.EncodedChunk
}

func (e staticEncoder) Descriptor() semantic.PipelineDescriptor { return e.descriptor }
func (e staticEncoder) Encode(_ context.Context, document fts.Document) ([]semantic.EncodedChunk, error) {
	return e.vectors[document.ID], nil
}

func TestPublishRejectsPendingMutationsWithoutCURRENT(t *testing.T) {
	service, encoder := persistenceService(t)
	if err := service.AddDocument(context.Background(), encoder, fts.Document{ID: "doc-a"}); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if _, err := Publish(context.Background(), root, service, Options{}); !errors.Is(err, semantic.ErrPendingMutations) {
		t.Fatalf("Publish error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, currentFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("CURRENT stat error = %v", err)
	}
}

func TestPublishOpenEmptyService(t *testing.T) {
	service, _ := persistenceService(t)
	root := t.TempDir()
	generation, err := Publish(t.Context(), root, service, Options{Durability: DurabilityAsynchronous})
	if err != nil {
		t.Fatal(err)
	}
	if generation.ID != 1 {
		t.Fatalf("empty generation = %+v", generation)
	}
	store, err := Open(t.Context(), root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if got := store.Service().Statistics(); got.Documents != 0 || got.LiveVectors != 0 {
		t.Fatalf("empty statistics = %+v", got)
	}
}

func TestWritableStoreRestartLifecycle(t *testing.T) {
	ctx := context.Background()
	service, encoder := persistenceService(t)
	encoder.vectors["query"] = []semantic.EncodedChunk{testVector("query", "query-1", []float32{0, 0})}
	encoder.vectors["doc-a"] = []semantic.EncodedChunk{testVector("doc-a", "a-old", []float32{1, 0})}
	encoder.vectors["doc-delete"] = []semantic.EncodedChunk{testVector("doc-delete", "delete-1", []float32{4, 0})}
	encoder.vectors["doc-added"] = []semantic.EncodedChunk{testVector("doc-added", "added-1", []float32{2, 0})}
	encoder.vectors["doc-after-restart"] = []semantic.EncodedChunk{testVector("doc-after-restart", "after-1", []float32{3, 0})}

	addAndFlush(t, service, encoder, "doc-a")
	addAndFlush(t, service, encoder, "doc-delete")
	root := t.TempDir()
	if generation, err := Publish(ctx, root, service, Options{Durability: DurabilityAsynchronous}); err != nil || generation.ID != 1 {
		t.Fatalf("initial Publish = %+v, %v", generation, err)
	}

	store, err := Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertSearchVisibility(t, store.Service(), encoder, map[fts.DocID]chunk.ID{
		"doc-a":      "a-old",
		"doc-delete": "delete-1",
	})
	if got := store.Service().Statistics().MaxAllocatedVectorID; got != 2 {
		t.Fatalf("initial max allocated vector ID = %d, want 2", got)
	}

	addAndFlush(t, store.Service(), encoder, "doc-added")
	encoder.vectors["doc-a"] = []semantic.EncodedChunk{testVector("doc-a", "a-new", []float32{1, 1})}
	if err := store.Service().ReplaceDocument(ctx, encoder, fts.Document{ID: "doc-a"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Service().DeleteDocument(ctx, "doc-delete"); err != nil {
		t.Fatal(err)
	}
	if err := store.Service().Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Service().Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if got := store.Service().Statistics(); got.Documents != 2 || got.PhysicalVectors != 2 || got.LiveVectors != 2 || got.StaleVectors != 0 || got.MaxAllocatedVectorID != 4 {
		t.Fatalf("compacted statistics = %+v", got)
	}
	assertSearchVisibility(t, store.Service(), encoder, map[fts.DocID]chunk.ID{
		"doc-a":     "a-new",
		"doc-added": "added-1",
	})
	if generation, err := store.Publish(ctx, Options{Durability: DurabilityAsynchronous}); err != nil || generation.ID != 2 {
		t.Fatalf("Store.Publish = %+v, %v", generation, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertSearchVisibility(t, store.Service(), encoder, map[fts.DocID]chunk.ID{
		"doc-a":     "a-new",
		"doc-added": "added-1",
	})
	addAndFlush(t, store.Service(), encoder, "doc-after-restart")
	if got := store.Service().Statistics().MaxAllocatedVectorID; got != 5 {
		t.Fatalf("max allocated vector ID after restart = %d, want 5", got)
	}
	if generation, err := store.Publish(ctx, Options{Durability: DurabilityAsynchronous}); err != nil || generation.ID != 3 {
		t.Fatalf("Store.Publish after restart = %+v, %v", generation, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertSearchVisibility(t, store.Service(), encoder, map[fts.DocID]chunk.ID{
		"doc-a":             "a-new",
		"doc-added":         "added-1",
		"doc-after-restart": "after-1",
	})
	if got := store.Service().Statistics().MaxAllocatedVectorID; got != 5 {
		t.Fatalf("reopened max allocated vector ID = %d, want 5", got)
	}
}

func TestOperationsRespectCanceledContext(t *testing.T) {
	service, _ := persistenceService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root := filepath.Join(t.TempDir(), "store")
	if _, err := Publish(ctx, root, service, Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish error = %v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled Publish created store: %v", err)
	}
}

func TestOpenHoldsLifetimeWriterLock(t *testing.T) {
	ctx := context.Background()
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	root := t.TempDir()
	if _, err := Publish(ctx, root, service, Options{}); err != nil {
		t.Fatal(err)
	}
	store, err := Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, root, OpenOptions{}); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("second Open error = %v", err)
	}
	if _, err := Publish(ctx, root, service, Options{ExpectedGeneration: 1}); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("detached Publish error = %v", err)
	}
	if err := RepairCurrent(ctx, root, 1, Options{}); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("RepairCurrent error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, root, OpenOptions{}); err != nil {
		t.Fatalf("Open after Close = %v", err)
	}
}

func TestStorePublishCloseAndCancellationAreSynchronized(t *testing.T) {
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	root := t.TempDir()
	if _, err := Publish(t.Context(), root, service, Options{Durability: DurabilityAsynchronous}); err != nil {
		t.Fatal(err)
	}
	store, err := Open(t.Context(), root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Publish(nil, Options{}); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil-context Publish error = %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	published := make(chan error, 1)
	go func() {
		_, err := store.Publish(context.Background(), Options{Durability: DurabilityAsynchronous, beforeStep: func(step publicationStep) error {
			if step == stepWriteState {
				close(entered)
				<-release
			}
			return nil
		}})
		published <- err
	}()
	<-entered
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Publish(waitCtx, Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting Publish error = %v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- store.Close() }()
	close(release)
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if _, err := store.Publish(t.Context(), Options{}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Publish after Close error = %v", err)
	}
}

func TestPublicationFailurePreservesCURRENTAndExplicitRepairSelectsOrphan(t *testing.T) {
	ctx := context.Background()
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	root := t.TempDir()
	if _, err := Publish(ctx, root, service, Options{Durability: DurabilityAsynchronous}); err != nil {
		t.Fatal(err)
	}
	addAndFlush(t, service, encoder, "doc-b")
	injected := errors.New("stop before CURRENT")
	_, err := Publish(ctx, root, service, Options{Durability: DurabilityAsynchronous, ExpectedGeneration: 1, afterStep: func(step publicationStep) error {
		if step == stepRenameGeneration {
			return injected
		}
		return nil
	}})
	if !errors.Is(err, injected) {
		t.Fatalf("Publish error = %v", err)
	}
	store, err := Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if store.generation.ID != 1 || store.Service().Statistics().Documents != 1 {
		t.Fatalf("active generation after failure = %+v", store.generation)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := RepairCurrent(ctx, root, 2, Options{Durability: DurabilityAsynchronous}); err != nil {
		t.Fatal(err)
	}
	repaired, err := Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer repaired.Close()
	if repaired.generation.ID != 2 || repaired.Service().Statistics().Documents != 2 {
		t.Fatalf("repaired generation = %+v, stats = %+v", repaired.generation, repaired.Service().Statistics())
	}
	encoder.vectors["doc-c"] = []semantic.EncodedChunk{testVector("doc-c", "c-1", []float32{0.5, 0.5})}
	addAndFlush(t, repaired.Service(), encoder, "doc-c")
	if generation, err := repaired.Publish(ctx, Options{Durability: DurabilityAsynchronous}); err != nil || generation.ID != 3 {
		t.Fatalf("Publish after repair = %+v, %v", generation, err)
	}
}

func TestFailureAfterCURRENTIsIndeterminateAndVisible(t *testing.T) {
	ctx := context.Background()
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	root := t.TempDir()
	injected := errors.New("after CURRENT")
	_, err := Publish(ctx, root, service, Options{Durability: DurabilityAsynchronous, afterStep: func(step publicationStep) error {
		if step == stepReplaceCurrent {
			return injected
		}
		return nil
	}})
	if !errors.Is(err, ErrIndeterminate) {
		t.Fatalf("Publish error = %v", err)
	}
	store, err := Open(ctx, root, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.generation.ID != 1 {
		t.Fatalf("generation = %+v", store.generation)
	}
}

func TestPublicationCrashMatrix(t *testing.T) {
	preCommit := []publicationStep{
		stepWriteVectors, stepWriteGraph, stepSyncSegment, stepRenameSegment,
		stepWriteState, stepWriteManifest, stepSyncGeneration,
		stepRenameGeneration, stepWriteCurrent, stepReplaceCurrent,
	}
	for _, step := range preCommit {
		t.Run("before_"+string(step), func(t *testing.T) {
			service, encoder := persistenceService(t)
			addAndFlush(t, service, encoder, "doc-a")
			injected := errors.New("injected")
			root := t.TempDir()
			_, err := Publish(t.Context(), root, service, Options{beforeStep: func(got publicationStep) error {
				if got == step {
					return injected
				}
				return nil
			}})
			if !errors.Is(err, injected) {
				t.Fatalf("Publish error = %v", err)
			}
			if _, err := Open(t.Context(), root, OpenOptions{}); !errors.Is(err, ErrCurrentMissing) {
				t.Fatalf("Open error = %v, want missing CURRENT", err)
			}
		})
	}
	for _, step := range preCommit[:len(preCommit)-1] {
		t.Run("after_"+string(step), func(t *testing.T) {
			service, encoder := persistenceService(t)
			addAndFlush(t, service, encoder, "doc-a")
			injected := errors.New("injected")
			root := t.TempDir()
			_, err := Publish(t.Context(), root, service, Options{afterStep: func(got publicationStep) error {
				if got == step {
					return injected
				}
				return nil
			}})
			if !errors.Is(err, injected) {
				t.Fatalf("Publish error = %v", err)
			}
			if _, err := Open(t.Context(), root, OpenOptions{}); !errors.Is(err, ErrCurrentMissing) {
				t.Fatalf("Open error = %v, want missing CURRENT", err)
			}
		})
	}
	for _, step := range preCommit {
		t.Run("update_before_"+string(step), func(t *testing.T) {
			service, encoder := persistenceService(t)
			addAndFlush(t, service, encoder, "doc-a")
			root := t.TempDir()
			if _, err := Publish(t.Context(), root, service, Options{}); err != nil {
				t.Fatal(err)
			}
			addAndFlush(t, service, encoder, "doc-b")
			injected := errors.New("injected")
			_, err := Publish(t.Context(), root, service, Options{ExpectedGeneration: 1, beforeStep: func(got publicationStep) error {
				if got == step {
					return injected
				}
				return nil
			}})
			if !errors.Is(err, injected) {
				t.Fatalf("Publish error = %v", err)
			}
			store, err := Open(t.Context(), root, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if store.generation.ID != 1 || store.Service().Statistics().Documents != 1 {
				t.Fatalf("active generation/state = %+v/%+v", store.generation, store.Service().Statistics())
			}
		})
	}

	postCommit := []publicationStep{stepReplaceCurrent, stepSyncStore}
	for _, step := range postCommit {
		t.Run("after_"+string(step), func(t *testing.T) {
			service, encoder := persistenceService(t)
			addAndFlush(t, service, encoder, "doc-a")
			injected := errors.New("injected")
			root := t.TempDir()
			_, err := Publish(t.Context(), root, service, Options{afterStep: func(got publicationStep) error {
				if got == step {
					return injected
				}
				return nil
			}})
			if !errors.Is(err, ErrIndeterminate) {
				t.Fatalf("Publish error = %v", err)
			}
			store, err := Open(t.Context(), root, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if store.generation.ID != 1 {
				t.Fatalf("generation = %+v", store.generation)
			}
		})
	}
	t.Run("before_"+string(stepSyncStore), func(t *testing.T) {
		service, encoder := persistenceService(t)
		addAndFlush(t, service, encoder, "doc-a")
		root := t.TempDir()
		_, err := Publish(t.Context(), root, service, Options{beforeStep: func(step publicationStep) error {
			if step == stepSyncStore {
				return errors.New("injected")
			}
			return nil
		}})
		if !errors.Is(err, ErrIndeterminate) {
			t.Fatalf("Publish error = %v", err)
		}
		store, err := Open(t.Context(), root, OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if store.generation.ID != 1 {
			t.Fatalf("generation = %+v", store.generation)
		}
	})
}

func TestPublishRejectsStaleGeneration(t *testing.T) {
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	root := t.TempDir()
	if _, err := Publish(t.Context(), root, service, Options{Durability: DurabilityAsynchronous}); err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(t.Context(), root, service, Options{Durability: DurabilityAsynchronous}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("Publish error = %v", err)
	}
}

func TestOpenRejectsCorruptGenerationFiles(t *testing.T) {
	for _, relative := range []string{
		filepath.Join(generationsDirectory, generationName(1), stateFileName),
		filepath.Join(generationsDirectory, generationName(1), manifestFileName),
		currentFileName,
		"vectors",
		"graph",
	} {
		t.Run(relative, func(t *testing.T) {
			service, encoder := persistenceService(t)
			addAndFlush(t, service, encoder, "doc-a")
			root := t.TempDir()
			_, err := Publish(t.Context(), root, service, Options{Durability: DurabilityAsynchronous})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, relative)
			if relative == "vectors" {
				path = filepath.Join(root, objectsDirectory, segmentsDirectory, soleSegmentObjectID(t, root), vectorsFileName)
			}
			if relative == "graph" {
				path = filepath.Join(root, objectsDirectory, segmentsDirectory, soleSegmentObjectID(t, root), graphFileName)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)/2] ^= 0xff
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if store, err := Open(t.Context(), root, OpenOptions{}); err == nil {
				_ = store.Close()
				t.Fatal("Open accepted corrupt generation")
			}
		})
	}
}

func TestOpenRejectsReferencedFileLargerThanDeclaredSize(t *testing.T) {
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	root := t.TempDir()
	if _, err := Publish(t.Context(), root, service, Options{Durability: DurabilityAsynchronous}); err != nil {
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
	if _, err := Open(t.Context(), root, OpenOptions{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open mismatched referenced size error = %v", err)
	}
}

func TestOpenRejectsDescriptorMismatchLimitsAndLockSymlink(t *testing.T) {
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	root := t.TempDir()
	if _, err := Publish(t.Context(), root, service, Options{Durability: DurabilityAsynchronous}); err != nil {
		t.Fatal(err)
	}
	otherEmbedding, err := semantic.NewEmbeddingDescriptor("other", "model", "v1", "fingerprint", 2, vector.MetricL2Squared, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), root, OpenOptions{ExpectedDescriptors: semantic.PipelineDescriptor{Embedding: otherEmbedding, Chunking: encoder.descriptor.Chunking}}); !errors.Is(err, ErrEmbeddingMismatch) {
		t.Fatalf("descriptor mismatch error = %v", err)
	}
	limits := DefaultLimits()
	limits.MaxFileBytes = 4096
	limits.MaxVectorBytes = 4096
	limits.MaxGraphBytes = 4096
	limits.MaxOpenBytes = 4096
	if _, err := Open(t.Context(), root, OpenOptions{Limits: limits}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("open allocation limit error = %v", err)
	}
	lockPath := filepath.Join(root, "LOCK")
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "target"), lockPath); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), root, OpenOptions{}); !errors.Is(err, ErrSymlink) {
		t.Fatalf("lock symlink error = %v", err)
	}
}

func TestOpenRejectsSymlinkedSegmentObject(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" && runtime.GOOS != "freebsd" {
		t.Skip("writable store locking is unsupported")
	}
	service, encoder := persistenceService(t)
	addAndFlush(t, service, encoder, "doc-a")
	root := t.TempDir()
	_, err := Publish(t.Context(), root, service, Options{Durability: DurabilityAsynchronous})
	if err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(root, objectsDirectory, segmentsDirectory, soleSegmentObjectID(t, root))
	realPath := objectPath + "-real"
	if err := os.Rename(objectPath, realPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realPath, objectPath); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(t.Context(), root, OpenOptions{}); !errors.Is(err, ErrSymlink) {
		t.Fatalf("symlinked object error = %v", err)
	}
}

func TestObjectIDAndPathContainmentValidation(t *testing.T) {
	ref := fileReference{Size: 1, SHA256: [32]byte{1}}
	if id := segmentObjectID(ref, ref); !validObjectID(id) {
		t.Fatalf("generated object ID %q is invalid", id)
	}
	for _, id := range []string{"", ".", "..", "seg-../x", "seg-foo/bar", `seg-foo\bar`, "/absolute", "SEG-" + string(make([]byte, 64))} {
		if validObjectID(id) {
			t.Errorf("validObjectID(%q) = true", id)
		}
	}

	root := t.TempDir()
	if err := ensureContained(root, filepath.Join(root, objectsDirectory, segmentsDirectory, segmentObjectID(ref, ref))); err != nil {
		t.Fatalf("contained path rejected: %v", err)
	}
	for _, path := range []string{
		filepath.Join(root, "..", "outside"),
		root + "-sibling",
	} {
		if err := ensureContained(root, path); !errors.Is(err, ErrPathEscape) {
			t.Errorf("ensureContained(%q) error = %v", path, err)
		}
	}
}

func persistenceService(t testing.TB) (*semantic.Service, staticEncoder) {
	t.Helper()
	embedding, err := semantic.NewEmbeddingDescriptor("test", "model", "v1", "fingerprint", 2, vector.MetricL2Squared, 1)
	if err != nil {
		t.Fatal(err)
	}
	config := semantic.Config{
		Embedding: embedding, Chunking: semantic.ChunkingDescriptor{ID: "chunks", Version: 1, Fingerprint: "chunks-v1"},
		Limits: semantic.Limits{
			MaxLiveVectors: 100, MaxChunksPerDocument: 4, MaxDocumentsPerSearch: 4,
			MaxChunkCandidates: 8, MaxChunksPerDocumentHit: 2,
		},
	}
	service, err := semantic.New(config)
	if err != nil {
		t.Fatal(err)
	}
	encoder := staticEncoder{descriptor: semantic.PipelineDescriptor{Embedding: config.Embedding, Chunking: config.Chunking}, vectors: map[fts.DocID][]semantic.EncodedChunk{
		"doc-a": {testVector("doc-a", "a-1", []float32{1, 0})},
		"doc-b": {testVector("doc-b", "b-1", []float32{0, 1})},
	}}
	return service, encoder
}

func addAndFlush(t testing.TB, service *semantic.Service, encoder staticEncoder, id fts.DocID) {
	t.Helper()
	if err := service.AddDocument(context.Background(), encoder, fts.Document{ID: id}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func assertSearchVisibility(t testing.TB, service *semantic.Service, encoder staticEncoder, want map[fts.DocID]chunk.ID) {
	t.Helper()
	result, err := service.SearchDocuments(context.Background(), encoder, fts.Document{ID: "query"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != len(want) {
		t.Fatalf("search hits = %+v, want documents = %+v", result.Hits, want)
	}
	for _, hit := range result.Hits {
		wantChunk, ok := want[hit.DocID]
		if !ok || len(hit.Chunks) != 1 || hit.Chunks[0].Ref.ID != wantChunk {
			t.Fatalf("search hit = %+v, want documents = %+v", hit, want)
		}
	}
}

func soleSegmentObjectID(t testing.TB, root string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, objectsDirectory, segmentsDirectory))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !entries[0].IsDir() || !validObjectID(entries[0].Name()) {
		t.Fatalf("segment objects = %+v", entries)
	}
	return entries[0].Name()
}

func testVector(docID fts.DocID, id chunk.ID, value []float32) semantic.EncodedChunk {
	return semantic.EncodedChunk{Ref: chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, EndByte: 1}, Vector: value}
}
