package semanticpersist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
)

func TestSaveRejectsPendingMutations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	index := newTestIndex(t)
	if err := stageTestDocument(ctx, index, "pending", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}

	_, err := Save(ctx, root, index, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if !errors.Is(err, semantic.ErrPendingMutations) {
		t.Fatalf("Save() error = %v, want semantic.ErrPendingMutations", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("storage root error = %v, want os.ErrNotExist", err)
	}
}

func TestSaveServiceRejectsExternalEncoders(t *testing.T) {
	index := newTestIndex(t)
	encoder := persistenceTestEncoder{}
	semanticService, err := service.NewService(index, encoder, encoder)
	if err != nil {
		t.Fatal(err)
	}
	options := SaveOptions{ExpectedGeneration: &Generation{}}
	if _, err := SaveService(t.Context(), t.TempDir(), semanticService, options); !errors.Is(err, service.ErrModelServiceUnavailable) {
		t.Fatalf("SaveService() error = %v, want ErrModelServiceUnavailable", err)
	}
	if _, err := CommitAndSaveService(t.Context(), t.TempDir(), semanticService, options); !errors.Is(err, service.ErrModelServiceUnavailable) {
		t.Fatalf("CommitAndSaveService() error = %v, want ErrModelServiceUnavailable", err)
	}
}

func TestCommitAndSaveServiceFailureKeepsMutationPending(t *testing.T) {
	client := newLineageTestClient(t)
	config, err := service.DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	semanticService, err := service.New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := semanticService.AddDocument(t.Context(), persistenceDocument("pending")); err != nil {
		t.Fatal(err)
	}

	failed := errors.New("injected service save failure")
	settings, err := newSettings(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	settings.checkpoints.probe = saveFaultInjector{beforeStep: func(checkpoint saveCheckpoint) error {
		if checkpoint == checkpointWriteCurrent {
			return failed
		}
		return nil
	}}
	serviceConfig, err := persistedService(semanticService)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if _, err := commitAndSaveService(t.Context(), root, semanticService, serviceConfig, 0, settings); !errors.Is(err, failed) {
		t.Fatalf("commitAndSaveService() error = %v, want injected error", err)
	}
	if _, err := semanticService.ExportState(t.Context()); !errors.Is(err, semantic.ErrPendingMutations) {
		t.Fatalf("ExportState() error = %v, want ErrPendingMutations", err)
	}
	result, err := semanticService.Search(t.Context(), "pending", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("failed candidate became searchable: %+v", result.Hits)
	}
}

func TestCommitAndSaveServiceIndeterminatePublishesCandidate(t *testing.T) {
	client := newLineageTestClient(t)
	config, err := service.DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	semanticService, err := service.New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := semanticService.AddDocument(t.Context(), persistenceDocument("committed")); err != nil {
		t.Fatal(err)
	}

	settings, err := newSettings(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	settings.checkpoints.probe = saveFaultInjector{afterStep: func(checkpoint saveCheckpoint) error {
		if checkpoint == checkpointReplaceCurrent {
			return errors.New("after CURRENT replacement")
		}
		return nil
	}}
	serviceConfig, err := persistedService(semanticService)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	generationID, err := commitAndSaveService(t.Context(), root, semanticService, serviceConfig, 0, settings)
	if !errors.Is(err, ErrIndeterminate) {
		t.Fatalf("commitAndSaveService() error = %v, want ErrIndeterminate", err)
	}
	if generationID != 0 {
		t.Fatalf("generation ID = %d, want 0 for indeterminate result", generationID)
	}
	committed, err := semanticService.ExportState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if committed.Revision != 1 {
		t.Fatalf("in-memory revision = %d, want 1", committed.Revision)
	}
	loader, err := NewLoader(root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, generationID, err := service.Load(t.Context(), loader, client)
	if err != nil {
		t.Fatal(err)
	}
	if generationID != 1 {
		t.Fatalf("loaded generation = %d, want 1", generationID)
	}
	loadedState, err := loaded.ExportState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if loadedState.Revision != committed.Revision {
		t.Fatalf("loaded revision = %d, want %d", loadedState.Revision, committed.Revision)
	}
}

type persistenceTestEncoder struct{}

func (persistenceTestEncoder) EncodeDocument(context.Context, fts.Document) ([]semantic.ChunkEmbedding, error) {
	return nil, nil
}

func (persistenceTestEncoder) EncodeQuery(context.Context, string) ([]semantic.ChunkEmbedding, error) {
	return nil, nil
}

func (persistenceTestEncoder) EncodeQueryDocument(context.Context, fts.Document) ([]semantic.ChunkEmbedding, error) {
	return nil, nil
}

func TestCommitAndSavePersistsPendingRevision(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	if err := stageTestDocument(ctx, index, "committed", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}

	generation, err := CommitAndSave(ctx, root, index, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if generation.ID != 1 {
		t.Fatalf("generation = %d, want 1", generation.ID)
	}

	loaded, loadedGeneration, err := Load(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if loadedGeneration != generation {
		t.Fatalf("loaded generation = %d, want %d", loadedGeneration.ID, generation.ID)
	}
	state, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 1 || len(state.Segments) != 1 {
		t.Fatalf("loaded state = revision %d, segments %d; want 1, 1",
			state.Revision, len(state.Segments))
	}
	if len(state.Segments[0].Rows) != 1 {
		t.Fatalf("loaded rows = %d, want 1", len(state.Segments[0].Rows))
	}
	if got := state.Segments[0].Rows[0].Chunk.DocID; got != "committed" {
		t.Fatalf("loaded document = %q, want committed", got)
	}
}

func TestCommitAndSaveStaleGenerationKeepsMutationPending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	if err := stageTestDocument(ctx, index, "persisted", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}
	persistedGeneration, err := CommitAndSave(ctx, root, index, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := stageTestDocument(ctx, index, "memory-only", []float32{3, 4}); err != nil {
		t.Fatal(err)
	}
	_, err = CommitAndSave(ctx, root, index, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("CommitAndSave() error = %v, want ErrStaleGeneration", err)
	}

	if _, err := index.ExportState(ctx); !errors.Is(err, semantic.ErrPendingMutations) {
		t.Fatalf("ExportState() error = %v, want semantic.ErrPendingMutations", err)
	}
	result, err := index.Search(
		ctx,
		[]semantic.ChunkEmbedding{{Embedding: []float32{3, 4}}},
		2,
		semantic.QueryConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range result.Hits {
		if hit.DocID == "memory-only" {
			t.Fatalf("Search() exposed rejected candidate: %+v", result.Hits)
		}
	}

	loaded, loadedGeneration, err := Load(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if loadedGeneration != persistedGeneration {
		t.Fatalf("loaded generation = %d, want %d", loadedGeneration.ID, persistedGeneration.ID)
	}
	persistedState, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if persistedState.Revision != 1 || len(persistedState.Segments) != 1 {
		t.Fatalf("persisted state = revision %d, segments %d; want 1, 1",
			persistedState.Revision, len(persistedState.Segments))
	}

	if err := stageTestDocument(ctx, index, "next", []float32{5, 6}); err != nil {
		t.Fatal(err)
	}
	nextGeneration, err := CommitAndSave(ctx, root, index, SaveOptions{
		ExpectedGeneration: &persistedGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	if nextGeneration.ID != persistedGeneration.ID+1 {
		t.Fatalf("retry generation = %d, want %d",
			nextGeneration.ID, persistedGeneration.ID+1)
	}
	retriedState, err := index.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if retriedState.Revision != 3 || len(retriedState.Segments) != 2 {
		t.Fatalf("retried state = revision %d, segments %d; want 3, 2",
			retriedState.Revision, len(retriedState.Segments))
	}
	if len(retriedState.Segments[1].Rows) != 2 {
		t.Fatalf("retried pending rows = %d, want 2", len(retriedState.Segments[1].Rows))
	}
}

func TestCommitAndSaveFailureBeforeCurrentKeepsMutationPending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	if err := stageTestDocument(ctx, index, "persisted", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}
	previous, err := CommitAndSave(ctx, root, index, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stageTestDocument(ctx, index, "pending", []float32{3, 4}); err != nil {
		t.Fatal(err)
	}

	failed := errors.New("injected save failure")
	cfg, err := newSettings(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.checkpoints.probe = saveFaultInjector{
		beforeStep: func(checkpoint saveCheckpoint) error {
			if checkpoint == checkpointWriteCurrent {
				return failed
			}
			return nil
		},
	}
	_, err = commitAndSave(ctx, root, index, nil, previous.ID, cfg)
	if !errors.Is(err, failed) {
		t.Fatalf("commitAndSave() error = %v, want injected failure", err)
	}
	if _, err := index.ExportState(ctx); !errors.Is(err, semantic.ErrPendingMutations) {
		t.Fatalf("ExportState() error = %v, want semantic.ErrPendingMutations", err)
	}
	result, err := index.Search(
		ctx,
		[]semantic.ChunkEmbedding{{Embedding: []float32{3, 4}}},
		2,
		semantic.QueryConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range result.Hits {
		if hit.DocID == "pending" {
			t.Fatalf("Search() exposed failed candidate: %+v", result.Hits)
		}
	}

	loaded, loadedGeneration, err := Load(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if loadedGeneration != previous {
		t.Fatalf("loaded generation = %d, want %d", loadedGeneration.ID, previous.ID)
	}
	loadedState, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loadedState.Revision != 1 {
		t.Fatalf("loaded revision = %d, want 1", loadedState.Revision)
	}

	if _, err := CommitAndSave(ctx, root, index, SaveOptions{
		ExpectedGeneration: &previous,
	}); err != nil {
		t.Fatalf("CommitAndSave() retry error = %v", err)
	}
	committed, err := index.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Revision != 2 {
		t.Fatalf("committed revision after retry = %d, want 2", committed.Revision)
	}
}

func TestCommitAndSaveIndeterminateOutcomePublishesCandidate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	if err := stageTestDocument(ctx, index, "committed", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}

	failed := errors.New("after CURRENT replacement")
	cfg, err := newSettings(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.checkpoints.probe = saveFaultInjector{
		afterStep: func(checkpoint saveCheckpoint) error {
			if checkpoint == checkpointReplaceCurrent {
				return failed
			}
			return nil
		},
	}
	generationID, err := commitAndSave(ctx, root, index, nil, 0, cfg)
	if !errors.Is(err, ErrIndeterminate) {
		t.Fatalf("commitAndSave() error = %v, want ErrIndeterminate", err)
	}
	if generationID != 0 {
		t.Fatalf("returned generation ID = %d, want 0 for indeterminate result", generationID)
	}
	committed, err := index.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Revision != 1 {
		t.Fatalf("in-memory revision = %d, want 1", committed.Revision)
	}

	loaded, loadedGeneration, err := Load(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if loadedGeneration.ID != 1 {
		t.Fatalf("loaded generation = %d, want 1", loadedGeneration.ID)
	}
	loadedState, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loadedState.Revision != committed.Revision {
		t.Fatalf("loaded revision = %d, want in-memory revision %d",
			loadedState.Revision, committed.Revision)
	}
}

func TestSaveSameCommittedRevisionReusesSegmentObject(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	if err := stageTestDocument(ctx, index, "doc", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}

	first, err := CommitAndSave(ctx, root, index, SaveOptions{
		ExpectedGeneration: &Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}
	objectsBefore, err := os.ReadDir(newLayout(root).segments)
	if err != nil {
		t.Fatal(err)
	}

	second, err := Save(ctx, root, index, SaveOptions{ExpectedGeneration: &first})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID+1 {
		t.Fatalf("second generation = %d, want %d", second.ID, first.ID+1)
	}
	objectsAfter, err := os.ReadDir(newLayout(root).segments)
	if err != nil {
		t.Fatal(err)
	}
	if len(objectsAfter) != len(objectsBefore) {
		t.Fatalf("segment objects after repeat save = %d, want %d", len(objectsAfter), len(objectsBefore))
	}

	loaded, _, err := Load(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Revision != 1 {
		t.Fatalf("loaded revision = %d, want 1", state.Revision)
	}
}

func TestCommitAndSaveBlocksMutationUntilPersistenceCompletes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	index := newTestIndex(t)
	if err := stageTestDocument(ctx, index, "captured", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}

	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseWrite) }) }
	defer release()

	cfg, err := newSettings(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.checkpoints.probe = saveFaultInjector{
		beforeStep: func(checkpoint saveCheckpoint) error {
			if checkpoint == checkpointWriteVectors {
				startOnce.Do(func() { close(writeStarted) })
				<-releaseWrite
			}
			return nil
		},
	}

	saveDone := make(chan error, 1)
	go func() {
		_, err := commitAndSave(ctx, root, index, nil, 0, cfg)
		saveDone <- err
	}()
	<-writeStarted

	mutationDone := make(chan error, 1)
	go func() {
		mutationDone <- stageTestDocument(ctx, index, "next", []float32{3, 4})
	}()
	select {
	case err := <-mutationDone:
		release()
		<-saveDone
		t.Fatalf("mutation completed before filesystem save: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected: Prepare, Save, and Publish share one mutation gate.
	}
	result, err := index.Search(
		ctx,
		[]semantic.ChunkEmbedding{{Embedding: []float32{1, 2}}},
		1,
		semantic.QueryConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("Search() during save returned hits %+v, want previous empty view", result.Hits)
	}

	release()
	if err := <-saveDone; err != nil {
		t.Fatal(err)
	}
	if err := <-mutationDone; err != nil {
		t.Fatal(err)
	}
	result, err = index.Search(
		ctx,
		[]semantic.ChunkEmbedding{{Embedding: []float32{1, 2}}},
		1,
		semantic.QueryConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != "captured" {
		t.Fatalf("Search() after save returned hits %+v, want captured", result.Hits)
	}
	if _, err := index.ExportState(ctx); !errors.Is(err, semantic.ErrPendingMutations) {
		t.Fatalf("ExportState() error = %v, want pending mutation", err)
	}

	loaded, _, err := Load(ctx, root, LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := loaded.ExportState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Segments) != 1 {
		t.Fatalf("saved segments = %d, want 1", len(state.Segments))
	}
	if len(state.Segments[0].Rows) != 1 {
		t.Fatalf("saved rows = %d, want 1", len(state.Segments[0].Rows))
	}
	if got := state.Segments[0].Rows[0].Chunk.DocID; got != "captured" {
		t.Fatalf("saved document = %q, want captured", got)
	}
}

func stageTestDocument(
	ctx context.Context,
	index *semantic.Index,
	docID fts.DocID,
	embedding []float32,
) error {
	return index.AddDocument(ctx, docID, []semantic.ChunkEmbedding{{
		Ref: semantic.Ref{
			ID:      semantic.ChunkID(docID + "-chunk"),
			DocID:   docID,
			Field:   fts.DefaultField,
			EndByte: 1,
		},
		Embedding: embedding,
	}})
}
