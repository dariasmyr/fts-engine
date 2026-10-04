package semantic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/vector"
	"github.com/dariasmyr/fts-engine/pkg/vectorstore"
)

func testConfig(maxDocuments, maxCandidates int) Config {
	embedding, err := NewEmbeddingDescriptor("test-provider", "test-model", "v1", "test-embedding-v1", 2, vector.MetricL2Squared, 1)
	if err != nil {
		panic(err)
	}
	return Config{
		Embedding: embedding,
		Chunking:  ChunkingDescriptor{ID: "test-chunks-v1", Version: 1, Fingerprint: "test-chunks-fp-v1"},
		Limits: Limits{
			MaxLiveVectors:          200,
			MaxChunksPerDocument:    20,
			MaxDocumentsPerSearch:   maxDocuments,
			MaxChunkCandidates:      maxCandidates,
			MaxChunksPerDocumentHit: 3,
		},
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	service, err := New(testConfig(10, 100))
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func addDocument(t *testing.T, service *Service, ctx context.Context, batch []EncodedChunk) error {
	t.Helper()
	if len(batch) == 0 {
		return service.addEncodedDocument(ctx, "", batch)
	}
	if err := service.addEncodedDocument(ctx, batch[0].Ref.DocID, batch); err != nil {
		return err
	}
	return service.Flush(ctx)
}

func replaceDocument(t *testing.T, service *Service, ctx context.Context, batch []EncodedChunk) error {
	t.Helper()
	if len(batch) == 0 {
		return service.replaceEncodedDocument(ctx, "", batch)
	}
	if err := service.replaceEncodedDocument(ctx, batch[0].Ref.DocID, batch); err != nil {
		return err
	}
	return service.Flush(ctx)
}

func deleteDocument(t *testing.T, service *Service, docID fts.DocID) bool {
	t.Helper()
	err := service.DeleteDocument(context.Background(), docID)
	if errors.Is(err, ErrDocumentNotFound) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	return true
}

func testChunk(docID fts.DocID, id chunk.ID, ordinal uint32, value []float32) EncodedChunk {
	return EncodedChunk{
		Ref:    chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, Ordinal: ordinal, StartByte: uint64(ordinal * 10), EndByte: uint64(ordinal*10 + 10)},
		Vector: value,
	}
}

func TestLifecycleChunkSearchGroupingAndStatistics(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{
		testChunk("doc-a", "a-1", 0, []float32{0, 0}),
		testChunk("doc-a", "a-2", 1, []float32{10, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "b-1", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}

	chunks, err := service.searchChunks(ctx, []float32{0, 0}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks.Hits) != 3 || chunks.Hits[0].Ref.DocID != "doc-a" || chunks.Hits[1].Ref.DocID != "doc-b" {
		t.Fatalf("chunk hits = %+v", chunks.Hits)
	}
	documents, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if documents.GroupingIncomplete || len(documents.Hits) != 2 || documents.Hits[0].DocID != "doc-a" || documents.Hits[1].DocID != "doc-b" {
		t.Fatalf("document result = %+v", documents)
	}
	if documents.Hits[0].Distance != 0 || len(documents.Hits[0].Chunks) != 2 {
		t.Fatalf("doc-a grouping = %+v", documents.Hits[0])
	}
	stats := service.Statistics()
	if stats.Documents != 2 || stats.PhysicalVectors != 3 || stats.LiveVectors != 3 || stats.StaleVectors != 0 || stats.MaxAllocatedVectorID != 3 {
		t.Fatalf("statistics = %+v", stats)
	}

	badReplacement := []EncodedChunk{testChunk("doc-a", "bad", 0, []float32{1})}
	if err := service.replaceEncodedDocument(ctx, "doc-a", badReplacement); !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Fatalf("replacement error = %v", err)
	}
	nearest, err := service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || nearest.Hits[0].Ref.ID != "a-1" {
		t.Fatalf("old version not preserved: %+v, %v", nearest, err)
	}
	if err := replaceDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-new", 0, []float32{3, 0})}); err != nil {
		t.Fatal(err)
	}
	nearest, err = service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if nearest.Hits[0].Ref.DocID != "doc-b" || nearest.Hits[1].Ref.ID != "a-new" {
		t.Fatalf("stale chunks leaked: %+v", nearest.Hits)
	}
	if !deleteDocument(t, service, "doc-b") || !errors.Is(service.DeleteDocument(ctx, "doc-b"), ErrDocumentNotFound) {
		t.Fatal("DeleteDocument result mismatch")
	}
	stats = service.Statistics()
	if stats.Documents != 1 || stats.PhysicalVectors != 4 || stats.LiveVectors != 1 || stats.StaleVectors != 3 || stats.MaxAllocatedVectorID != 4 {
		t.Fatalf("post-update statistics = %+v", stats)
	}
}

func TestMultiSegmentSearchMergesChunksAndDocuments(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-far", "far", 0, []float32{10, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-near", "near", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	chunks, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks.Hits) != 2 || chunks.Hits[0].Ref.DocID != "doc-near" || chunks.Hits[1].Ref.DocID != "doc-far" {
		t.Fatalf("merged chunk hits = %+v", chunks.Hits)
	}

	documents, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if documents.GroupingIncomplete || len(documents.Hits) != 2 || documents.Hits[0].DocID != "doc-near" || documents.Hits[1].DocID != "doc-far" {
		t.Fatalf("merged document hits = %+v", documents)
	}
}

func TestMultiSegmentSearchUsesDeterministicComponentTieOrdering(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-first", "first", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-second", "second", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	result, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.Hits[0].Ref.DocID != "doc-first" || result.Hits[1].Ref.DocID != "doc-second" {
		t.Fatalf("tie-ordered hits = %+v", result.Hits)
	}
}

func TestMultiSegmentSearchAppliesPublishedStaleFilters(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-old", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-live", "live", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteDocument(ctx, "doc-old"); err != nil {
		t.Fatal(err)
	}

	beforeFlush, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeFlush.Hits) != 2 {
		t.Fatalf("pre-publication hits = %+v", beforeFlush.Hits)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	afterFlush, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterFlush.Hits) != 1 || afterFlush.Hits[0].Ref.DocID != "doc-live" {
		t.Fatalf("post-publication hits = %+v", afterFlush.Hits)
	}
}

func TestMultiSegmentSearchPropagatesIncompleteANNState(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 3 {
		docID := fts.DocID(fmt.Sprintf("doc-%d", i))
		if err := service.addEncodedDocument(ctx, docID, []EncodedChunk{testChunk(docID, chunk.ID(fmt.Sprintf("chunk-%d", i)), 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := service.searchEncodedDocumentsWithOptions(ctx, []float32{0, 0}, 1, SearchOptions{
		CandidateChunks: 3,
		VisitLimit:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.GroupingIncomplete || result.Stats.Termination != vector.TerminationVisitLimit {
		t.Fatalf("incomplete grouping result = %+v", result)
	}
}

func TestMutationsBecomeVisibleAfterFlush(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	old := []EncodedChunk{testChunk("doc", "old", 0, []float32{0, 0})}
	if err := service.addEncodedDocument(ctx, "doc", old); err != nil {
		t.Fatal(err)
	}
	result, err := service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("unflushed add is visible: %+v", result.Hits)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.ID != "old" {
		t.Fatalf("flushed add result = %+v, %v", result, err)
	}

	if err := service.replaceEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", "new", 0, []float32{10, 0})}); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.ID != "old" {
		t.Fatalf("unflushed replacement result = %+v, %v", result, err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.ID != "new" {
		t.Fatalf("flushed replacement result = %+v, %v", result, err)
	}

	if err := service.DeleteDocument(ctx, "doc"); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.ID != "new" {
		t.Fatalf("unflushed deletion result = %+v, %v", result, err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 0 {
		t.Fatalf("flushed deletion result = %+v, %v", result, err)
	}
}

func TestRepeatedPendingReplacementIsCoalesced(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", "stable", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	for version := 1; version <= 20; version++ {
		if err := service.replaceEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", "stable", 0, []float32{float32(version), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	if stats := service.Statistics(); stats.PhysicalVectors != 1 || stats.LiveVectors != 1 || stats.MaxAllocatedVectorID != 21 {
		t.Fatalf("coalesced pending statistics = %+v", stats)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := service.searchChunks(ctx, []float32{20, 0}, 1)
	if err != nil || len(result.Hits) != 1 || result.Hits[0].Distance != 0 {
		t.Fatalf("coalesced result = %+v, %v", result, err)
	}
}

func TestDiscardSupersededVersionSeparatesPublishedAndPendingIDs(t *testing.T) {
	tests := []struct {
		name          string
		ids           []uint64
		published     []uint64
		wantPublished []uint64
		wantPending   []uint64
		wantErr       error
	}{
		{name: "published", ids: []uint64{1, 2}, published: []uint64{1, 2}, wantPublished: []uint64{1, 2}, wantPending: []uint64{1, 2, 3, 4, 5, 6}},
		{name: "pending head", ids: []uint64{1, 2}, wantPending: []uint64{3, 4, 5, 6}},
		{name: "pending tail", ids: []uint64{5, 6}, wantPending: []uint64{1, 2, 3, 4}},
		{name: "mixed publication state", ids: []uint64{1, 2}, published: []uint64{1}, wantPending: []uint64{1, 2, 3, 4, 5, 6}, wantErr: ErrInternalState},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := benchmarkServiceWithPendingVectors(6)
			for _, id := range test.published {
				service.locations[id] = vectorLocation{}
			}

			version := documentVersion{firstVectorID: test.ids[0], vectorCount: len(test.ids)}
			gotPublished, err := service.discardSupersededVersion(version)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if !slices.Equal(gotPublished, test.wantPublished) {
				t.Fatalf("published IDs = %v, want %v", gotPublished, test.wantPublished)
			}
			gotPending := make([]uint64, len(service.pendingVectors))
			for i, item := range service.pendingVectors {
				gotPending[i] = item.row.VectorID
			}
			if !slices.Equal(gotPending, test.wantPending) {
				t.Fatalf("pending IDs = %v, want %v", gotPending, test.wantPending)
			}
		})
	}
}

func TestDiscardSupersededVersionRejectsMissingPendingRange(t *testing.T) {
	service := benchmarkServiceWithPendingVectors(6)
	service.pendingVectors[2].row.VectorID = 4
	before := append([]pendingVector(nil), service.pendingVectors...)

	_, err := service.discardSupersededVersion(documentVersion{firstVectorID: 2, vectorCount: 2})
	if !errors.Is(err, ErrInternalState) {
		t.Fatalf("error = %v, want %v", err, ErrInternalState)
	}
	if !reflect.DeepEqual(service.pendingVectors, before) {
		t.Fatalf("pending vectors changed after rejected range: %+v", service.pendingVectors)
	}
}

func TestCanceledFlushDoesNotPublishPendingBatch(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", "chunk", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := service.Flush(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush error = %v", err)
	}
	result, err := service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("canceled flush published rows: %+v", result.Hits)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	result, err = service.searchChunks(ctx, []float32{0, 0}, 1)
	if err != nil || len(result.Hits) != 1 {
		t.Fatalf("retry flush result = %+v, %v", result, err)
	}
}

func TestFlushRejectsPublicationAfterConcurrentMutation(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "doc-a", []EncodedChunk{testChunk("doc-a", "a", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	state, err := service.captureFlushState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.addEncodedDocument(ctx, "doc-b", []EncodedChunk{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	segment, err := buildPendingSegment(ctx, state.componentID, state.pendingVectors, state.config)
	if err != nil {
		t.Fatal(err)
	}
	published, locations, err := publishIndex(ctx, state.base, state.locations, state.disabledIDs, segment, state.componentID, state.revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.commitFlush(ctx, state, published, locations, segment != nil); !errors.Is(err, ErrPublicationConflict) {
		t.Fatalf("Flush error = %v, want %v", err, ErrPublicationConflict)
	}

	beforeRetry, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeRetry.Hits) != 0 {
		t.Fatalf("conflicting flush published stale view: %+v", beforeRetry.Hits)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	afterRetry, err := service.searchChunks(ctx, []float32{0, 0}, 2)
	if err != nil || len(afterRetry.Hits) != 2 {
		t.Fatalf("retry did not publish both pending mutations: %+v, %v", afterRetry, err)
	}
}

func TestCanceledFlushWaitDoesNotBlockOnAnotherPublication(t *testing.T) {
	service := newTestService(t)
	service.flushGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.Flush(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush error = %v", err)
	}
	<-service.flushGate
}

func TestCanceledOperationDoesNotWaitForStateLock(t *testing.T) {
	service := newTestService(t)
	service.lockStateUninterruptible()
	ctx, cancel := context.WithCancel(context.Background())
	searchResult := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))
		_, err := service.SearchDocuments(ctx, encoder, fts.Document{ID: "query"}, 1)
		searchResult <- err
	}()
	<-started
	cancel()
	select {
	case err := <-searchResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SearchDocuments error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SearchDocuments remained blocked after cancellation")
	}
	if err := service.DeleteDocument(ctx, "doc"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteDocument error = %v", err)
	}
	if err := service.Flush(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Flush error = %v", err)
	}
	service.unlockState()
	if err := service.lockFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	service.unlockFlush()
}

type cancelAfterErrContext struct {
	context.Context
	mu        sync.Mutex
	remaining int
	done      chan struct{}
	once      sync.Once
}

func newCancelAfterErrContext(parent context.Context, calls int) *cancelAfterErrContext {
	return &cancelAfterErrContext{Context: parent, remaining: calls, done: make(chan struct{})}
}

func (c *cancelAfterErrContext) Done() <-chan struct{} { return c.done }

func (c *cancelAfterErrContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.remaining--
	if c.remaining > 0 {
		return nil
	}
	c.once.Do(func() { close(c.done) })
	return context.Canceled
}

func TestCanceledCompactLeavesServiceUnchanged(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := replaceDocument(t, service, ctx, []EncodedChunk{testChunk("doc", "new", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	want := service.Statistics()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := service.Compact(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled compaction error = %v", err)
	}
	if got := service.Statistics(); got != want {
		t.Fatalf("canceled compaction changed service: got %+v, want %+v", got, want)
	}
}

func TestPreCanceledCompactAlwaysReturnsCancellation(t *testing.T) {
	ctx := context.Background()
	empty := newTestService(t)
	clean := newTestService(t)
	if err := addDocument(t, clean, ctx, []EncodedChunk{testChunk("doc", "chunk", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	pending := newTestService(t)
	if err := pending.addEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", "chunk", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	for name, service := range map[string]*Service{"empty": empty, "clean": clean, "pending": pending} {
		t.Run(name, func(t *testing.T) {
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := service.Compact(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("Compact error = %v", err)
			}
		})
	}
}

type blockingPreparedVectorStore struct {
	vectorstore.PreparedVectorStore
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingPreparedVectorStore) ReadVectorInto(ctx context.Context, ordinal vector.Ordinal, dst []float32) error {
	blocked := false
	s.once.Do(func() {
		blocked = true
		close(s.reached)
	})
	if blocked {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.PreparedVectorStore.ReadVectorInto(ctx, ordinal, dst)
}

func blockCompactionMaterialization(t *testing.T, service *Service) *blockingPreparedVectorStore {
	t.Helper()
	view := service.ReadView()
	if len(view.segments) < 2 {
		t.Fatalf("segments = %d, want at least two", len(view.segments))
	}
	store := &blockingPreparedVectorStore{
		PreparedVectorStore: view.segments[0].segment.vectorStore(),
		reached:             make(chan struct{}),
		release:             make(chan struct{}),
	}
	view.segments[0].segment.vectors = store
	return store
}

func TestCompactRejectsPublicationAfterConcurrentMutation(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	before := service.ReadView()
	store := blockCompactionMaterialization(t, service)
	result := make(chan error, 1)
	go func() { result <- service.Compact(ctx) }()
	<-store.reached

	encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))
	search, err := service.SearchDocuments(ctx, encoder, fts.Document{ID: "query"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(search.Hits) != 2 || search.Hits[0].DocID != "doc-a" || search.Hits[1].DocID != "doc-b" {
		t.Fatalf("search during compaction = %+v", search.Hits)
	}
	if err := service.addEncodedDocument(ctx, "doc-c", []EncodedChunk{testChunk("doc-c", "c", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	close(store.release)
	if err := <-result; !errors.Is(err, ErrPublicationConflict) {
		t.Fatalf("Compact error = %v, want %v", err, ErrPublicationConflict)
	}
	if got := service.ReadView(); got != before {
		t.Fatal("conflicting compaction replaced the published view")
	}
	if stats := service.Statistics(); stats.Documents != 3 || stats.LiveVectors != 3 {
		t.Fatalf("concurrent mutation was lost: %+v", stats)
	}
}

func TestDocumentValidationAndExplicitMutationErrors(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "", nil); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("empty batch error = %v", err)
	}
	mixed := []EncodedChunk{
		testChunk("doc-a", "one", 0, []float32{0, 0}),
		testChunk("doc-b", "two", 1, []float32{1, 0}),
	}
	if err := service.addEncodedDocument(ctx, "doc-a", mixed); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("mixed batch error = %v", err)
	}
	invalidRange := testChunk("doc-a", "bad-range", 0, []float32{0, 0})
	invalidRange.Ref.StartByte, invalidRange.Ref.EndByte = 10, 2
	if err := service.addEncodedDocument(ctx, "doc-a", []EncodedChunk{invalidRange}); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("range error = %v", err)
	}
	duplicateChunks := []EncodedChunk{
		testChunk("doc-a", "duplicate", 0, []float32{0, 0}),
		testChunk("doc-a", "duplicate", 1, []float32{1, 0}),
	}
	if err := service.addEncodedDocument(ctx, "doc-a", duplicateChunks); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("duplicate chunk error = %v", err)
	}
	valid := []EncodedChunk{testChunk("doc-a", "one", 0, []float32{0, 0})}
	if err := addDocument(t, service, ctx, valid); err != nil {
		t.Fatal(err)
	}
	if err := service.addEncodedDocument(ctx, "doc-a", valid); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("duplicate document error = %v", err)
	}
	if err := service.replaceEncodedDocument(ctx, "missing", []EncodedChunk{testChunk("missing", "one", 0, []float32{0, 0})}); !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("missing replacement error = %v", err)
	}
	if stats := service.Statistics(); stats.Documents != 1 || stats.PhysicalVectors != 1 || stats.MaxAllocatedVectorID != 1 {
		t.Fatalf("failed operations changed state: %+v", stats)
	}
}

func TestVectorIDsAreMonotonicAndExhaustionIsAtomic(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	deleteDocument(t, service, "doc-a")
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	if stats := service.Statistics(); stats.MaxAllocatedVectorID != 2 || stats.LiveVectors != 1 || stats.StaleVectors != 1 {
		t.Fatalf("statistics = %+v", stats)
	}

	config := testConfig(2, 2)
	config, err := config.normalized()
	if err != nil {
		t.Fatal(err)
	}
	exhausted, err := Hydrate(ctx, HydrationState{
		Config:               config,
		MaxAllocatedVectorID: uint64(math.MaxUint64 - 1),
		NextComponentID:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = exhausted.addEncodedDocument(ctx, "doc", []EncodedChunk{
		testChunk("doc", "one", 0, []float32{0, 0}),
		testChunk("doc", "two", 1, []float32{1, 0}),
	})
	if !errors.Is(err, ErrVectorIDExhausted) {
		t.Fatalf("exhaustion error = %v", err)
	}
	if stats := exhausted.Statistics(); stats.PhysicalVectors != 0 || stats.MaxAllocatedVectorID != uint64(math.MaxUint64-1) {
		t.Fatalf("exhausted state changed: %+v", stats)
	}
}

func TestReplacementUsesLiveCapacity(t *testing.T) {
	config := testConfig(2, 2)
	config.Limits.MaxLiveVectors = 2
	config.Limits.MaxChunksPerDocument = 2
	config.Limits.MaxChunksPerDocumentHit = 2
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	err = replaceDocument(t, service, ctx, []EncodedChunk{
		testChunk("doc", "new-1", 0, []float32{5, 0}),
		testChunk("doc", "new-2", 1, []float32{6, 0}),
	})
	if err != nil {
		t.Fatalf("replacement error = %v", err)
	}
	result, err := service.searchChunks(ctx, []float32{5, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 2 || result.Hits[0].Ref.ID != "new-1" {
		t.Fatalf("replacement result = %+v", result.Hits)
	}
	if stats := service.Statistics(); stats.PhysicalVectors != 3 || stats.LiveVectors != 2 || stats.StaleVectors != 1 || stats.MaxAllocatedVectorID != 3 {
		t.Fatalf("replacement statistics = %+v", stats)
	}
}

func TestWholeDocumentAndGroupingBudget(t *testing.T) {
	config := testConfig(1, 1)
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	whole, err := chunk.Whole("doc-a", fts.DefaultField, "short text")
	if err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, context.Background(), []EncodedChunk{{Ref: whole.Ref, Vector: []float32{0, 0}}}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, context.Background(), []EncodedChunk{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}
	result, err := service.searchEncodedDocuments(context.Background(), []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].Chunks[0].Ref.ID != chunk.WholeID || !result.GroupingIncomplete || result.CandidateChunks != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestCompleteGroupingUsesAllCandidateChunks(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{
		testChunk("doc-a", "a-1", 0, []float32{0, 0}),
		testChunk("doc-a", "a-2", 1, []float32{1, 0}),
		testChunk("doc-a", "a-3", 2, []float32{2, 0}),
	}); err != nil {
		t.Fatal(err)
	}
	for i, docID := range []fts.DocID{"doc-b", "doc-c", "doc-d"} {
		if err := addDocument(t, service, ctx, []EncodedChunk{testChunk(docID, "whole", 0, []float32{float32(i + 3), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupingIncomplete || result.CandidateChunks != 6 || result.DistinctDocuments != 4 {
		t.Fatalf("grouping result = %+v", result)
	}
	if result.Stats.VisitedNodes != 6 || result.Stats.DistanceComputations != 6 {
		t.Fatalf("complete stats = %+v, want six candidate distances", result.Stats)
	}
}

func TestDocumentGroupingCandidateBudgetMayExceedPublicMaxK(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 11 {
		docID := fts.DocID(fmt.Sprintf("doc-%02d", i))
		if err := addDocument(t, service, ctx, []EncodedChunk{testChunk(docID, "whole", 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := service.searchEncodedDocuments(ctx, []float32{0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupingIncomplete || result.CandidateChunks != 11 || result.DistinctDocuments != 11 {
		t.Fatalf("grouping result = %+v", result)
	}
	if _, err := service.searchChunks(ctx, []float32{0, 0}, 11); !errors.Is(err, vector.ErrInvalidK) {
		t.Fatalf("public chunk search error = %v", err)
	}
}

func TestDocumentSearchCandidateBudgetIsRequestScoped(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	for i := range 4 {
		docID := fts.DocID(fmt.Sprintf("doc-%d", i))
		if err := addDocument(t, service, ctx, []EncodedChunk{testChunk(docID, "whole", 0, []float32{float32(i), 0})}); err != nil {
			t.Fatal(err)
		}
	}

	result, err := service.searchEncodedDocumentsWithOptions(ctx, []float32{0, 0}, 2, SearchOptions{CandidateChunks: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks != 2 || result.DistinctDocuments != 2 || !result.GroupingIncomplete {
		t.Fatalf("limited grouping result = %+v", result)
	}
	if len(result.Hits) != 2 || result.Hits[0].DocID != "doc-0" || result.Hits[1].DocID != "doc-1" {
		t.Fatalf("limited grouping hits = %+v", result.Hits)
	}

	result, err = service.searchEncodedDocumentsWithOptions(ctx, []float32{0, 0}, 2, SearchOptions{CandidateChunks: 4})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks != 4 || result.DistinctDocuments != 4 || result.GroupingIncomplete {
		t.Fatalf("expanded grouping result = %+v", result)
	}
}

func TestDocumentSearchClampsCandidateBudgetToConfiguredMaximum(t *testing.T) {
	service := newTestService(t)
	if err := addDocument(t, service, context.Background(), []EncodedChunk{testChunk("doc", "whole", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	result, err := service.searchEncodedDocumentsWithOptions(context.Background(), []float32{0, 0}, 1, SearchOptions{CandidateChunks: 101})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateChunks != 1 || result.GroupingIncomplete {
		t.Fatalf("clamped grouping result = %+v", result)
	}
}

func TestConcurrentSameDocumentAddAndSearchReplace(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	const writers = 8
	var wg sync.WaitGroup
	errorsCh := make(chan error, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errorsCh <- service.addEncodedDocument(ctx, "shared", []EncodedChunk{testChunk("shared", chunk.ID("version"), 0, []float32{float32(i), 0})})
		}(i)
	}
	wg.Wait()
	close(errorsCh)
	successes := 0
	for err := range errorsCh {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrDocumentExists) {
			t.Fatalf("AddDocument error = %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful adds = %d, want 1", successes)
	}

	searchErrors := make(chan error, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 20 {
			_, err := service.searchChunks(ctx, []float32{0, 0}, 1)
			if err != nil {
				searchErrors <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 20 {
			if err := service.replaceEncodedDocument(ctx, "shared", []EncodedChunk{testChunk("shared", "version", 0, []float32{float32(i), 0})}); err != nil {
				searchErrors <- err
				return
			}
		}
	}()
	wg.Wait()
	close(searchErrors)
	for err := range searchErrors {
		t.Fatal(err)
	}
	if stats := service.Statistics(); stats.Documents != 1 || stats.LiveVectors != 1 || stats.PhysicalVectors != 1 {
		t.Fatalf("concurrent statistics = %+v", stats)
	}
}

func TestConcurrentSearchAcrossFlushPublications(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc", "chunk-0", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	errCh := make(chan error, 4)
	var wait sync.WaitGroup
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				result, err := service.searchChunks(ctx, []float32{0, 0}, 1)
				if err != nil || len(result.Hits) != 1 || result.Hits[0].Ref.DocID != "doc" {
					errCh <- fmt.Errorf("incoherent search result: %+v: %v", result, err)
					return
				}
			}
		}()
	}
	for version := 1; version <= 20; version++ {
		if err := service.replaceEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", chunk.ID(fmt.Sprintf("chunk-%d", version)), 0, []float32{float32(version), 0})}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wait.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestOldReadViewRemainsCoherentAfterCompaction(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := replaceDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a-new", 0, []float32{2, 0})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "b", 0, []float32{1, 0})}); err != nil {
		t.Fatal(err)
	}

	oldView := service.ReadView()
	oldRevision := oldView.revision
	oldSegments := append([]visibleSegment(nil), oldView.segments...)
	encoder := newSearchEncoder(service.config, testChunk("query", "query", 0, []float32{0, 0}))
	before, err := oldView.SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 2, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	after, err := oldView.SearchDocumentsWithOptions(ctx, encoder, fts.Document{ID: "query"}, 2, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if oldView.revision != oldRevision || len(oldView.segments) != len(oldSegments) {
		t.Fatalf("old view metadata changed: revision=%d segments=%d", oldView.revision, len(oldView.segments))
	}
	for i, segment := range oldSegments {
		if oldView.segments[i].segment != segment.segment ||
			!slices.Equal(oldView.segments[i].filter.SnapshotWords(), segment.filter.SnapshotWords()) {
			t.Fatalf("old view segment %d changed", i)
		}
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("old view search changed after compaction\nbefore=%+v\nafter=%+v", before, after)
	}
	current := service.ReadView()
	if current == oldView || len(current.segments) != 1 || current.liveCount != oldView.liveCount {
		t.Fatalf("compacted view is incoherent: old=%p current=%p segments=%d live=%d", oldView, current, len(current.segments), current.liveCount)
	}
}

func TestCosineCompactionPreservesPreparedVectorBits(t *testing.T) {
	config := testConfig(10, 100)
	config.Embedding.Metric = vector.MetricCosine
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-a", "a", 0, []float32{3, 4})}); err != nil {
		t.Fatal(err)
	}
	if err := addDocument(t, service, ctx, []EncodedChunk{testChunk("doc-b", "b", 0, []float32{5, 12})}); err != nil {
		t.Fatal(err)
	}
	want := preparedVectorBits(t, service.ReadView())
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	got := preparedVectorBits(t, service.ReadView())
	if len(got) != len(want) {
		t.Fatalf("prepared rows = %d, want %d", len(got), len(want))
	}
	for id, bits := range want {
		if !slices.Equal(got[id], bits) {
			t.Fatalf("vector %d bits changed: got %v, want %v", id, got[id], bits)
		}
	}
}

func preparedVectorBits(t *testing.T, view *ReadView) map[uint64][]uint32 {
	t.Helper()
	result := make(map[uint64][]uint32)
	for _, item := range view.segments {
		for ordinal, row := range item.segment.rows {
			if !item.filter.Allows(vector.Ordinal(ordinal)) {
				continue
			}
			value := make([]float32, item.segment.dimensions())
			if err := item.segment.vectorStore().ReadVectorInto(context.Background(), vector.Ordinal(ordinal), value); err != nil {
				t.Fatal(err)
			}
			bits := make([]uint32, len(value))
			for i, component := range value {
				bits[i] = math.Float32bits(component)
			}
			result[row.VectorID] = bits
		}
	}
	return result
}

func TestConcurrentReplaceAndDeleteAreLinearizable(t *testing.T) {
	service := newTestService(t)
	ctx := context.Background()
	if err := service.addEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", "old", 0, []float32{0, 0})}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	replaceResult := make(chan error, 1)
	deleteResult := make(chan error, 1)
	go func() {
		<-start
		replaceResult <- service.replaceEncodedDocument(ctx, "doc", []EncodedChunk{testChunk("doc", "new", 0, []float32{1, 0})})
	}()
	go func() {
		<-start
		deleteResult <- service.DeleteDocument(ctx, "doc")
	}()
	close(start)
	replaceErr := <-replaceResult
	deleteErr := <-deleteResult
	if deleteErr != nil {
		t.Fatalf("DeleteDocument error = %v", deleteErr)
	}
	if replaceErr != nil && !errors.Is(replaceErr, ErrDocumentNotFound) {
		t.Fatalf("ReplaceDocument error = %v", replaceErr)
	}
	if stats := service.Statistics(); stats.Documents != 0 || stats.LiveVectors != 0 {
		t.Fatalf("final state is not deleted: %+v", stats)
	}
}
