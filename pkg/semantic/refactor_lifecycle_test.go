package semantic_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestCompactAfterDeletingLastDocument(t *testing.T) {
	ctx := context.Background()
	service, encoder := newLifecycleService(t)
	encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", "same", []float32{1, 0})}

	if err := service.AddDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteDocument(ctx, "doc"); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	stats := service.Statistics()
	if stats.Documents != 0 || stats.LiveVectors != 0 || stats.PhysicalVectors != 0 {
		t.Fatalf("unexpected stats after empty compaction: %+v", stats)
	}
}

func TestRestoreAllowsStaleChunkIdentityAfterReplace(t *testing.T) {
	ctx := context.Background()
	service, encoder := newLifecycleService(t)
	encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", "same", []float32{1, 0})}

	if err := service.AddDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Reusing the same logical chunk identity is valid: the first physical row
	// becomes stale and the replacement is the only live row.
	encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", "same", []float32{2, 0})}
	if err := service.ReplaceDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	state, err := service.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = semantic.Open(ctx, *state)
	if err != nil {
		t.Fatalf("restore rejected valid stale chunk identity: %v", err)
	}
}

func TestAddThenDeleteBeforeFlushStillCommitsRevision(t *testing.T) {
	ctx := context.Background()
	service, encoder := newLifecycleService(t)
	encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", "same", []float32{1, 0})}

	if err := service.AddDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteDocument(ctx, "doc"); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.State(ctx); err != nil {
		t.Fatalf("empty net mutation remained pending after flush: %v", err)
	}
}

func TestLifecycleLimitDefaultsAndCombinedCapacity(t *testing.T) {
	service, _ := newLifecycleService(t)
	state, err := service.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Config.Limits.MaxStaleVectors != state.Config.Limits.MaxLiveVectors {
		t.Fatalf("MaxStaleVectors = %d, want %d", state.Config.Limits.MaxStaleVectors, state.Config.Limits.MaxLiveVectors)
	}
	if state.Config.Limits.MaxSegments != 16 {
		t.Fatalf("MaxSegments = %d, want 16", state.Config.Limits.MaxSegments)
	}

	config := state.Config
	config.Limits.MaxLiveVectors = math.MaxInt
	config.Limits.MaxStaleVectors = 1
	if _, err := semantic.New(config); !errors.Is(err, semantic.ErrInvalidConfig) {
		t.Fatalf("New combined capacity error = %v, want ErrInvalidConfig", err)
	}
}

func TestFlushAutomaticallyCompactsSegmentBoundWithoutExtraID(t *testing.T) {
	ctx := context.Background()
	service, encoder := newLifecycleServiceWithLimits(t, 8, 8, 2)
	for n, docID := range []fts.DocID{"one", "two", "three"} {
		encoder.vectors[docID] = []semantic.EncodedChunk{lifecycleChunk(docID, chunk.ID(docID), []float32{float32(n), 0})}
		if err := service.AddDocument(ctx, encoder, fts.Document{ID: docID}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	state, err := service.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Segments) != 1 || state.NextSegmentID != 4 {
		t.Fatalf("segments = %d, next segment ID = %d; want 1 and 4", len(state.Segments), state.NextSegmentID)
	}
	if stats := service.Statistics(); stats.PhysicalVectors != 3 || stats.StaleVectors != 0 {
		t.Fatalf("unexpected compacted stats: %+v", stats)
	}
}

func TestFlushAutomaticallyCompactsStaleBound(t *testing.T) {
	ctx := context.Background()
	service, encoder := newLifecycleServiceWithLimits(t, 8, 1, 16)
	encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", "v0", []float32{0, 0})}
	if err := service.AddDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for n, id := range []chunk.ID{"v1", "v2"} {
		encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", id, []float32{float32(n + 1), 0})}
		if err := service.ReplaceDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}

	state, err := service.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Segments) != 1 || state.NextSegmentID != 4 {
		t.Fatalf("segments = %d, next segment ID = %d; want 1 and 4", len(state.Segments), state.NextSegmentID)
	}
	if stats := service.Statistics(); stats.PhysicalVectors != 1 || stats.StaleVectors != 0 {
		t.Fatalf("unexpected compacted stats: %+v", stats)
	}
}

func TestOpenEnforcesLifecycleBounds(t *testing.T) {
	ctx := context.Background()
	service, encoder := newLifecycleServiceWithLimits(t, 8, 8, 8)
	for _, docID := range []fts.DocID{"one", "two"} {
		encoder.vectors[docID] = []semantic.EncodedChunk{lifecycleChunk(docID, chunk.ID(docID), []float32{1, 0})}
		if err := service.AddDocument(ctx, encoder, fts.Document{ID: docID}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	state, err := service.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state.Config.Limits.MaxSegments = 1
	if _, err := semantic.Open(ctx, *state); !errors.Is(err, semantic.ErrCapacityExceeded) {
		t.Fatalf("Open segment bound error = %v, want ErrCapacityExceeded", err)
	}

	service, encoder = newLifecycleServiceWithLimits(t, 8, 8, 8)
	encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", "v0", []float32{0, 0})}
	if err := service.AddDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []chunk.ID{"v1", "v2"} {
		encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", id, []float32{1, 0})}
		if err := service.ReplaceDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
			t.Fatal(err)
		}
		if err := service.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	state, err = service.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state.Config.Limits.MaxStaleVectors = 1
	if _, err := semantic.Open(ctx, *state); !errors.Is(err, semantic.ErrCapacityExceeded) {
		t.Fatalf("Open stale bound error = %v, want ErrCapacityExceeded", err)
	}
}

func newLifecycleService(t *testing.T) (*semantic.Service, *differentialEncoder) {
	return newLifecycleServiceWithLimits(t, 32, 0, 0)
}

func newLifecycleServiceWithLimits(t *testing.T, maxLive, maxStale, maxSegments int) (*semantic.Service, *differentialEncoder) {
	t.Helper()
	embedding, err := semantic.NewEmbeddingDescriptor("test", "model", "v1", "pipeline-v1", 2, vector.MetricL2Squared, 1)
	if err != nil {
		t.Fatal(err)
	}
	config := semantic.Config{
		Schema: semantic.Schema{
			Embedding: embedding,
			Chunking:  semantic.ChunkingDescriptor{ID: "chunks", Version: 1, Fingerprint: "chunks-v1"},
		},
		Limits: semantic.Limits{
			MaxLiveVectors:          maxLive,
			MaxStaleVectors:         maxStale,
			MaxSegments:             maxSegments,
			MaxChunksPerDocument:    4,
			MaxDocumentsPerSearch:   4,
			MaxChunkCandidates:      maxLive,
			MaxChunksPerDocumentHit: 2,
		},
	}
	service, err := semantic.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return service, &differentialEncoder{
		descriptor: semantic.Schema{Embedding: embedding, Chunking: config.Schema.Chunking},
		vectors:    make(map[fts.DocID][]semantic.EncodedChunk),
	}
}

func lifecycleChunk(docID fts.DocID, id chunk.ID, value []float32) semantic.EncodedChunk {
	return semantic.EncodedChunk{
		Ref:    chunk.Ref{ID: id, DocID: docID, Field: fts.DefaultField, EndByte: 1},
		Vector: value,
	}
}
