package semantic_test

import (
	"context"
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

func newLifecycleService(t *testing.T) (*semantic.Service, *differentialEncoder) {
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
			MaxLiveVectors:          32,
			MaxChunksPerDocument:    4,
			MaxDocumentsPerSearch:   4,
			MaxChunkCandidates:      32,
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
