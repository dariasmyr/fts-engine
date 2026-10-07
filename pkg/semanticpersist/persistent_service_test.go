package semanticpersist_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func TestPublishOpenRestoresHNSWSearch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	embedding, err := semantic.NewEmbeddingDescriptor(
		"test",
		"model",
		"v1",
		"pipeline-v1",
		2,
		vector.MetricL2Squared,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	schema := semantic.Schema{
		Embedding: embedding,
		Chunking: semantic.ChunkingDescriptor{
			ID:          "chunks",
			Version:     1,
			Fingerprint: "chunks-v1",
		},
	}
	config := semantic.Config{
		Schema: schema,
		Limits: semantic.Limits{
			MaxLiveVectors:          16,
			MaxChunksPerDocument:    2,
			MaxDocumentsPerSearch:   4,
			MaxChunkCandidates:      16,
			MaxChunksPerDocumentHit: 2,
		},
	}
	index, err := semantic.NewIndex(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		docID  fts.DocID
		vector []float32
	}{
		{docID: "near", vector: []float32{0, 0}},
		{docID: "far", vector: []float32{10, 0}},
	} {
		err := index.Add(ctx, item.docID, []semantic.EncodedChunk{{
			Ref: chunk.Ref{
				ID:      chunk.ID(item.docID + "-chunk"),
				DocID:   item.docID,
				Field:   fts.DefaultField,
				EndByte: 1,
			},
			Vector: item.vector,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	query := []semantic.EncodedChunk{{Vector: []float32{0.1, 0}}}
	want, err := index.Search(ctx, query, 2, semantic.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	generation, err := semanticpersist.Publish(ctx, root, index, semanticpersist.PublishOptions{
		ExpectedGeneration: &semanticpersist.Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if generation.ID != 1 {
		t.Fatalf("generation ID = %d, want 1", generation.ID)
	}

	store, err := semanticpersist.Open(ctx, root, semanticpersist.OpenOptions{
		Limits:         semanticpersist.DefaultLimits(),
		ExpectedSchema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	got, err := store.Index().Search(ctx, query, 2, semantic.SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored search result = %+v, want %+v", got, want)
	}
}
