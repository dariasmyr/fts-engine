package semanticpersist_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestSaveLoadRestoresHNSWSearch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	config := semantic.IndexConfig{
		Dimensions: 2,
		Metric:     vector.MetricL2Squared,
		Limits:     semantic.LimitsConfig{MaxLiveVectors: 16, MaxChunksPerDocument: 2},
		Search:     semantic.SearchConfig{MaxResults: 4, MaxCandidateChunks: 16, MaxChunksPerResult: 2},
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
		err := index.AddDocument(ctx, item.docID, []semantic.ChunkEmbedding{{
			Ref: semantic.Ref{
				ID:      semantic.ChunkID(item.docID + "-chunk"),
				DocID:   item.docID,
				Field:   fts.DefaultField,
				EndByte: 1,
			},
			Embedding: item.vector,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	query := []semantic.ChunkEmbedding{{Embedding: []float32{0.1, 0}}}
	want, err := index.Search(ctx, query, 2, semantic.QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	generation, err := semanticpersist.Save(ctx, root, index, semanticpersist.SaveConfig{
		ExpectedGeneration: &semanticpersist.Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if generation.ID != 1 {
		t.Fatalf("generation ID = %d, want 1", generation.ID)
	}

	loaded, loadedGeneration, err := semanticpersist.Load(ctx, root, semanticpersist.LoadConfig{
		Config: semanticpersist.DefaultConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := loaded.Search(ctx, query, 2, semantic.QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored search result = %+v, want %+v", got, want)
	}
	next, err := semanticpersist.Save(ctx, root, loaded, semanticpersist.SaveConfig{
		ExpectedGeneration: &loadedGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.ID != 2 {
		t.Fatalf("Save() generation = %d, want 2", next.ID)
	}
}
