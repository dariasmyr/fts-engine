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
		Operational: semantic.OperationalLimits{
			Capacity:  semantic.CapacityLimits{MaxLiveVectors: 16},
			Ingestion: semantic.IngestionLimits{MaxChunksPerDocument: 2},
			Search:    semantic.SearchLimits{MaxResults: 4, MaxCandidateChunks: 16, MaxChunksPerResult: 2},
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

func TestCompactSaveLoadContinuesWriting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	config := semantic.IndexConfig{
		Dimensions: 2,
		Metric:     vector.MetricL2Squared,
		Operational: semantic.OperationalLimits{
			Capacity:  semantic.CapacityLimits{MaxLiveVectors: 16, MaxStaleVectors: 16, MaxSegments: 8},
			Ingestion: semantic.IngestionLimits{MaxChunksPerDocument: 2},
			Search: semantic.SearchLimits{
				MaxResults:         4,
				MaxCandidateChunks: 16,
				MaxChunksPerResult: 2,
			},
		},
	}
	index, err := semantic.NewIndex(config)
	if err != nil {
		t.Fatal(err)
	}
	add := func(docID fts.DocID, values []float32) {
		t.Helper()
		err := index.AddDocument(ctx, docID, []semantic.ChunkEmbedding{{
			Ref: semantic.Ref{
				ID:      semantic.ChunkID(docID + "-chunk"),
				DocID:   docID,
				Field:   fts.DefaultField,
				EndByte: 1,
			},
			Embedding: values,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	add("replace", []float32{0, 0})
	add("delete", []float32{10, 0})
	if err := index.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := index.ReplaceDocument(ctx, "replace", []semantic.ChunkEmbedding{{
		Ref: semantic.Ref{
			ID: "replace-new", DocID: "replace", Field: fts.DefaultField, EndByte: 1,
		},
		Embedding: []float32{20, 0},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := index.DeleteDocument(ctx, "delete"); err != nil {
		t.Fatal(err)
	}
	if err := index.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := index.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	generation, err := semanticpersist.Save(ctx, root, index, semanticpersist.SaveConfig{
		ExpectedGeneration: &semanticpersist.Generation{},
	})
	if err != nil {
		t.Fatal(err)
	}
	loaded, loadedGeneration, err := semanticpersist.Load(ctx, root, semanticpersist.LoadConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if loadedGeneration != generation {
		t.Fatalf("Load() generation = %v, want %v", loadedGeneration, generation)
	}
	if got := loaded.Statistics(); got.Documents != 1 || got.PhysicalVectors != 1 ||
		got.LiveVectors != 1 || got.StaleVectors != 0 || got.MaxAllocatedVectorID != 3 {
		t.Fatalf("loaded compacted statistics = %+v", got)
	}
	result, err := loaded.Search(ctx, []semantic.ChunkEmbedding{{Embedding: []float32{20, 0}}}, 2, semantic.QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != "replace" {
		t.Fatalf("loaded compacted hits = %+v, want replace", result.Hits)
	}

	index = loaded
	add("continued", []float32{30, 0})
	nextGeneration, err := semanticpersist.CommitAndSave(ctx, root, loaded, semanticpersist.SaveConfig{
		ExpectedGeneration: &loadedGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	reopened, reopenedGeneration, err := semanticpersist.Load(ctx, root, semanticpersist.LoadConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if reopenedGeneration != nextGeneration {
		t.Fatalf("reopened generation = %v, want %v", reopenedGeneration, nextGeneration)
	}
	if got := reopened.Statistics(); got.Documents != 2 || got.LiveVectors != 2 || got.MaxAllocatedVectorID != 4 {
		t.Fatalf("continued statistics = %+v", got)
	}
	result, err = reopened.Search(ctx, []semantic.ChunkEmbedding{{Embedding: []float32{30, 0}}}, 1, semantic.QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != "continued" {
		t.Fatalf("continued hits = %+v, want continued", result.Hits)
	}
}
