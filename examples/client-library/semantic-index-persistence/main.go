package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// This example persists a raw semantic.Index. The caller owns chunking and
// embedding, so only IndexConfig and prepared vectors are saved.
func main() {
	ctx := context.Background()
	root, err := os.MkdirTemp("", "fts-semantic-index-persistence-")
	must(err)
	defer os.RemoveAll(root)

	index, err := semantic.NewIndex(semantic.IndexConfig{
		Dimensions: 2,
		Metric:     vector.MetricCosine,
		Operational: semantic.OperationalLimits{
			Capacity:  semantic.CapacityLimits{MaxLiveVectors: 100, MaxStaleVectors: 100},
			Ingestion: semantic.IngestionLimits{MaxChunksPerDocument: 10},
			Search:    semantic.SearchLimits{MaxResults: 10, MaxCandidateChunks: 100, MaxChunksPerResult: 3},
		},
	})
	must(err)

	must(index.AddDocument(ctx, "hotel", []semantic.ChunkEmbedding{
		chunkEmbedding("hotel", []float32{1, 0}),
	}))
	must(index.AddDocument(ctx, "boats", []semantic.ChunkEmbedding{
		chunkEmbedding("boats", []float32{0, 1}),
	}))

	generation, err := semanticpersist.CommitAndSave(
		ctx,
		root,
		index,
		semanticpersist.SaveOptions{
			ExpectedGeneration: &semanticpersist.Generation{},
		},
	)
	must(err)
	fmt.Printf("saved generation=%d\n", generation.ID)

	loaded, loadedGeneration, err := semanticpersist.Load(
		ctx,
		root,
		semanticpersist.LoadOptions{},
	)
	must(err)
	fmt.Printf("loaded generation=%d\n", loadedGeneration.ID)

	result, err := loaded.Search(
		ctx,
		[]semantic.ChunkEmbedding{{Embedding: []float32{1, 0}}},
		2,
		semantic.QueryConfig{
			EfSearch:        16,
			VisitLimit:      100,
			CandidateChunks: 10,
		},
	)
	must(err)
	for _, hit := range result.Hits {
		fmt.Printf("doc=%s distance=%.1f\n", hit.DocID, hit.Distance)
	}
}

func chunkEmbedding(docID fts.DocID, embedding []float32) semantic.ChunkEmbedding {
	return semantic.ChunkEmbedding{
		Ref: semantic.Ref{
			ID: "whole", DocID: docID, Field: "body", EndByte: 1,
		},
		Embedding: embedding,
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
