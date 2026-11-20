package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// This example uses semantic.Index directly. The caller owns chunking and
// embedding and passes ready-to-index vectors to the low-level API.
func main() {
	ctx := context.Background()
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
		chunkEmbedding("hotel", "body/chunk-000000", 0, 24, []float32{1, 0}),
	}))
	must(index.AddDocument(ctx, "boats", []semantic.ChunkEmbedding{
		chunkEmbedding("boats", "body/chunk-000000", 0, 20, []float32{0, 1}),
	}))
	must(index.Commit(ctx))

	result, err := index.Search(
		ctx,
		[]semantic.ChunkEmbedding{{Embedding: []float32{1, 0}}},
		2,
		semantic.QueryConfig{},
	)
	must(err)
	for _, hit := range result.Hits {
		fmt.Printf("doc=%s distance=%.1f\n", hit.DocID, hit.Distance)
	}
}

func chunkEmbedding(
	docID fts.DocID,
	chunkID semantic.ChunkID,
	startByte uint64,
	endByte uint64,
	embedding []float32,
) semantic.ChunkEmbedding {
	return semantic.ChunkEmbedding{
		Ref: semantic.Ref{
			ID: chunkID, DocID: docID, Field: "body",
			StartByte: startByte, EndByte: endByte,
		},
		Embedding: embedding,
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
