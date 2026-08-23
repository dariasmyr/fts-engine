package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func main() {
	search, err := semantic.New(semantic.Config{
		Space: semantic.SpaceDescriptor{
			ID:                  "example-embedding-v1",
			Dimensions:          3,
			Metric:              vector.MetricCosine,
			Normalization:       vector.NormalizationUnitLength,
			VectorFormatVersion: 1,
		},
		Chunking:                semantic.ChunkingDescriptor{ID: "caller-chunks-v1"},
		MaxVectors:              1_000,
		MaxChunksPerDocument:    32,
		MaxK:                    10,
		MaxChunkCandidates:      100,
		MaxChunksPerDocumentHit: 3,
		InitialVectorCapacity:   64,
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := search.AddDocument(ctx, []semantic.ChunkVector{
		{
			Ref:    chunk.Ref{ID: "intro", DocID: "doc-hotel", Field: fts.DefaultField, Ordinal: 0, StartByte: 0, EndByte: 18},
			Vector: []float32{1, 0, 0},
		},
		{
			Ref:    chunk.Ref{ID: "location", DocID: "doc-hotel", Field: fts.DefaultField, Ordinal: 1, StartByte: 19, EndByte: 36},
			Vector: []float32{0.8, 0.2, 0},
		},
	}); err != nil {
		panic(err)
	}

	whole, err := chunk.Whole("doc-boat", fts.DefaultField, "A cargo boat on the river")
	if err != nil {
		panic(err)
	}
	if err := search.AddDocument(ctx, []semantic.ChunkVector{{Ref: whole.Ref, Vector: []float32{0, 1, 0}}}); err != nil {
		panic(err)
	}

	result, err := search.SearchDocuments(ctx, []float32{1, 0.1, 0}, 2)
	if err != nil {
		panic(err)
	}
	for _, hit := range result.Hits {
		fmt.Printf("doc=%s distance=%.4f best_chunk=%s\n", hit.DocID, hit.Distance, hit.Chunks[0].Ref.ID)
	}
}
