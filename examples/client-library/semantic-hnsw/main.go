package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticencode"
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
	encoder, err := semanticencode.New(nil, exampleEmbedder{})
	if err != nil {
		panic(err)
	}
	if err := search.AddDocument(ctx, encoder, semantic.Document{ID: "doc-hotel", Fields: map[string]string{"body": "hotel"}}); err != nil {
		panic(err)
	}
	if err := search.AddDocument(ctx, encoder, semantic.Document{ID: "doc-boat", Fields: map[string]string{"body": "boat"}}); err != nil {
		panic(err)
	}
	if err := search.Flush(ctx); err != nil {
		panic(err)
	}

	result, err := search.SearchDocuments(ctx, encoder, semantic.Document{ID: "query", Fields: map[string]string{"body": "hotel"}}, 2)
	if err != nil {
		panic(err)
	}
	for _, hit := range result.Hits {
		fmt.Printf("doc=%s distance=%.4f best_chunk=%s\n", hit.DocID, hit.Distance, hit.Chunks[0].Ref.ID)
	}
}

type exampleEmbedder struct{}

func (exampleEmbedder) Embed(_ context.Context, chunks []chunk.Chunk) ([][]float32, error) {
	result := make([][]float32, len(chunks))
	for i, item := range chunks {
		if item.Text == "boat" {
			result[i] = []float32{0, 1, 0}
		} else {
			result[i] = []float32{1, 0, 0}
		}
	}
	return result, nil
}
