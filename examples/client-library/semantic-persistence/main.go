package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func main() {
	ctx := context.Background()
	root, err := os.MkdirTemp("", "fts-semantic-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(root)

	service, err := semantic.New(semantic.Config{
		Space: semantic.SpaceDescriptor{
			ID:                  "example-embedding-v1",
			Dimensions:          2,
			Metric:              vector.MetricL2Squared,
			Normalization:       vector.NormalizationNone,
			VectorFormatVersion: 1,
		},
		Chunking:                semantic.ChunkingDescriptor{ID: "example-chunks-v1"},
		MaxVectors:              100,
		MaxChunksPerDocument:    10,
		MaxK:                    10,
		MaxChunkCandidates:      100,
		MaxChunksPerDocumentHit: 3,
	})
	if err != nil {
		panic(err)
	}

	// Embeddings are produced by the caller or an external model.
	must(service.AddDocument(ctx, []semantic.ChunkVector{chunkVector("doc-a", "a-v1", []float32{0, 0})}))
	must(service.AddDocument(ctx, []semantic.ChunkVector{chunkVector("doc-b", "b-v1", []float32{5, 0})}))
	must(service.ReplaceDocument(ctx, []semantic.ChunkVector{chunkVector("doc-a", "a-v2", []float32{10, 0})}))

	// Create an immutable HNSW snapshot containing only active vectors.
	snapshot, err := service.Snapshot(ctx)
	must(err)
	generation, err := semanticpersist.Publish(ctx, root, 1, snapshot, semanticpersist.Options{
		ExpectedGeneration: 0,
		Durability:         semanticpersist.DurabilitySynchronous,
	})
	must(err)

	loaded, err := semanticpersist.Open(root, semanticpersist.DefaultLimits())
	must(err)
	defer loaded.Close()
	result, err := loaded.Snapshot.SearchDocuments(ctx, []float32{0, 0}, 2)
	must(err)

	fmt.Printf("generation=%d kind=%d rows=%d\n", generation.ID, loaded.Snapshot.Segment.Kind(), len(loaded.Snapshot.Segment.Rows()))
	for _, hit := range result.Hits {
		fmt.Printf("doc=%s distance=%.0f\n", hit.DocID, hit.Distance)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func chunkVector(docID fts.DocID, chunkID chunk.ID, embedding []float32) semantic.ChunkVector {
	return semantic.ChunkVector{
		Ref:    chunk.Ref{ID: chunkID, DocID: docID, Field: fts.DefaultField, EndByte: 10},
		Vector: embedding,
	}
}
