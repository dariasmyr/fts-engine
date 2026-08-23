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

	// These toy vectors stand in for embeddings produced by an external model.
	if err := service.AddDocument(ctx, []semantic.ChunkVector{chunkVector("doc-a", "a-v1", []float32{0, 0})}); err != nil {
		panic(err)
	}
	if err := service.AddDocument(ctx, []semantic.ChunkVector{chunkVector("doc-b", "b-v1", []float32{5, 0})}); err != nil {
		panic(err)
	}

	// A checkpoint freezes vectors, liveness, mappings, and descriptors as one
	// coherent generation input.
	checkpoint1, err := service.Checkpoint()
	if err != nil {
		panic(err)
	}
	generation1, err := semanticpersist.Publish(ctx, root, 1, checkpoint1, semanticpersist.Options{
		ExpectedGeneration: 0,
		Durability:         semanticpersist.DurabilitySynchronous,
	})
	if err != nil {
		panic(err)
	}

	openedID, bestDoc, err := openAndSearch(ctx, root, []float32{0, 0})
	if err != nil {
		panic(err)
	}
	fmt.Printf("published=%d opened=%d best=%s\n", generation1.ID, openedID, bestDoc)

	// Replace appends a new physical vector and marks doc-a's previous vector
	// stale. Nothing is durable until the next checkpoint is published.
	if err := service.ReplaceDocument(ctx, []semantic.ChunkVector{chunkVector("doc-a", "a-v2", []float32{10, 0})}); err != nil {
		panic(err)
	}
	checkpoint2, err := service.Checkpoint()
	if err != nil {
		panic(err)
	}
	generation2, err := semanticpersist.Publish(ctx, root, 2, checkpoint2, semanticpersist.Options{
		// Reject this publication if another writer changed CURRENT after generation 1.
		ExpectedGeneration: generation1.ID,
		Durability:         semanticpersist.DurabilitySynchronous,
	})
	if err != nil {
		panic(err)
	}

	openedID, bestDoc, err = openAndSearch(ctx, root, []float32{0, 0})
	if err != nil {
		panic(err)
	}
	fmt.Printf("published=%d opened=%d best=%s\n", generation2.ID, openedID, bestDoc)
}

func openAndSearch(ctx context.Context, root string, query []float32) (uint64, fts.DocID, error) {
	// Open follows CURRENT and holds a shared store lock until Close.
	loaded, err := semanticpersist.Open(root, semanticpersist.Limits{})
	if err != nil {
		return 0, "", err
	}
	defer loaded.Close()

	result, err := loaded.Reader.SearchDocuments(ctx, query, 2)
	if err != nil {
		return 0, "", err
	}
	if len(result.Hits) == 0 {
		return 0, "", fmt.Errorf("semantic persistence example: no search hits")
	}
	return loaded.Generation.ID, result.Hits[0].DocID, nil
}

func chunkVector(docID fts.DocID, chunkID chunk.ID, embedding []float32) semantic.ChunkVector {
	return semantic.ChunkVector{
		Ref:    chunk.Ref{ID: chunkID, DocID: docID, Field: fts.DefaultField, EndByte: 10},
		Vector: embedding,
	}
}
