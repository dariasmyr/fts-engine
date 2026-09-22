package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticencode"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// This example shows that mutations become searchable only after Flush.
func main() {
	ctx := context.Background()
	embedding, err := semantic.NewEmbeddingDescriptor("example-provider", "publication-example", "v1", "publication-embedding-v1", semantic.VectorSpec{Dimensions: 2, Metric: vector.MetricL2Squared, VectorFormatVersion: 1})
	if err != nil {
		panic(err)
	}
	service, err := semantic.New(semantic.Config{
		Embedding:               embedding,
		Chunking:                semantic.ChunkingDescriptor{ID: "publication-example-v1", Version: 1, Fingerprint: "publication-chunks-fp-v1"},
		MaxVectors:              100,
		MaxChunksPerDocument:    10,
		MaxK:                    10,
		MaxChunkCandidates:      100,
		MaxChunksPerDocumentHit: 3,
	})
	must(err)
	embedding, chunking := service.Embedding(), service.Chunking()
	encoder, err := semanticencode.New(nil, publicationEmbedder{}, embedding, chunking)
	must(err)

	// Expected: no hits. The initial published index is empty.
	search(ctx, encoder, service, "initial empty published index")

	must(service.AddDocument(ctx, encoder, semantic.Document{ID: "doc-a", Fields: map[string]string{"body": "doc-a"}}))
	// Expected: no hits. AddDocument only queues the mutation.
	search(ctx, encoder, service, "after AddDocument, before Flush")

	must(service.Flush(ctx))
	// Expected: doc-a. Flush publishes a new searchable index.
	search(ctx, encoder, service, "after Flush")

	service.DeleteDocument("doc-a")
	// Expected: doc-a. The deletion is queued until Flush.
	search(ctx, encoder, service, "after DeleteDocument, before Flush")

	must(service.Flush(ctx))
	// Expected: no hits. The deletion is now published.
	search(ctx, encoder, service, "after delete Flush")
}

func search(ctx context.Context, encoder semantic.Encoder, service *semantic.Service, label string) {
	result, err := service.SearchDocuments(ctx, encoder, semantic.Document{ID: "query", Fields: map[string]string{"body": "query"}}, 1)
	must(err)
	if len(result.Hits) == 0 {
		fmt.Printf("%s: no hits\n", label)
		return
	}
	fmt.Printf("%s: doc=%s\n", label, result.Hits[0].DocID)
}

type publicationEmbedder struct{}

func (publicationEmbedder) Embed(_ context.Context, chunks []chunk.Chunk) ([][]float32, error) {
	result := make([][]float32, len(chunks))
	for i, item := range chunks {
		if item.Text == "doc-a" || item.Text == "query" {
			result[i] = []float32{0, 0}
		} else {
			result[i] = []float32{1, 0}
		}
	}
	return result, nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
