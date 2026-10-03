package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticencode"
	"github.com/dariasmyr/fts-engine/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

func main() {
	ctx := context.Background()
	root, err := os.MkdirTemp("", "fts-semantic-persistence-")
	must(err)
	defer os.RemoveAll(root)

	embedding, err := semantic.NewEmbeddingDescriptor(
		"example-provider", "persistence-example", "v1",
		"persistence-embedding-v1", 2, vector.MetricL2Squared, 1,
	)
	must(err)
	chunking := semantic.ChunkingDescriptor{
		ID: "persistence-example-v1", Version: 1, Fingerprint: "persistence-chunks-v1",
	}
	service, err := semantic.New(semantic.Config{
		Embedding:               embedding,
		Chunking:                chunking,
		MaxVectors:              100,
		MaxChunksPerDocument:    10,
		MaxK:                    10,
		MaxChunkCandidates:      100,
		MaxChunksPerDocumentHit: 3,
	})
	must(err)
	encoder, err := semanticencode.New(nil, persistenceEmbedder{}, embedding, chunking)
	must(err)

	must(service.AddDocument(ctx, encoder, document("doc-a", "a-v1")))
	must(service.AddDocument(ctx, encoder, document("doc-b", "b")))
	must(service.Flush(ctx))

	generation, err := semanticpersist.Publish(ctx, root, service, semanticpersist.Options{
		ExpectedGeneration: 0,
		Durability:         semanticpersist.DurabilitySynchronous,
	})
	must(err)
	fmt.Printf("initial generation=%d\n", generation.ID)

	store, err := semanticpersist.Open(ctx, root, semanticpersist.OpenOptions{
		Limits: semanticpersist.DefaultLimits(),
		ExpectedDescriptors: semantic.PipelineDescriptor{
			Embedding: embedding,
			Chunking:  chunking,
		},
	})
	must(err)
	writable := store.Service()
	search(ctx, encoder, writable, "query-v1", "after open")

	must(writable.ReplaceDocument(ctx, encoder, document("doc-a", "a-v2")))
	must(writable.Flush(ctx))
	generation, err = store.Publish(ctx, semanticpersist.Options{
		Durability: semanticpersist.DurabilitySynchronous,
	})
	must(err)
	fmt.Printf("updated generation=%d\n", generation.ID)
	must(store.Close())

	reopened, err := semanticpersist.Open(ctx, root, semanticpersist.OpenOptions{
		Limits: semanticpersist.DefaultLimits(),
		ExpectedDescriptors: semantic.PipelineDescriptor{
			Embedding: embedding,
			Chunking:  chunking,
		},
	})
	must(err)
	search(ctx, encoder, reopened.Service(), "query-v2", "after reopen")
	must(reopened.Close())
}

func document(id fts.DocID, body string) semantic.Document {
	return semantic.Document{ID: id, Fields: map[string]string{"body": body}}
}

func search(ctx context.Context, encoder semantic.Encoder, service *semantic.Service, query, label string) {
	result, err := service.SearchDocuments(ctx, encoder, document("query", query), 1)
	must(err)
	if len(result.Hits) == 0 {
		fmt.Printf("%s: no hits\n", label)
		return
	}
	fmt.Printf("%s: doc=%s distance=%.0f\n", label, result.Hits[0].DocID, result.Hits[0].Distance)
}

type persistenceEmbedder struct{}

func (persistenceEmbedder) Embed(_ context.Context, chunks []chunk.Chunk) ([][]float32, error) {
	result := make([][]float32, len(chunks))
	for i, item := range chunks {
		switch item.Text {
		case "a-v1", "query-v1":
			result[i] = []float32{0, 0}
		case "a-v2", "query-v2":
			result[i] = []float32{4, 0}
		default:
			result[i] = []float32{10, 0}
		}
	}
	return result, nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
