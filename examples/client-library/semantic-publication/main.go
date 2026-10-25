package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticencode"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

// This example shows that mutations become searchable only after Flush.
func main() {
	ctx := context.Background()
	embedding, err := semantic.NewEmbeddingDescriptor("example-provider", "publication-example", "v1", "publication-embedding-v1", 2, vector.MetricL2Squared, 1)
	if err != nil {
		panic(err)
	}
	chunking := semantic.ChunkingDescriptor{
		ID:          "publication-example-v1",
		Version:     1,
		Fingerprint: "publication-chunks-fp-v1",
	}
	schema := semantic.Schema{Embedding: embedding, Chunking: chunking}

	splitter, err := chunk.NewSplitter(chunk.Descriptor{
		TargetBytes:  512,
		MaxBytes:     768,
		OverlapBytes: 64,
	})
	must(err)

	encoder, err := semanticencode.New(semanticencode.Config{
		Schema:   schema,
		Chunker:  splitter,
		Embedder: publicationEmbedder{},
		Limits: semanticencode.Limits{
			MaxFields:         10,
			MaxSourceBytes:    1 << 20,
			MaxChunks:         10,
			MaxEmbeddingBytes: 1 << 20,
		},
	})
	must(err)

	service, err := semantic.New(semantic.Config{
		Schema: schema,
		Limits: semantic.Limits{
			MaxLiveVectors:          100,
			MaxChunksPerDocument:    10,
			MaxDocumentsPerSearch:   10,
			MaxChunkCandidates:      100,
			MaxChunksPerDocumentHit: 3,
		},
	}, encoder)
	must(err)

	// Expected: no hits. The initial published index is empty.
	search(ctx, service, "initial empty published index")

	must(service.AddDocument(ctx, fts.Document{ID: "doc-a", Fields: map[string]fts.FieldData{"body": {Text: "doc-a"}}}))
	// Expected: no hits. AddDocument only queues the mutation.
	search(ctx, service, "after AddDocument, before Flush")

	must(service.Flush(ctx))
	// Expected: doc-a. Flush publishes a new searchable index.
	search(ctx, service, "after Flush")

	if err := service.DeleteDocument(ctx, "doc-a"); err != nil {
		panic(err)
	}
	// Expected: doc-a. The deletion is queued until Flush.
	search(ctx, service, "after DeleteDocument, before Flush")

	must(service.Flush(ctx))
	// Expected: no hits. The deletion is now published.
	search(ctx, service, "after delete Flush")
}

func search(ctx context.Context, service *semantic.Service, label string) {
	result, err := service.SearchDocuments(ctx, fts.Document{ID: "query", Fields: map[string]fts.FieldData{"body": {Text: "query"}}}, 1)
	must(err)
	if len(result.Hits) == 0 {
		fmt.Printf("%s: no hits\n", label)
		return
	}
	fmt.Printf("%s: doc=%s\n", label, result.Hits[0].DocID)
}

type publicationEmbedder struct{}

func (publicationEmbedder) Embed(_ context.Context, chunks []semanticencode.EmbeddingInput) ([][]float32, error) {
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
