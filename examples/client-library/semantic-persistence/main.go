package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/semanticencode"
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

	embedding, err := semantic.NewEmbeddingDescriptor("example-provider", "example-embedding", "v1", "example-embedding-v1", semantic.VectorSpec{Dimensions: 2, Metric: vector.MetricL2Squared, VectorFormatVersion: 1})
	must(err)
	chunking := semantic.ChunkingDescriptor{ID: "example-chunks-v1", Version: 1, Fingerprint: "example-chunks-fp-v1"}
	service, err := semantic.New(semantic.Config{
		Embedding:               embedding,
		Chunking:                chunking,
		MaxVectors:              100,
		MaxChunksPerDocument:    10,
		MaxK:                    10,
		MaxChunkCandidates:      100,
		MaxChunksPerDocumentHit: 3,
	})
	if err != nil {
		panic(err)
	}
	encoder, err := semanticencode.New(nil, persistenceEmbedder{}, embedding, chunking)
	must(err)

	// Embeddings are produced by the caller or an external model.
	must(service.AddDocument(ctx, encoder, semantic.Document{ID: "doc-a", Fields: map[string]string{"body": "a-v1"}}))
	must(service.AddDocument(ctx, encoder, semantic.Document{ID: "doc-b", Fields: map[string]string{"body": "b-v1"}}))
	must(service.ReplaceDocument(ctx, encoder, semantic.Document{ID: "doc-a", Fields: map[string]string{"body": "a-v2"}}))

	// Compact and publish the active immutable HNSW segment.
	must(service.Compact(ctx))
	view, err := service.ReadView(ctx)
	must(err)
	segment := view.Segments()[0]
	stats := service.Statistics()
	sealed := semanticpersist.SealedSegment{
		Segment: segment, Embedding: service.Embedding(), Chunking: service.Chunking(),
		MaxAllocatedVectorID: stats.MaxAllocatedVectorID, MaxK: 10,
		MaxChunkCandidates: 100, MaxChunksPerDocumentHit: 3,
	}
	generation, err := semanticpersist.PublishSealedSegment(ctx, root, 1, sealed, semanticpersist.Options{
		ExpectedGeneration: 0,
		Durability:         semanticpersist.DurabilitySynchronous,
	})
	must(err)

	loaded, err := semanticpersist.Open(root, semanticpersist.OpenOptions{Limits: semanticpersist.DefaultLimits(), ExpectedDescriptors: semantic.PipelineDescriptor{Embedding: embedding, Chunking: chunking}})
	must(err)
	defer loaded.Close()
	loadedView, err := semantic.NewReadView(generation.ID, []*semantic.Segment{loaded.Sealed.Segment})
	must(err)
	result, err := loadedView.SearchDocuments(ctx, encoder, semantic.Document{ID: "query", Fields: map[string]string{"body": "query"}}, 2)
	must(err)

	fmt.Printf("generation=%d kind=%d rows=%d\n", generation.ID, loaded.Sealed.Segment.Kind(), len(loaded.Sealed.Segment.Rows()))
	for _, hit := range result.Hits {
		fmt.Printf("doc=%s distance=%.0f\n", hit.DocID, hit.Distance)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type persistenceEmbedder struct{}

func (persistenceEmbedder) Embed(_ context.Context, chunks []chunk.Chunk) ([][]float32, error) {
	result := make([][]float32, len(chunks))
	for i, item := range chunks {
		switch item.Text {
		case "a-v2":
			result[i] = []float32{10, 0}
		case "b-v1":
			result[i] = []float32{5, 0}
		default:
			result[i] = []float32{0, 0}
		}
	}
	return result, nil
}
