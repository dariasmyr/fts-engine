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
		"example-provider",
		"persistence-example",
		"v1",
		"persistence-embedding-v1",
		2,
		vector.MetricL2Squared,
		1,
	)
	must(err)

	chunking := semantic.ChunkingDescriptor{
		ID:          "persistence-example-v1",
		Version:     1,
		Fingerprint: "persistence-chunks-v1",
	}

	schema := semantic.Schema{
		Embedding: embedding,
		Chunking:  chunking,
	}

	index, err := semantic.NewIndex(semantic.Config{
		Schema: schema,
		Limits: semantic.Limits{
			MaxLiveVectors:          100,
			MaxChunksPerDocument:    10,
			MaxDocumentsPerSearch:   10,
			MaxChunkCandidates:      100,
			MaxChunksPerDocumentHit: 3,
		},
	})
	must(err)

	encoder, err := semanticencode.New(
		nil,
		persistenceEmbedder{},
		embedding,
		chunking,
	)
	must(err)

	addDocument(
		ctx,
		index,
		encoder,
		fts.Document{
			ID: "doc-a",
			Fields: map[string]fts.FieldData{
				"body": {Text: "a-v1"},
			},
		},
	)

	addDocument(
		ctx,
		index,
		encoder,
		fts.Document{
			ID: "doc-b",
			Fields: map[string]fts.FieldData{
				"body": {Text: "b"},
			},
		},
	)

	must(index.Flush(ctx))

	generation, err := semanticpersist.Publish(
		ctx,
		root,
		index,
		semanticpersist.PublishOptions{
			ExpectedGeneration: &semanticpersist.Generation{
				ID: 0,
			},
			Durability: semanticpersist.DurabilitySynchronous,
		},
	)
	must(err)

	fmt.Printf("initial generation=%d\n", generation.ID)

	store, err := semanticpersist.Open(
		ctx,
		root,
		semanticpersist.OpenOptions{
			Limits:         semanticpersist.DefaultLimits(),
			ExpectedSchema: schema,
		},
	)
	must(err)

	writable := store.Index()

	search(
		ctx,
		encoder,
		writable,
		"query-v1",
		"after open",
	)

	replaceDocument(
		ctx,
		writable,
		encoder,
		fts.Document{
			ID: "doc-a",
			Fields: map[string]fts.FieldData{
				"body": {Text: "a-v2"},
			},
		},
	)

	must(writable.Flush(ctx))

	generation, err = store.Publish(
		ctx,
		semanticpersist.PublishOptions{
			Durability: semanticpersist.DurabilitySynchronous,
		},
	)
	must(err)

	fmt.Printf("updated generation=%d\n", generation.ID)

	must(store.Close())

	reopened, err := semanticpersist.Open(
		ctx,
		root,
		semanticpersist.OpenOptions{
			Limits:         semanticpersist.DefaultLimits(),
			ExpectedSchema: schema,
		},
	)
	must(err)

	search(
		ctx,
		encoder,
		reopened.Index(),
		"query-v2",
		"after reopen",
	)

	must(reopened.Close())
}

func addDocument(
	ctx context.Context,
	index *semantic.Index,
	encoder semantic.Encoder,
	document fts.Document,
) {
	encoded, err := encoder.Encode(ctx, document)
	must(err)

	must(index.Add(
		ctx,
		document.ID,
		encoded,
	))
}

func replaceDocument(
	ctx context.Context,
	index *semantic.Index,
	encoder semantic.Encoder,
	document fts.Document,
) {
	encoded, err := encoder.Encode(ctx, document)
	must(err)

	must(index.Replace(
		ctx,
		document.ID,
		encoded,
	))
}

func search(
	ctx context.Context,
	encoder semantic.Encoder,
	index *semantic.Index,
	query string,
	label string,
) {
	document := fts.Document{
		ID: "query",
		Fields: map[string]fts.FieldData{
			"body": {Text: query},
		},
	}

	encoded, err := encoder.Encode(ctx, document)
	must(err)

	result, err := index.Search(
		ctx,
		encoded,
		1,
		semantic.SearchOptions{},
	)
	must(err)

	if len(result.Hits) == 0 {
		fmt.Printf("%s: no hits\n", label)
		return
	}

	fmt.Printf(
		"%s: doc=%s distance=%.0f\n",
		label,
		result.Hits[0].DocID,
		result.Hits[0].Distance,
	)
}

type persistenceEmbedder struct{}

func (persistenceEmbedder) Embed(
	_ context.Context,
	chunks []chunk.Chunk,
) ([][]float32, error) {
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
