package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// This example shows that mutations become searchable only after Commit.
func main() {
	ctx := context.Background()
	embedder := exampleEmbedder{}
	config, err := semantic.DefaultServiceConfig(embedder.Model())
	must(err)
	config.Index.Metric = vector.MetricL2Squared
	config.Index.Limits.MaxLiveVectors = 100
	config.Index.Limits.MaxStaleVectors = 100
	config.Index.Limits.MaxChunksPerDocument = 10
	config.Index.Search.MaxResults = 10
	config.Index.Search.MaxCandidateChunks = 100
	config.Index.Search.MaxChunksPerResult = 3
	service, err := semantic.New(embedder, config)
	must(err)

	// Expected: no hits. The initial committed index is empty.
	search(ctx, service, "initial empty committed index")

	must(service.AddDocument(ctx, fts.Document{ID: "doc-a", Fields: map[string]fts.FieldData{"body": {Text: "doc-a"}}}))
	// Expected: no hits. AddDocument only queues the mutation.
	search(ctx, service, "after AddDocument, before Commit")

	must(service.Commit(ctx))
	// Expected: doc-a. Commit makes the new index view searchable.
	search(ctx, service, "after Commit")

	if err := service.DeleteDocument(ctx, "doc-a"); err != nil {
		panic(err)
	}
	// Expected: doc-a. The deletion is queued until Commit.
	search(ctx, service, "after DeleteDocument, before Commit")

	must(service.Commit(ctx))
	// Expected: no hits. The deletion is now committed.
	search(ctx, service, "after delete Commit")
}

func search(ctx context.Context, service *semantic.Service, label string) {
	result, err := service.Search(ctx, "query", 1)
	must(err)
	if len(result.Hits) == 0 {
		fmt.Printf("%s: no hits\n", label)
		return
	}
	fmt.Printf("%s: doc=%s\n", label, result.Hits[0].DocID)
}

type exampleEmbedder struct{}

func (exampleEmbedder) Model() embedding.Model {
	return embedding.Model{
		ID: "commit-example", Revision: "v1", Dimensions: 2, ContextTokens: 512,
	}
}

func (exampleEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i, text := range texts {
		if text == "doc-a" || text == "query" {
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
