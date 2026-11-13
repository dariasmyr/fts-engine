package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// This example shows that mutations become searchable only after Commit.
func main() {
	ctx := context.Background()
	embedder := exampleEmbedder{}
	config, err := service.DefaultConfig(embedder.Model())
	must(err)
	config.Index.Metric = vector.MetricL2Squared
	config.Index.Operational.Capacity.MaxLiveVectors = 100
	config.Index.Operational.Capacity.MaxStaleVectors = 100
	config.Index.Operational.Ingestion.MaxChunksPerDocument = 10
	config.Index.Operational.Search.MaxResults = 10
	config.Index.Operational.Search.MaxCandidateChunks = 100
	config.Index.Operational.Search.MaxChunksPerResult = 3
	semanticService, err := service.New(embedder, config)
	must(err)

	// Expected: no hits. The initial committed index is empty.
	search(ctx, semanticService, "initial empty committed index")

	must(semanticService.AddDocument(ctx, fts.Document{ID: "doc-a", Fields: map[string]fts.FieldData{"body": {Text: "doc-a"}}}))
	// Expected: no hits. AddDocument only queues the mutation.
	search(ctx, semanticService, "after AddDocument, before Commit")

	must(semanticService.Commit(ctx))
	// Expected: doc-a. Commit makes the new index view searchable.
	search(ctx, semanticService, "after Commit")

	if err := semanticService.DeleteDocument(ctx, "doc-a"); err != nil {
		panic(err)
	}
	// Expected: doc-a. The deletion is queued until Commit.
	search(ctx, semanticService, "after DeleteDocument, before Commit")

	must(semanticService.Commit(ctx))
	// Expected: no hits. The deletion is now committed.
	search(ctx, semanticService, "after delete Commit")
}

func search(ctx context.Context, service *service.Service, label string) {
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
