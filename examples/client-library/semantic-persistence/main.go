package main

import (
	"context"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/service"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticpersist"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func main() {
	ctx := context.Background()
	root, err := os.MkdirTemp("", "fts-semantic-persistence-")
	must(err)
	defer os.RemoveAll(root)

	embedder := persistenceEmbedder{}
	config, err := service.DefaultConfig(embedder.Model())
	must(err)
	config.Index.Metric = vector.MetricL2Squared
	config.Pipeline.Chunking = chunk.Config{
		TargetBytes: 512, MaxBytes: 768, OverlapBytes: 64,
	}
	config.Index.Operational.Capacity.MaxLiveVectors = 100
	config.Index.Operational.Capacity.MaxStaleVectors = 100
	config.Index.Operational.Ingestion.MaxChunksPerDocument = 10
	config.Index.Operational.Search.MaxResults = 10
	config.Index.Operational.Search.MaxCandidateChunks = 100
	config.Index.Operational.Search.MaxChunksPerResult = 3

	semanticService, err := service.New(embedder, config)
	must(err)

	must(semanticService.AddDocument(ctx, document("doc-a", "a-v1")))
	must(semanticService.AddDocument(ctx, document("doc-b", "b")))
	generation, err := semanticpersist.CommitAndSaveService(
		ctx,
		root,
		semanticService,
		semanticpersist.SaveConfig{
			ExpectedGeneration: &semanticpersist.Generation{},
		},
	)
	must(err)
	fmt.Printf("initial content generation=%d\n", generation.ID)
	search(ctx, semanticService, "query-v1", "after commit and save")

	must(semanticService.ReplaceDocument(ctx, document("doc-a", "a-v2")))
	generation, err = semanticpersist.CommitAndSaveService(
		ctx,
		root,
		semanticService,
		semanticpersist.SaveConfig{
			ExpectedGeneration: &generation,
		},
	)
	must(err)
	fmt.Printf("updated generation=%d\n", generation.ID)

	loaded, _, err := semanticpersist.LoadService(
		ctx,
		root,
		persistenceEmbedder{},
		semanticpersist.LoadConfig{},
	)
	must(err)
	search(ctx, loaded, "query-v2", "after reopen")
}

func document(id fts.DocID, text string) fts.Document {
	return fts.Document{
		ID: id,
		Fields: map[string]fts.FieldData{
			"body": {Text: text},
		},
	}
}

func search(
	ctx context.Context,
	service *service.Service,
	query string,
	label string,
) {
	result, err := service.Search(ctx, query, 1)
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

func (persistenceEmbedder) Model() embedding.Model {
	return embedding.Model{
		ID: "persistence-example", Revision: "v1", Dimensions: 2, ContextTokens: 512,
	}
}

func (persistenceEmbedder) Embed(
	_ context.Context,
	texts []string,
) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i, text := range texts {
		switch text {
		case "document: a-v1", "query: query-v1":
			result[i] = []float32{0, 0}
		case "document: a-v2", "query: query-v2":
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
