package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
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
		semanticpersist.SaveOptions{
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
		semanticpersist.SaveOptions{
			ExpectedGeneration: &generation,
		},
	)
	must(err)
	fmt.Printf("updated generation=%d\n", generation.ID)

	info, err := semanticpersist.Inspect(ctx, root, semanticpersist.LoadOptions{})
	must(err)
	fmt.Printf(
		"stored generation=%d dimensions=%d model=%s\n",
		info.Generation.ID,
		info.IndexConfig.Dimensions,
		info.Pipeline.Model.ID,
	)

	otherService, err := service.New(embedder, config)
	must(err)
	_, err = semanticpersist.SaveService(
		ctx,
		root,
		otherService,
		semanticpersist.SaveOptions{ExpectedGeneration: &generation},
	)
	if !errors.Is(err, semanticpersist.ErrIndexIdentityMismatch) {
		panic(fmt.Errorf("save with another index identity: %w", err))
	}
	fmt.Println("save with another index identity was rejected")

	loader, err := semanticpersist.NewLoader(root, semanticpersist.LoadOptions{})
	must(err)
	loaded, generationID, err := service.Load(ctx, loader, persistenceEmbedder{})
	must(err)
	generation = semanticpersist.Generation{ID: generationID}
	fmt.Printf("reopened generation=%d\n", generation.ID)
	search(ctx, loaded, "query-v2", "after reopen")

	must(loaded.AddDocument(ctx, document("doc-c", "c")))
	generation, err = semanticpersist.CommitAndSaveService(
		ctx,
		root,
		loaded,
		semanticpersist.SaveOptions{ExpectedGeneration: &generation},
	)
	must(err)
	fmt.Printf("post-reopen generation=%d\n", generation.ID)
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
	semanticService *service.Service,
	query string,
	label string,
) {
	result, err := semanticService.SearchWithConfig(ctx, query, 1, semantic.QueryConfig{
		EfSearch:        8,
		VisitLimit:      100,
		CandidateChunks: 10,
	})
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
		DocumentPrefix: "document: ", QueryPrefix: "query: ",
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
