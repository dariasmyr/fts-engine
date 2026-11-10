package main

import (
	"context"
	"fmt"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/semanticencode"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// This example shows that mutations become searchable only after Commit.
func main() {
	ctx := context.Background()
	splitter, err := chunk.NewSplitter(chunk.Config{
		TargetBytes:  512,
		MaxBytes:     768,
		OverlapBytes: 64,
	})
	must(err)

	encoder, err := semanticencode.NewEncoder(semanticencode.EncoderConfig{
		Metric:   vector.MetricL2Squared,
		Splitter: splitter,
		Client:   commitEmbedder{},
		Limits: semanticencode.InputLimits{
			MaxFields:         10,
			MaxSourceBytes:    1 << 20,
			MaxChunks:         10,
			MaxEmbeddingBytes: 1 << 20,
		},
	})
	must(err)

	index, err := semantic.NewIndex(semantic.SemanticConfig{
		Model: semantic.ModelConfig{ID: "commit-example", Revision: "v1", Dimensions: 2, ContextTokens: 512},
		Index: semantic.IndexConfig{
			Metric: vector.MetricL2Squared,
			Limits: semantic.LimitsConfig{MaxLiveVectors: 100, MaxChunksPerDocument: 10},
			Search: semantic.SearchConfig{MaxResults: 10, MaxCandidateChunks: 100, MaxChunksPerResult: 3},
		},
		Chunking: semantic.ChunkingConfig{Kind: semantic.ChunkingKindCustom},
	})
	must(err)
	service, err := semantic.NewService(index, encoder, encoder)
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

type commitEmbedder struct{}

func (commitEmbedder) Model() embedding.Model {
	return embedding.Model{
		ID: "commit-example", Revision: "v1", Dimensions: 2, ContextTokens: 512,
	}
}

func (commitEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
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
