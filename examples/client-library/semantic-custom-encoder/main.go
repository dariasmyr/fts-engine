package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

// This example shows the low-level pipeline: the caller owns document and
// query encoders and binds them to an index through semantic.NewService.
func main() {
	ctx := context.Background()
	client := exampleEmbedder{}
	splitter := paragraphSplitter{}
	limits := semantic.InputLimits{
		MaxFields:         4,
		MaxSourceBytes:    1 << 20,
		MaxChunks:         16,
		MaxEmbeddingBytes: 1 << 20,
	}

	encoder, err := semantic.NewEncoder(semantic.EncoderConfig{
		Limits: limits, Splitter: splitter, Client: client,
	})
	must(err)

	index, err := semantic.NewIndex(semantic.IndexConfig{
		Dimensions: client.Model().Dimensions,
		Metric:     vector.MetricL2Squared,
		Operational: semantic.OperationalLimits{
			Capacity:  semantic.CapacityLimits{MaxLiveVectors: 100, MaxStaleVectors: 100},
			Ingestion: semantic.IngestionLimits{MaxChunksPerDocument: 16},
			Search:    semantic.SearchLimits{MaxResults: 10, MaxCandidateChunks: 100, MaxChunksPerResult: 3},
		},
	})
	must(err)
	service, err := semantic.NewService(index, encoder, encoder)
	must(err)

	must(service.AddDocument(ctx, fts.Document{
		ID: "travel",
		Fields: map[string]fts.FieldData{
			"body": {Text: "A hotel stands beside the river.\n\nBoats leave every morning."},
		},
	}))
	must(service.Commit(ctx))

	result, err := service.Search(ctx, "hotel beside the river", 1)
	must(err)
	for _, hit := range result.Hits {
		fmt.Printf("doc=%s distance=%.1f\n", hit.DocID, hit.Distance)
	}
}

// paragraphSplitter is application code. The same instance is supplied to the
// document and query encoders, so both paths follow the same chunking rules.
type paragraphSplitter struct{}

func (paragraphSplitter) Split(text string) ([]chunk.Range, error) {
	ranges := []chunk.Range{}
	for start := 0; start < len(text); {
		relative := strings.Index(text[start:], "\n\n")
		if relative < 0 {
			ranges = append(ranges, chunk.Range{StartByte: start, EndByte: len(text)})
			break
		}
		end := start + relative
		if end > start {
			ranges = append(ranges, chunk.Range{StartByte: start, EndByte: end})
		}
		start = end + len("\n\n")
	}
	return ranges, nil
}

type exampleEmbedder struct{}

func (exampleEmbedder) Model() embedding.Model {
	return embedding.Model{
		ID: "custom-encoder-example", Revision: "v1", Dimensions: 2, ContextTokens: 512,
	}
}

func (exampleEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i, text := range texts {
		if strings.Contains(text, "hotel") || strings.Contains(text, "river") {
			vectors[i] = []float32{0, 0}
			continue
		}
		vectors[i] = []float32{10, 0}
	}
	return vectors, nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
