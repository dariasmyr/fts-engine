package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding/ollama"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

func main() {
	ctx := context.Background()
	baseURL := envOr("OLLAMA_URL", "http://localhost:11434")
	client, err := ollama.New(ctx, ollama.Config{
		BaseURL:        baseURL,
		Model:          envOr("OLLAMA_MODEL", "nomic-embed-text:latest"),
		Dimensions:     envIntOr("OLLAMA_DIMENSIONS", 768),
		ContextTokens:  envIntOr("OLLAMA_CONTEXT_TOKENS", 8192),
		DocumentPrefix: "search_document: ",
		QueryPrefix:    "search_query: ",
	})
	must(err)
	config, err := semantic.DefaultServiceConfig(client.Model())
	must(err)
	service, err := semantic.New(client, config)
	must(err)

	for _, document := range []fts.Document{
		{ID: "hotel", Fields: map[string]fts.FieldData{"body": {Text: "A quiet hotel near a river in France."}}},
		{ID: "shipping", Fields: map[string]fts.FieldData{"body": {Text: "Cargo shipping operations on European rivers."}}},
	} {
		must(service.AddDocument(ctx, document))
	}
	must(service.Commit(ctx))

	result, err := service.Search(ctx, "French hotel by a river", 2)
	must(err)
	for _, hit := range result.Hits {
		fmt.Printf("doc=%s distance=%.4f\n", hit.DocID, hit.Distance)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envIntOr(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	must(err)
	return parsed
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
