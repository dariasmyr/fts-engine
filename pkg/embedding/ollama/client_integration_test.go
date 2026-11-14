//go:build integration

package ollama

import (
	"os"
	"strconv"
	"testing"
)

func TestIntegrationEmbed(t *testing.T) {
	baseURL := os.Getenv("OLLAMA_URL")
	model := os.Getenv("OLLAMA_MODEL")
	dimensionsText := os.Getenv("OLLAMA_DIMENSIONS")
	contextText := os.Getenv("OLLAMA_CONTEXT_TOKENS")
	if baseURL == "" || model == "" || dimensionsText == "" || contextText == "" {
		t.Skip("set OLLAMA_URL, OLLAMA_MODEL, OLLAMA_DIMENSIONS, and OLLAMA_CONTEXT_TOKENS")
	}
	dimensions, err := strconv.Atoi(dimensionsText)
	if err != nil {
		t.Fatal(err)
	}
	contextTokens, err := strconv.Atoi(contextText)
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(t.Context(), Config{
		BaseURL: baseURL, Model: model, Dimensions: dimensions, ContextTokens: contextTokens,
	})
	if err != nil {
		t.Fatal(err)
	}
	vectors, err := client.Embed(t.Context(), []string{"semantic search integration test"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vectors) != 1 || len(vectors[0]) != dimensions {
		t.Fatalf("embedding shape = %d x %d", len(vectors), len(vectors[0]))
	}
}
