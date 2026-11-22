package embedding

import (
	"context"
	"slices"
	"testing"
)

func TestWithPrefixDoesNotMutateInput(t *testing.T) {
	input := []string{"one", "two"}
	var received []string
	embeddingClient, err := NewClient(Model{ID: "model", Revision: "digest", Dimensions: 2, ContextTokens: 512}, func(_ context.Context, texts []string) ([][]float32, error) {
		received = append([]string(nil), texts...)
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, 2}
		}
		return vectors, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client := WithPrefix(embeddingClient, "search_document: ")

	if _, err := client.Embed(t.Context(), input); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(input, []string{"one", "two"}) {
		t.Fatalf("input mutated: %v", input)
	}
	if !slices.Equal(received, []string{"search_document: one", "search_document: two"}) {
		t.Fatalf("received = %v", received)
	}
	if client.Model() != embeddingClient.Model() {
		t.Fatalf("Model() = %+v, want %+v", client.Model(), embeddingClient.Model())
	}
}
