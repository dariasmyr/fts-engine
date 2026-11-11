package semantic_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding/ollama"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

func TestDocumentPipelineFromProviderToSearch(t *testing.T) {
	var (
		requestsMu sync.Mutex
		requests   [][]string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tags" {
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []map[string]any{{
				"name": "test-model", "model": "test-model", "digest": "digest-v1",
			}}})
			return
		}
		if r.URL.Path == "/api/show" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"capabilities": []string{"embedding"},
				"model_info": map[string]any{
					"general.architecture":  "test",
					"test.embedding_length": 2,
					"test.context_length":   512,
				},
			})
			return
		}
		if r.URL.Path != "/api/embed" {
			http.NotFound(w, r)
			return
		}
		var request struct {
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		requestsMu.Lock()
		requests = append(requests, append([]string(nil), request.Input...))
		requestsMu.Unlock()

		vectors := make([][]float32, len(request.Input))
		for i, text := range request.Input {
			switch {
			case strings.Contains(strings.ToLower(text), "hotel"):
				vectors[i] = []float32{1, 0}
			default:
				vectors[i] = []float32{0, 1}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": vectors})
	}))
	defer server.Close()

	embeddingClient, err := ollama.New(t.Context(), ollama.Config{
		BaseURL: server.URL, Model: "test-model", Dimensions: 2, ContextTokens: 512,
		DocumentPrefix: "search_document: ", QueryPrefix: "search_query: ",
	})
	if err != nil {
		t.Fatal(err)
	}
	config, err := semantic.DefaultServiceConfig(embeddingClient.Model())
	if err != nil {
		t.Fatal(err)
	}
	config.Chunking = chunk.Config{
		TargetBytes: 64, MaxBytes: 96, OverlapBytes: 8,
	}
	service, err := semantic.New(embeddingClient, config)
	if err != nil {
		t.Fatal(err)
	}

	for _, document := range []fts.Document{
		{ID: "hotel", Fields: map[string]fts.FieldData{"body": {Text: "A quiet hotel near a river."}}},
		{ID: "shipping", Fields: map[string]fts.FieldData{"body": {Text: "Cargo shipping on European rivers."}}},
	} {
		if err := service.AddDocument(t.Context(), document); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	result, err := service.Search(t.Context(), "hotel by a river", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != "hotel" {
		t.Fatalf("search hits = %+v, want hotel", result.Hits)
	}

	requestsMu.Lock()
	defer requestsMu.Unlock()
	var documentInputs, queryInputs int
	for _, batch := range requests {
		for _, input := range batch {
			switch {
			case strings.HasPrefix(input, "search_document: "):
				documentInputs++
			case strings.HasPrefix(input, "search_query: "):
				queryInputs++
			default:
				t.Fatalf("provider input %q has no retrieval-role prefix", input)
			}
		}
	}
	if documentInputs != 2 || queryInputs != 1 {
		t.Fatalf("provider role inputs: documents=%d queries=%d, want 2 and 1", documentInputs, queryInputs)
	}
}
