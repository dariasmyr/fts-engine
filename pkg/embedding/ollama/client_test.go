package ollama

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
)

func TestClientEmbed(t *testing.T) {
	server := newModelServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/embed" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var request struct {
			Model      string   `json:"model"`
			Input      []string `json:"input"`
			Truncate   bool     `json:"truncate"`
			Dimensions int      `json:"dimensions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Model != "test-model" || !slices.Equal(request.Input, []string{"one", "two"}) ||
			!request.Truncate || request.Dimensions != 2 {
			t.Errorf("request = %+v", request)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": [][]float32{{1, 2}, {3, 4}}})
	}))
	defer server.Close()

	config := testConfig()
	config.BaseURL = server.URL
	config.Truncate = true
	client, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if client.Model() != testModel() {
		t.Fatalf("Model() = %+v, want %+v", client.Model(), testModel())
	}
	vectors, err := client.Embed(t.Context(), []string{"one", "two"})
	if err != nil {
		t.Fatal(err)
	}
	if len(vectors) != 2 || !slices.Equal(vectors[0], []float32{1, 2}) || !slices.Equal(vectors[1], []float32{3, 4}) {
		t.Fatalf("vectors = %v", vectors)
	}
}

func TestClientEmbedErrors(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		maxBytes   int64
		wantAPI    bool
		wantStatus int
	}{
		{
			name: "api status",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "model unavailable", http.StatusServiceUnavailable)
			},
			wantAPI: true, wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "malformed response",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("{"))
			},
		},
		{
			name: "wrong dimensions",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"embeddings":[[1]]}`))
			},
		},
		{
			name: "response limit",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"embeddings":[[1,2]]}`))
			},
			maxBytes: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newModelServer(t, tt.handler)
			defer server.Close()
			config := testConfig()
			config.BaseURL = server.URL
			client, err := New(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			client.maxResponseBytes = tt.maxBytes
			if client.maxResponseBytes == 0 {
				client.maxResponseBytes = defaultMaxResponseBytes
			}
			_, err = client.Embed(t.Context(), []string{"text"})
			if err == nil {
				t.Fatal("Embed() error = nil")
			}
			if tt.wantAPI {
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.StatusCode != tt.wantStatus {
					t.Fatalf("Embed() error = %v", err)
				}
			}
		})
	}
}

func TestClientEmbedCancellation(t *testing.T) {
	server := newModelServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": [][]float32{{1, 2}}})
	}))
	defer server.Close()
	config := testConfig()
	config.BaseURL = server.URL
	client, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Embed(ctx, []string{"text"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Embed() error = %v, want context.Canceled", err)
	}
}

func TestNewVerifiesModel(t *testing.T) {
	tests := []struct {
		name         string
		digest       string
		dimensions   int
		context      int
		capabilities []string
	}{
		{name: "different dimensions", digest: "digest", dimensions: 3, context: 512, capabilities: []string{"embedding"}},
		{name: "different context", digest: "digest", dimensions: 2, context: 1024, capabilities: []string{"embedding"}},
		{name: "missing capability", digest: "digest", dimensions: 2, context: 512, capabilities: []string{"completion"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(metadataHandler(
				t, tt.digest, tt.dimensions, tt.context, tt.capabilities, nil,
			))
			defer server.Close()
			config := testConfig()
			config.BaseURL = server.URL
			if _, err := New(t.Context(), config); !errors.Is(err, ErrModelMismatch) {
				t.Fatalf("New() error = %v, want ErrModelMismatch", err)
			}
		})
	}
}

func TestNewRejectsRevisionChangeDuringInitialization(t *testing.T) {
	metadata := metadataHandler(t, "first", 2, 512, []string{"embedding"}, nil)
	tagRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tags" {
			metadata.ServeHTTP(w, r)
			return
		}
		tagRequests++
		digest := "first"
		if tagRequests > 1 {
			digest = "second"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"models": []listedModel{{
			Name: "test-model", Model: "test-model", Digest: digest,
		}}})
	}))
	defer server.Close()

	config := testConfig()
	config.BaseURL = server.URL
	if _, err := New(t.Context(), config); !errors.Is(err, ErrModelMismatch) {
		t.Fatalf("New() error = %v, want ErrModelMismatch", err)
	}
}

func TestNewUsesModelNameWhenDigestReferencesAreUnsupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []listedModel{{
				Name: "test-model", Model: "test-model", Digest: "digest",
			}}})
		case "/api/show":
			var request map[string]string
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request["model"] != "test-model" {
				http.Error(w, "model not found", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"capabilities": []string{"embedding"},
				"model_info": map[string]any{
					"general.architecture":  "test",
					"test.embedding_length": 2,
					"test.context_length":   512,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	config := testConfig()
	config.BaseURL = server.URL
	client, err := New(t.Context(), config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if client.Model().Revision != "digest" {
		t.Fatalf("Model().Revision = %q, want digest", client.Model().Revision)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	valid := testConfig()
	for _, config := range []Config{
		{},
		{BaseURL: "localhost:11434", Model: valid.Model, Dimensions: 2, ContextTokens: 512},
		{BaseURL: "ftp://localhost", Model: valid.Model, Dimensions: 2, ContextTokens: 512},
		{BaseURL: "http://localhost?token=secret", Model: valid.Model, Dimensions: 2, ContextTokens: 512},
		{BaseURL: "http://localhost", Dimensions: 2, ContextTokens: 512},
		{BaseURL: "http://localhost", Model: valid.Model, ContextTokens: 512},
		{BaseURL: "http://localhost", Model: valid.Model, Dimensions: 2},
		{BaseURL: "http://localhost", Model: valid.Model, Dimensions: 2, ContextTokens: 512, MaxResponseBytes: -1},
	} {
		if _, err := New(t.Context(), config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("New(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}
}

func TestNewCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	config := testConfig()
	config.BaseURL = "http://localhost:11434"
	if _, err := New(ctx, config); !errors.Is(err, context.Canceled) {
		t.Fatalf("New() error = %v, want context.Canceled", err)
	}
}

func newModelServer(t testing.TB, embed http.Handler) *httptest.Server {
	t.Helper()
	return httptest.NewServer(metadataHandler(t, "digest", 2, 512, []string{"embedding"}, embed))
}

func metadataHandler(
	t testing.TB,
	digest string,
	dimensions, contextTokens int,
	capabilities []string,
	embed http.Handler,
) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []listedModel{{
				Name: "test-model", Model: "test-model", Digest: digest,
			}}})
		case "/api/show":
			var request map[string]string
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if request["model"] != "test-model" {
				t.Errorf("show model = %q, want test-model", request["model"])
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"capabilities": capabilities,
				"model_info": map[string]any{
					"general.architecture":  "test",
					"test.embedding_length": dimensions,
					"test.context_length":   contextTokens,
				},
			})
		case "/api/embed":
			if embed == nil {
				http.Error(w, "unexpected embed request", http.StatusInternalServerError)
				return
			}
			embed.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

func testModel() embedding.Model {
	return embedding.Model{
		ID: "test-model", Revision: "digest", Dimensions: 2, ContextTokens: 512,
	}
}

func testConfig() Config {
	return Config{Model: "test-model", Dimensions: 2, ContextTokens: 512}
}
