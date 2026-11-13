package service

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic/encoding"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestNewIndexesCommitsAndSearchesText(t *testing.T) {
	client, received := modelServiceTestClient(t, 512)
	service, err := New(client, defaultModelConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AddDocument(t.Context(), fts.Document{
		ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "document text"}},
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := service.Search(t.Context(), "query text", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending.Hits) != 0 {
		t.Fatalf("Search() before Commit hits = %+v, want none", pending.Hits)
	}
	if err := service.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	result, err := service.Search(t.Context(), "query text", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 || result.Hits[0].DocID != "doc" {
		t.Fatalf("Search() hits = %+v", result.Hits)
	}
	if !slices.Equal(*received, []string{
		"document: document text",
		"query: query text",
		"query: query text",
	}) {
		t.Fatalf("embedded inputs = %q", *received)
	}
}

func TestNewAcceptsCompleteConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	config.Index.Metric = vector.MetricL2Squared
	config.Pipeline.Chunking = chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	config.Index.Operational.Capacity.MaxLiveVectors = 256
	config.Index.Operational.Capacity.MaxStaleVectors = 256
	config.Index.Operational.Ingestion.MaxChunksPerDocument = 128
	config.Pipeline.Operational.Encoding.MaxEmbeddingBytes = uint64(128 * client.Model().Dimensions * 4)
	config.Index.Operational.Search = SearchLimits{
		MaxResults: 12, MaxCandidateChunks: 128, MaxChunksPerResult: 6,
		MaxQueryChunks: 128, MaxEfSearch: 128, MaxVisitLimit: config.Index.Operational.SearchDefaults.VisitLimit,
	}

	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := service.Config()
	if !ok || got != config || service.index.Config() != config.Index {
		t.Fatalf("config = %+v/%+v, want %+v", got, service.index.Config(), config)
	}
}

func TestNewSplitsQueriesWithDocumentSplitter(t *testing.T) {
	client, received := modelServiceTestClient(t, 512)
	config := defaultModelConfig(t, client)
	config.Pipeline.Chunking = chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1}
	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Search(t.Context(), "abcdefghij", 1); err != nil {
		t.Fatal(err)
	}
	want := []string{"query: abcd", "query: defg", "query: ghij"}
	if !slices.Equal(*received, want) {
		t.Fatalf("embedded inputs = %q, want %q", *received, want)
	}
}

func TestQueryConfigIsRequestLocal(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	service, err := New(client, defaultModelConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	before, ok := service.Config()
	if !ok {
		t.Fatal("Config() unavailable")
	}
	_, _ = service.SearchWithConfig(t.Context(), "query", 1, QueryConfig{CandidateChunks: 10})
	after, ok := service.Config()
	if !ok || after != before {
		t.Fatal("request-local query config changed static semantic config")
	}
}

func TestNewBoundsAutomaticallySplitQueries(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelConfig(t, client)
	config.Pipeline.Chunking = chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1}
	config.Index.Operational.Ingestion.MaxChunksPerDocument = 2
	config.Index.Operational.Search.MaxQueryChunks = 2
	config.Index.Operational.Search.MaxChunksPerResult = 2
	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.Search(t.Context(), "abcdefghij", 1)
	if !errors.Is(err, encoding.ErrInputLimitExceeded) {
		t.Fatalf("Search() chunk-limit error = %v, want ErrInputLimitExceeded", err)
	}

	_, err = service.Search(t.Context(), strings.Repeat("a", int(defaultMaxSourceBytes)+1), 1)
	if !errors.Is(err, encoding.ErrInputLimitExceeded) {
		t.Fatalf("Search() source-limit error = %v, want ErrInputLimitExceeded", err)
	}
}

func TestNewPreservesProviderAndContextErrors(t *testing.T) {
	providerError := errors.New("provider unavailable")
	client, err := embedding.NewClient(embedding.Model{
		ID: "model", Revision: "revision", Dimensions: 2, ContextTokens: 512,
	}, func(context.Context, []string) ([][]float32, error) {
		return nil, providerError
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(client, defaultModelConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}
	err = service.AddDocument(t.Context(), fts.Document{
		ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "text"}},
	})
	if !errors.Is(err, providerError) {
		t.Fatalf("AddDocument() error = %v, want provider error", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.Search(ctx, "query", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Search() error = %v, want context canceled", err)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "invalid metric", mutate: func(c *Config) { c.Index.Metric = 255 }},
		{name: "invalid indexed vectors", mutate: func(c *Config) { c.Index.Operational.Capacity.MaxLiveVectors = 0 }},
		{name: "invalid document chunks", mutate: func(c *Config) { c.Index.Operational.Ingestion.MaxChunksPerDocument = 0 }},
		{name: "invalid search config", mutate: func(c *Config) { c.Index.Operational.Search = SearchLimits{} }},
		{name: "search candidates exceed capacity", mutate: func(c *Config) {
			c.Index.Operational.Capacity.MaxLiveVectors = 10
			c.Index.Operational.Capacity.MaxStaleVectors = 10
			c.Index.Operational.Search = SearchLimits{MaxResults: 5, MaxCandidateChunks: 11, MaxChunksPerResult: 1}
		}},
		{name: "invalid built-in chunking", mutate: func(c *Config) { c.Pipeline.Chunking = chunk.Config{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := defaultModelConfig(t, client)
			test.mutate(&config)
			if _, err := New(client, config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}

}

func TestNewUsesIndependentDocumentAndQueryChunkLimits(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelConfig(t, client)
	config.Index.Operational.Ingestion.MaxChunksPerDocument = 7
	config.Index.Operational.Search = SearchLimits{
		MaxResults:         5,
		MaxCandidateChunks: 10,
		MaxChunksPerResult: 2,
		MaxQueryChunks:     3,
	}
	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	documents, ok := service.documentEncoder.(*encoding.Encoder)
	if !ok {
		t.Fatalf("document encoder type = %T", service.documentEncoder)
	}
	if service.index.Config().Operational.Ingestion.MaxChunksPerDocument != 7 {
		t.Fatalf("index document chunk limit = %d, want 7", service.index.Config().Operational.Ingestion.MaxChunksPerDocument)
	}
	queries, ok := service.queryEncoder.(*encoding.Encoder)
	if !ok {
		t.Fatalf("query encoder type = %T", service.queryEncoder)
	}
	if documents != queries {
		t.Fatal("built-in service uses different document and query encoders")
	}
	if service.index.Config().Operational.Search.MaxQueryChunks != 3 {
		t.Fatalf("index query chunk limit = %d, want 3", service.index.Config().Operational.Search.MaxQueryChunks)
	}
}

func TestNewUsesConfiguredEncodingLimits(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelConfig(t, client)
	config.Pipeline.Operational.Encoding = EncodingLimits{
		MaxFields: 2, MaxSourceBytes: 128, MaxEmbeddingBytes: 16,
	}
	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	documents := service.documentEncoder.(*encoding.Encoder)
	queries := service.queryEncoder.(*encoding.Encoder)
	if documents != queries {
		t.Fatal("built-in service uses different document and query encoders")
	}
}

func modelServiceTestClient(t *testing.T, contextTokens int) (embedding.Client, *[]string) {
	t.Helper()
	received := []string{}
	client, err := embedding.NewClient(embedding.Model{
		ID: "model", Revision: "revision", Dimensions: 2, ContextTokens: contextTokens,
		DocumentPrefix: "document: ", QueryPrefix: "query: ",
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		received = append(received, texts...)
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, &received
}

func defaultModelConfig(t *testing.T, client embedding.Client) Config {
	t.Helper()
	config, err := DefaultConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	return config
}
