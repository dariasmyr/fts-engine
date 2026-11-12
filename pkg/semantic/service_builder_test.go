package semantic

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestNewIndexesCommitsAndSearchesText(t *testing.T) {
	client, received := modelServiceTestClient(t, 512)
	service, err := New(client, defaultModelServiceConfig(t, client))
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

func TestNewAcceptsCompleteServiceConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultServiceConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	config.Index.Metric = vector.MetricL2Squared
	config.Chunking = chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	config.Index.Limits.MaxLiveVectors = 256
	config.Index.Limits.MaxStaleVectors = 256
	config.Index.Limits.MaxChunksPerDocument = 128
	config.Encoding.MaxEmbeddingBytes = uint64(128 * client.Model().Dimensions * 4)
	config.Index.Search = SearchConfig{
		MaxResults: 12, MaxCandidateChunks: 128, MaxChunksPerResult: 6,
		MaxQueryChunks: 128, MaxEfSearch: 128, MaxVisitLimit: config.Index.HNSW.Search.VisitLimit,
	}

	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := service.Config()
	if !ok || got != config || service.index.config != config.Index {
		t.Fatalf("config = %+v/%+v, want %+v", got, service.index.config, config)
	}
}

func TestNewFromIndexBindsExistingIndex(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	original, err := New(client, defaultModelServiceConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}

	service, err := NewFromIndex(client, original.Index(), originalConfig(t, original))
	if err != nil {
		t.Fatal(err)
	}
	if service.Index() != original.Index() {
		t.Fatal("NewFromIndex() replaced the provided index")
	}
}

func TestNewFromIndexValidatesModelConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	original, err := New(client, defaultModelServiceConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}

	incompatible, err := embedding.NewClient(embedding.Model{
		ID: "model", Revision: "revision", Dimensions: 2, ContextTokens: 512,
		DocumentPrefix: "changed: ", QueryPrefix: "query: ",
	}, func(_ context.Context, texts []string) ([][]float32, error) {
		return make([][]float32, len(texts)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewFromIndex(incompatible, original.Index(), originalConfig(t, original))
	if !errors.Is(err, ErrModelServiceUnavailable) {
		t.Fatalf("NewFromIndex() error = %v, want ErrModelServiceUnavailable", err)
	}
}

func TestNewFromIndexValidatesIndexConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	original, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	config.Index.Metric = vector.MetricL2Squared
	if _, err := NewFromIndex(client, original.Index(), config); !errors.Is(err, ErrModelServiceUnavailable) {
		t.Fatalf("NewFromIndex() error = %v, want ErrModelServiceUnavailable", err)
	}
}

func TestNewFromIndexUsesIndexConfiguration(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Index.Metric = vector.MetricL2Squared
	config.Chunking = chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	config.Index.Limits.MaxLiveVectors = 256
	config.Index.Limits.MaxStaleVectors = 256
	config.Index.Limits.MaxChunksPerDocument = 128
	config.Encoding.MaxEmbeddingBytes = uint64(128 * client.Model().Dimensions * 4)
	config.Index.Search = SearchConfig{
		MaxResults:         12,
		MaxCandidateChunks: 128,
		MaxChunksPerResult: 6,
		MaxQueryChunks:     128,
		MaxEfSearch:        128,
		MaxVisitLimit:      config.Index.HNSW.Search.VisitLimit,
	}
	original, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	service, err := NewFromIndex(client, original.Index(), config)
	if err != nil {
		t.Fatal(err)
	}
	documents, ok := service.documentEncoder.(*Encoder)
	if !ok {
		t.Fatalf("document encoder type = %T", service.documentEncoder)
	}
	if documents.documentLimits.MaxChunks != 128 {
		t.Fatalf("document encoder chunk limit = %d, want 128", documents.documentLimits.MaxChunks)
	}
	if service.index.config.Limits.MaxStaleVectors != 256 {
		t.Fatalf("stale vector limit = %d, want 256", service.index.config.Limits.MaxStaleVectors)
	}
	serviceConfig, ok := service.Config()
	if !ok || serviceConfig.Index.Metric != vector.MetricL2Squared {
		t.Fatalf("service config = %+v, %v", serviceConfig, ok)
	}
	wantChunking := chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	splitter, ok := documents.splitter.(*chunk.Splitter)
	if !ok {
		t.Fatalf("document chunk splitter = %T, want *chunk.Splitter", documents.splitter)
	}
	if splitter.Config() != wantChunking {
		t.Fatalf("document chunk config = %+v, want %+v", splitter.Config(), wantChunking)
	}
}

func TestNewSplitsQueriesWithDocumentSplitter(t *testing.T) {
	client, received := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Chunking = chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1}
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
	service, err := New(client, defaultModelServiceConfig(t, client))
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
	config := defaultModelServiceConfig(t, client)
	config.Chunking = chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1}
	config.Index.Limits.MaxChunksPerDocument = 2
	config.Index.Search.MaxQueryChunks = 2
	config.Index.Search.MaxChunksPerResult = 2
	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.Search(t.Context(), "abcdefghij", 1)
	if !errors.Is(err, ErrInputLimitExceeded) {
		t.Fatalf("Search() chunk-limit error = %v, want ErrInputLimitExceeded", err)
	}

	_, err = service.Search(t.Context(), strings.Repeat("a", int(defaultMaxSourceBytes)+1), 1)
	if !errors.Is(err, ErrInputLimitExceeded) {
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
	service, err := New(client, defaultModelServiceConfig(t, client))
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
		mutate func(*ServiceConfig)
	}{
		{name: "invalid metric", mutate: func(c *ServiceConfig) { c.Index.Metric = 255 }},
		{name: "invalid indexed vectors", mutate: func(c *ServiceConfig) { c.Index.Limits.MaxLiveVectors = 0 }},
		{name: "invalid document chunks", mutate: func(c *ServiceConfig) { c.Index.Limits.MaxChunksPerDocument = 0 }},
		{name: "invalid search config", mutate: func(c *ServiceConfig) { c.Index.Search = SearchConfig{} }},
		{name: "search candidates exceed capacity", mutate: func(c *ServiceConfig) {
			c.Index.Limits.MaxLiveVectors = 10
			c.Index.Limits.MaxStaleVectors = 10
			c.Index.Search = SearchConfig{MaxResults: 5, MaxCandidateChunks: 11, MaxChunksPerResult: 1}
		}},
		{name: "invalid built-in chunking", mutate: func(c *ServiceConfig) { c.Chunking = chunk.Config{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := defaultModelServiceConfig(t, client)
			test.mutate(&config)
			if _, err := New(client, config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}

}

func TestNewUsesIndependentDocumentAndQueryChunkLimits(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Index.Limits.MaxChunksPerDocument = 7
	config.Index.Search = SearchConfig{
		MaxResults:         5,
		MaxCandidateChunks: 10,
		MaxChunksPerResult: 2,
		MaxQueryChunks:     3,
	}
	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	documents, ok := service.documentEncoder.(*Encoder)
	if !ok {
		t.Fatalf("document encoder type = %T", service.documentEncoder)
	}
	if documents.documentLimits.MaxChunks != 7 || service.index.limits.ingestion.maxChunksPerDocument != 7 {
		t.Fatalf(
			"chunk limits: encoder=%d index=%d, want 7",
			documents.documentLimits.MaxChunks,
			service.index.limits.ingestion.maxChunksPerDocument,
		)
	}
	queries, ok := service.queryEncoder.(*Encoder)
	if !ok {
		t.Fatalf("query encoder type = %T", service.queryEncoder)
	}
	if queries.queryLimits.MaxChunks != 3 || service.index.limits.search.maxQueryChunks != 3 {
		t.Fatalf(
			"query chunk limits: encoder=%d index=%d, want 3",
			queries.queryLimits.MaxChunks,
			service.index.limits.search.maxQueryChunks,
		)
	}
}

func TestNewUsesConfiguredEncodingLimits(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Encoding = EncodingLimits{
		MaxFields: 2, MaxSourceBytes: 128, MaxEmbeddingBytes: 16,
	}
	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	documents := service.documentEncoder.(*Encoder)
	queries := service.queryEncoder.(*Encoder)
	if documents != queries {
		t.Fatal("built-in service uses different document and query encoders")
	}
	for name, limits := range map[string]InputLimits{
		"documents": documents.documentLimits,
		"queries":   queries.queryLimits,
	} {
		if limits.MaxFields != 2 || limits.MaxSourceBytes != 128 || limits.MaxEmbeddingBytes != 16 {
			t.Fatalf("%s encoding limits = %+v", name, limits)
		}
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

func defaultModelServiceConfig(t *testing.T, client embedding.Client) ServiceConfig {
	t.Helper()
	config, err := DefaultServiceConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func originalConfig(t *testing.T, service *Service) ServiceConfig {
	t.Helper()
	config, ok := service.Config()
	if !ok {
		t.Fatal("Config() unavailable")
	}
	return config
}
