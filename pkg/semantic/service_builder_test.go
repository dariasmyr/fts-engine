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

func TestNewAcceptsCompleteSemanticConfig(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config, err := DefaultSemanticConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	config.Index.Metric = vector.MetricL2Squared
	config.Chunking.Builtin = chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	config.Index.Limits.MaxLiveVectors = 256
	config.Index.Limits.MaxStaleVectors = 256
	config.Index.Limits.MaxChunksPerDocument = 128
	config.Index.Search = SearchConfig{MaxResults: 12, MaxCandidateChunks: 128, MaxChunksPerResult: 6}

	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	if service.index.config != config {
		t.Fatalf("config = %+v, want %+v", service.index.config, config)
	}
}

func TestNewFromIndexBindsExistingIndex(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	original, err := New(client, defaultModelServiceConfig(t, client))
	if err != nil {
		t.Fatal(err)
	}

	service, err := NewFromIndex(client, original.Index())
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
	_, err = NewFromIndex(incompatible, original.Index())
	if !errors.Is(err, ErrModelServiceUnavailable) {
		t.Fatalf("NewFromIndex() error = %v, want ErrModelServiceUnavailable", err)
	}
}

func TestNewFromIndexUsesIndexConfiguration(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Index.Metric = vector.MetricL2Squared
	config.Chunking.Builtin = chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	config.Index.Limits.MaxLiveVectors = 256
	config.Index.Limits.MaxStaleVectors = 256
	config.Index.Limits.MaxChunksPerDocument = 128
	config.Index.Search = SearchConfig{
		MaxResults:         12,
		MaxCandidateChunks: 128,
		MaxChunksPerResult: 6,
	}
	original, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}

	service, err := NewFromIndex(client, original.Index())
	if err != nil {
		t.Fatal(err)
	}
	documents, ok := service.documentEncoder.(*modelEncoder)
	if !ok {
		t.Fatalf("document encoder type = %T", service.documentEncoder)
	}
	if documents.limits.maxChunks != 128 {
		t.Fatalf("document encoder chunk limit = %d, want 128", documents.limits.maxChunks)
	}
	if service.index.config.Index.Limits.MaxStaleVectors != 256 {
		t.Fatalf("stale vector limit = %d, want 256", service.index.config.Index.Limits.MaxStaleVectors)
	}
	if service.Config().Index.Metric != vector.MetricL2Squared {
		t.Fatalf("service metric = %v, want L2 squared", service.Config().Index.Metric)
	}
	wantChunking := chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	if documents.splitter.Config() != wantChunking {
		t.Fatalf("document chunk config = %+v, want %+v", documents.splitter.Config(), wantChunking)
	}
}

func TestNewFromIndexRejectsExternalEncodingPipeline(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Chunking = ChunkingConfig{Kind: ChunkingKindExternal}
	index, err := NewIndex(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewFromIndex(client, index); !errors.Is(err, ErrModelServiceUnavailable) {
		t.Fatalf("NewFromIndex() error = %v, want ErrModelServiceUnavailable", err)
	}
}

func TestNewSplitsQueriesWithDocumentSplitter(t *testing.T) {
	client, received := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Chunking.Builtin = chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1}
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
	before := service.Config()
	_, _ = service.SearchWithConfig(t.Context(), "query", 1, QueryConfig{CandidateChunks: 10})
	if service.Config() != before {
		t.Fatal("request-local query config changed static semantic config")
	}
}

func TestNewBoundsAutomaticallySplitQueries(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Chunking.Builtin = chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1}
	config.Index.Limits.MaxChunksPerDocument = 2
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
		mutate func(*SemanticConfig)
	}{
		{name: "invalid metric", mutate: func(c *SemanticConfig) { c.Index.Metric = 255 }},
		{name: "invalid indexed vectors", mutate: func(c *SemanticConfig) { c.Index.Limits.MaxLiveVectors = 0 }},
		{name: "invalid document chunks", mutate: func(c *SemanticConfig) { c.Index.Limits.MaxChunksPerDocument = 0 }},
		{name: "invalid search config", mutate: func(c *SemanticConfig) { c.Index.Search = SearchConfig{} }},
		{name: "search candidates exceed capacity", mutate: func(c *SemanticConfig) {
			c.Index.Limits.MaxLiveVectors = 10
			c.Index.Limits.MaxStaleVectors = 10
			c.Index.Search = SearchConfig{MaxResults: 5, MaxCandidateChunks: 11, MaxChunksPerResult: 1}
		}},
		{name: "invalid built-in chunking", mutate: func(c *SemanticConfig) { c.Chunking.Builtin = chunk.Config{} }},
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

	config := defaultModelServiceConfig(t, client)
	config.Chunking = ChunkingConfig{Kind: ChunkingKindExternal}
	if _, err := New(client, config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() external config error = %v, want ErrInvalidConfig", err)
	}
}

func TestNewDerivesOneChunkLimitForEncoderAndIndex(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	config := defaultModelServiceConfig(t, client)
	config.Index.Limits.MaxChunksPerDocument = 7
	config.Index.Search = SearchConfig{
		MaxResults:         5,
		MaxCandidateChunks: 10,
		MaxChunksPerResult: 2,
	}
	service, err := New(client, config)
	if err != nil {
		t.Fatal(err)
	}
	documents, ok := service.documentEncoder.(*modelEncoder)
	if !ok {
		t.Fatalf("document encoder type = %T", service.documentEncoder)
	}
	if documents.limits.maxChunks != 7 || service.index.limits.ingestion.maxChunksPerDocument != 7 {
		t.Fatalf(
			"chunk limits: encoder=%d index=%d, want 7",
			documents.limits.maxChunks,
			service.index.limits.ingestion.maxChunksPerDocument,
		)
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

func defaultModelServiceConfig(t *testing.T, client embedding.Client) SemanticConfig {
	t.Helper()
	config, err := DefaultSemanticConfig(client.Model())
	if err != nil {
		t.Fatal(err)
	}
	return config
}
