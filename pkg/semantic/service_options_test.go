package semantic

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestNewIndexesCommitsAndSearchesText(t *testing.T) {
	client, received := modelServiceTestClient(t, 512)
	service, err := New(client)
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

func TestNewFromIndexBindsExistingIndex(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	original, err := New(client)
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

func TestNewFromIndexValidatesDerivedSchema(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	original, err := New(client)
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
	if !errors.Is(err, ErrEmbeddingMismatch) {
		t.Fatalf("NewFromIndex() error = %v, want ErrEmbeddingMismatch", err)
	}
}

func TestNewFromIndexUsesIndexConfiguration(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	original, err := New(
		client,
		WithMetric(vector.MetricL2Squared),
		WithChunking(chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}),
		WithMaxIndexedVectors(256),
		WithMaxChunksPerDocument(128),
		WithSearchLimits(SearchLimits{
			MaxResults:         12,
			MaxCandidateChunks: 128,
			MaxChunksPerResult: 6,
		}),
		WithQueryConfig(QueryConfig{MaxBytes: 128, LongQuery: LongQueryTruncate}),
	)
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
	if service.Schema().Embedding.Metric != vector.MetricL2Squared {
		t.Fatalf("service metric = %v, want L2 squared", service.Schema().Embedding.Metric)
	}
	documentChunker, ok := documents.chunker.(*chunk.Splitter)
	if !ok {
		t.Fatalf("document chunker type = %T", documents.chunker)
	}
	wantChunking := chunk.Config{TargetBytes: 32, MaxBytes: 48, OverlapBytes: 4}
	if documentChunker.Config() != wantChunking {
		t.Fatalf("document chunk config = %+v, want %+v", documentChunker.Config(), wantChunking)
	}
	if documents.queryConfig != (QueryConfig{MaxBytes: 128, LongQuery: LongQueryTruncate}) {
		t.Fatalf("query config = %+v", documents.queryConfig)
	}
}

func TestNewFromIndexRejectsCustomChunkerWithoutImplementation(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	original, err := New(client, WithCustomChunker(
		failingChunker{err: errors.New("unused")},
		ChunkingSchema{ID: "custom", Version: 1, Fingerprint: "v1:custom"},
	))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewFromIndex(client, original.Index()); !errors.Is(err, ErrModelServiceUnavailable) {
		t.Fatalf("NewFromIndex() error = %v, want ErrModelServiceUnavailable", err)
	}
}

func TestNewLongQueryConfig(t *testing.T) {
	tests := []struct {
		name     string
		strategy LongQueryStrategy
		want     []string
		wantErr  error
	}{
		{name: "reject", strategy: LongQueryReject, wantErr: ErrInputLimitExceeded},
		{name: "truncate", strategy: LongQueryTruncate, want: []string{"query: éa"}},
		{name: "chunk", strategy: LongQueryChunk, want: []string{"query: éa", "query: bcd"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, received := modelServiceTestClient(t, 512)
			service, err := New(client, WithQueryConfig(QueryConfig{
				MaxBytes: 3, LongQuery: test.strategy,
			}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.Search(t.Context(), "éabcd", 1)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Search() error = %v, want %v", err, test.wantErr)
			}
			if !slices.Equal(*received, test.want) {
				t.Fatalf("embedded inputs = %q, want %q", *received, test.want)
			}
		})
	}
}

func TestNewQueryConfigChangesRetrievalNotSchema(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	first, err := New(client, WithQueryConfig(QueryConfig{MaxBytes: 16, LongQuery: LongQueryReject}))
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(client, WithQueryConfig(QueryConfig{MaxBytes: 16, LongQuery: LongQueryTruncate}))
	if err != nil {
		t.Fatal(err)
	}
	if first.Schema() != second.Schema() {
		t.Fatal("query-only config changed persisted schema")
	}
	if first.RetrievalFingerprint() == second.RetrievalFingerprint() {
		t.Fatal("query-only config did not change retrieval fingerprint")
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
	service, err := New(client)
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

func TestNewPreservesChunkerErrors(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	splitError := errors.New("split failed")
	service, err := New(client, WithCustomChunker(
		failingChunker{err: splitError},
		ChunkingSchema{ID: "failing", Version: 1, Fingerprint: "v1:test"},
	))
	if err != nil {
		t.Fatal(err)
	}
	err = service.AddDocument(t.Context(), fts.Document{
		ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "text"}},
	})
	if !errors.Is(err, ErrInvalidDocument) || !errors.Is(err, splitError) {
		t.Fatalf("AddDocument() error = %v, want invalid document and split errors", err)
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	tests := []struct {
		name    string
		options []Option
	}{
		{name: "nil option", options: []Option{nil}},
		{name: "invalid metric", options: []Option{WithMetric(255)}},
		{name: "invalid indexed vectors", options: []Option{WithMaxIndexedVectors(0)}},
		{name: "invalid document chunks", options: []Option{WithMaxChunksPerDocument(0)}},
		{name: "invalid search limits", options: []Option{WithSearchLimits(SearchLimits{})}},
		{name: "search candidates exceed capacity", options: []Option{
			WithMaxIndexedVectors(10),
			WithSearchLimits(SearchLimits{MaxResults: 5, MaxCandidateChunks: 11, MaxChunksPerResult: 1}),
		}},
		{name: "invalid built-in chunking", options: []Option{WithChunking(chunk.Config{})}},
		{name: "nil custom chunker", options: []Option{WithCustomChunker(
			nil,
			ChunkingSchema{ID: "custom", Version: 1, Fingerprint: "v1:test"},
		)}},
		{name: "invalid custom chunking schema", options: []Option{WithCustomChunker(
			failingChunker{err: errors.New("unused")},
			ChunkingSchema{},
		)}},
		{name: "missing query bytes", options: []Option{WithQueryConfig(QueryConfig{LongQuery: LongQueryReject})}},
		{name: "unknown query strategy", options: []Option{WithQueryConfig(QueryConfig{MaxBytes: 1})}},
		{name: "query limit exceeds model context", options: []Option{WithQueryConfig(QueryConfig{
			MaxBytes: 513, LongQuery: LongQueryReject,
		})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(client, test.options...); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestNewDerivesOneChunkLimitForEncoderAndIndex(t *testing.T) {
	client, _ := modelServiceTestClient(t, 512)
	service, err := New(
		client,
		WithMaxChunksPerDocument(7),
		WithSearchLimits(SearchLimits{
			MaxResults:         5,
			MaxCandidateChunks: 10,
			MaxChunksPerResult: 2,
		}),
	)
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

type failingChunker struct {
	err error
}

func (c failingChunker) Split(string) ([]chunk.Range, error) {
	return nil, c.err
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
