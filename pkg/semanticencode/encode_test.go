package semanticencode

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type testEmbedder struct {
	seen  []string
	calls int
}

func (e *testEmbedder) Embed(_ context.Context, chunks []chunk.Chunk) ([][]float32, error) {
	e.calls++
	result := make([][]float32, len(chunks))
	for i, item := range chunks {
		e.seen = append(e.seen, string(item.Ref.ID))
		result[i] = []float32{float32(i), 0}
	}
	return result, nil
}

func testConfig(embedder EmbeddingProvider) Config {
	embedding, chunking := testDescriptors()
	return Config{
		Schema:   semantic.Schema{Embedding: embedding, Chunking: chunking},
		Embedder: embedder,
		Limits: Limits{
			MaxFields: 4, MaxSourceBytes: 1 << 20, MaxChunks: 10, MaxEmbeddingBytes: 1 << 20,
		},
	}
}

func testService(t *testing.T) *semantic.Service {
	t.Helper()
	embedding, chunking := testDescriptors()
	service, err := semantic.New(semantic.Config{
		Embedding: embedding,
		Chunking:  chunking,
		Limits: semantic.Limits{
			MaxLiveVectors:          10,
			MaxChunksPerDocument:    10,
			MaxDocumentsPerSearch:   10,
			MaxChunkCandidates:      10,
			MaxChunksPerDocumentHit: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func testDescriptors() (semantic.EmbeddingDescriptor, semantic.ChunkingDescriptor) {
	embedding, err := semantic.NewEmbeddingDescriptor("test-provider", "test-model", "v1", "test-embedding", 2, vector.MetricL2Squared, 1)
	if err != nil {
		panic(err)
	}
	return embedding, semantic.ChunkingDescriptor{ID: "test-chunks", Version: 1, Fingerprint: "test-chunks-fp"}
}

func TestDocumentEncoderWithoutChunkerUsesWholeFields(t *testing.T) {
	service := testService(t)
	embedder := &testEmbedder{}
	encoder, err := New(testConfig(embedder))
	if err != nil {
		t.Fatal(err)
	}
	document := fts.Document{ID: "doc", Fields: map[string]fts.FieldData{
		"a-field": {Text: "first"},
		"z-field": {Text: "second"},
	}}
	vectors, err := encoder.Encode(context.Background(), document)
	if err != nil {
		t.Fatal(err)
	}
	if len(vectors) != 2 || vectors[0].Ref.ID != "_whole/a-field" || vectors[1].Ref.ID != "_whole/z-field" {
		t.Fatalf("encoded vectors = %+v", vectors)
	}
	if err := service.AddDocument(context.Background(), encoder, document); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := service.SearchDocuments(context.Background(), encoder, fts.Document{ID: "query", Fields: map[string]fts.FieldData{
		"a-field": {Text: "first"},
		"z-field": {Text: "second"},
	}}, 1)
	if err != nil || len(result.Hits) != 1 || len(result.Hits[0].Chunks) != 2 || result.Stats.Termination == "" {
		t.Fatalf("encoded search = %+v, %v", result, err)
	}
}

func TestDocumentEncoderRejectsEmbeddingCountMismatch(t *testing.T) {
	encoder, err := New(testConfig(mismatchedEmbedder{}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = encoder.Encode(context.Background(), fts.Document{ID: "doc", Fields: map[string]fts.FieldData{"field": {Text: "text"}}})
	if !errors.Is(err, ErrEmbeddingCountMismatch) {
		t.Fatalf("embedding count error = %v", err)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "embedder", mutate: func(c *Config) { c.Embedder = nil }},
		{name: "schema", mutate: func(c *Config) { c.Schema.Chunking = semantic.ChunkingDescriptor{} }},
		{name: "max fields", mutate: func(c *Config) { c.Limits.MaxFields = 0 }},
		{name: "max source bytes", mutate: func(c *Config) { c.Limits.MaxSourceBytes = 0 }},
		{name: "max chunks", mutate: func(c *Config) { c.Limits.MaxChunks = 0 }},
		{name: "max embedding bytes", mutate: func(c *Config) { c.Limits.MaxEmbeddingBytes = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := testConfig(&testEmbedder{})
			tt.mutate(&config)
			if _, err := New(config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New error = %v", err)
			}
			if err := config.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate error = %v", err)
			}
		})
	}
}

func TestDocumentEncoderEnforcesLimitsBeforeEmbedding(t *testing.T) {
	tests := []struct {
		name     string
		document fts.Document
		mutate   func(*Config)
	}{
		{
			name: "fields", document: documentWithFields("a", "b"),
			mutate: func(c *Config) { c.Limits.MaxFields = 1 },
		},
		{
			name: "source bytes", document: documentWithFields("abcd"),
			mutate: func(c *Config) { c.Limits.MaxSourceBytes = 3 },
		},
		{
			name: "chunks", document: documentWithFields("abcdef"),
			mutate: func(c *Config) {
				c.Limits.MaxChunks = 1
				c.Chunker = staticChunker{chunks: []chunk.Chunk{
					{Ref: chunk.Ref{ID: "a", DocID: "doc", Field: "field-0", EndByte: 3}, Text: "abc"},
					{Ref: chunk.Ref{ID: "b", DocID: "doc", Field: "field-0", Ordinal: 1, StartByte: 3, EndByte: 6}, Text: "def"},
				}}
			},
		},
		{
			name: "embedding bytes", document: documentWithFields("text"),
			mutate: func(c *Config) { c.Limits.MaxEmbeddingBytes = 7 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder := &testEmbedder{}
			config := testConfig(embedder)
			tt.mutate(&config)
			encoder, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := encoder.Encode(context.Background(), tt.document); !errors.Is(err, ErrLimitExceeded) {
				t.Fatalf("Encode error = %v", err)
			}
			if embedder.calls != 0 {
				t.Fatalf("Embed calls = %d, want 0", embedder.calls)
			}
		})
	}
}

func TestDocumentEncoderValidatesChunksAgainstSource(t *testing.T) {
	tests := []struct {
		name   string
		chunks []chunk.Chunk
		want   error
	}{
		{
			name: "wrong document", want: ErrInvalidChunk,
			chunks: []chunk.Chunk{{Ref: chunk.Ref{ID: "a", DocID: "other", Field: "field-0", EndByte: 4}, Text: "text"}},
		},
		{
			name: "wrong field", want: ErrInvalidChunk,
			chunks: []chunk.Chunk{{Ref: chunk.Ref{ID: "a", DocID: "doc", Field: "other", EndByte: 4}, Text: "text"}},
		},
		{
			name: "wrong range", want: ErrInvalidChunk,
			chunks: []chunk.Chunk{{Ref: chunk.Ref{ID: "a", DocID: "doc", Field: "field-0", EndByte: 5}, Text: "text"}},
		},
		{
			name: "wrong text", want: ErrInvalidChunk,
			chunks: []chunk.Chunk{{Ref: chunk.Ref{ID: "a", DocID: "doc", Field: "field-0", EndByte: 4}, Text: "nope"}},
		},
		{
			name: "duplicate ID", want: ErrDuplicateChunkID,
			chunks: []chunk.Chunk{
				{Ref: chunk.Ref{ID: "a", DocID: "doc", Field: "field-0", EndByte: 2}, Text: "te"},
				{Ref: chunk.Ref{ID: "a", DocID: "doc", Field: "field-0", Ordinal: 1, StartByte: 2, EndByte: 4}, Text: "xt"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder := &testEmbedder{}
			config := testConfig(embedder)
			config.Chunker = staticChunker{chunks: tt.chunks}
			encoder, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := encoder.Encode(context.Background(), documentWithFields("text")); !errors.Is(err, tt.want) {
				t.Fatalf("Encode error = %v, want %v", err, tt.want)
			}
			if embedder.calls != 0 {
				t.Fatalf("Embed calls = %d, want 0", embedder.calls)
			}
		})
	}
}

func TestDocumentEncoderValidatesAndCopiesProviderVectors(t *testing.T) {
	tests := []struct {
		name   string
		vector []float32
		want   error
	}{
		{name: "dimensions", vector: []float32{1}, want: vector.ErrDimensionMismatch},
		{name: "NaN", vector: []float32{float32(math.NaN()), 0}, want: vector.ErrNonFiniteVector},
		{name: "infinity", vector: []float32{float32(math.Inf(1)), 0}, want: vector.ErrNonFiniteVector},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &vectorEmbedder{vector: tt.vector}
			encoder, err := New(testConfig(provider))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := encoder.Encode(context.Background(), documentWithFields("text")); !errors.Is(err, tt.want) {
				t.Fatalf("Encode error = %v, want %v", err, tt.want)
			}
		})
	}

	provider := &vectorEmbedder{vector: []float32{1, 2}}
	encoder, err := New(testConfig(provider))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encoder.Encode(context.Background(), documentWithFields("text"))
	if err != nil {
		t.Fatal(err)
	}
	provider.vector[0] = 99
	if encoded[0].Vector[0] != 1 {
		t.Fatalf("encoded vector aliases provider buffer: %v", encoded[0].Vector)
	}
}

func TestDocumentEncoderChecksAndPropagatesContext(t *testing.T) {
	encoder, err := New(testConfig(&testEmbedder{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Encode(nil, documentWithFields("text")); !errors.Is(err, vector.ErrNilContext) {
		t.Fatalf("nil context error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := encoder.Encode(ctx, documentWithFields("text")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}

	canceling := &cancelingEmbedder{}
	config := testConfig(canceling)
	encoder, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	canceling.cancel = cancel
	if _, err := encoder.Encode(ctx, documentWithFields("text")); !errors.Is(err, context.Canceled) {
		t.Fatalf("provider cancellation error = %v", err)
	}
}

type mismatchedEmbedder struct{}

func (mismatchedEmbedder) Embed(context.Context, []chunk.Chunk) ([][]float32, error) {
	return nil, nil
}

type staticChunker struct {
	chunks []chunk.Chunk
}

func (s staticChunker) SplitContext(ctx context.Context, _ fts.DocID, _, _ string) ([]chunk.Chunk, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.chunks, nil
}

type vectorEmbedder struct {
	vector []float32
}

func (e *vectorEmbedder) Embed(context.Context, []chunk.Chunk) ([][]float32, error) {
	return [][]float32{e.vector}, nil
}

type cancelingEmbedder struct {
	cancel context.CancelFunc
}

func (e *cancelingEmbedder) Embed(_ context.Context, _ []chunk.Chunk) ([][]float32, error) {
	e.cancel()
	return [][]float32{{1, 2}}, nil
}

func documentWithFields(values ...string) fts.Document {
	fields := make(map[string]fts.FieldData, len(values))
	for i, value := range values {
		fields["field-"+string(rune('0'+i))] = fts.FieldData{Text: value}
	}
	return fts.Document{ID: "doc", Fields: fields}
}
