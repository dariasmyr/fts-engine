package semantic

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
)

func TestBuiltinEncoderDocumentAndQueryContract(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		batches [][]string
	)
	client := builtinEncoderTestClient(t, func(_ context.Context, texts []string) ([][]float32, error) {
		mu.Lock()
		batches = append(batches, append([]string(nil), texts...))
		mu.Unlock()
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{float32(i + 1), 0}
		}
		return vectors, nil
	})
	splitter, err := chunk.NewSplitter(chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	limits := InputLimits{MaxFields: 2, MaxSourceBytes: 64, MaxChunks: 8, MaxEmbeddingBytes: 64}
	model := newModelConfig(client.Model())
	encoder, err := newEncoder(client, splitter, limits, limits, &model)
	if err != nil {
		t.Fatal(err)
	}

	documentChunks, err := encoder.EncodeDocument(t.Context(), fts.Document{
		ID: "doc",
		Fields: map[string]fts.FieldData{
			"title": {Text: "xyz"},
			"body":  {Value: "abcdefghij"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	queryChunks, err := encoder.EncodeQuery(t.Context(), "abcdefghij", QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}

	wantDocumentRefs := []Ref{
		{ID: "body/chunk-000000", DocID: "doc", Field: "body", Ordinal: 0, StartByte: 0, EndByte: 4},
		{ID: "body/chunk-000001", DocID: "doc", Field: "body", Ordinal: 1, StartByte: 3, EndByte: 7},
		{ID: "body/chunk-000002", DocID: "doc", Field: "body", Ordinal: 2, StartByte: 6, EndByte: 10},
		{ID: "title/chunk-000000", DocID: "doc", Field: "title", Ordinal: 0, StartByte: 0, EndByte: 3},
	}
	if got := chunkRefs(documentChunks); !reflect.DeepEqual(got, wantDocumentRefs) {
		t.Fatalf("document refs = %+v, want %+v", got, wantDocumentRefs)
	}
	wantQueryRefs := []Ref{
		{ID: "query-000000", DocID: "_query", Field: fts.DefaultField, Ordinal: 0, StartByte: 0, EndByte: 4},
		{ID: "query-000001", DocID: "_query", Field: fts.DefaultField, Ordinal: 1, StartByte: 3, EndByte: 7},
		{ID: "query-000002", DocID: "_query", Field: fts.DefaultField, Ordinal: 2, StartByte: 6, EndByte: 10},
	}
	if got := chunkRefs(queryChunks); !reflect.DeepEqual(got, wantQueryRefs) {
		t.Fatalf("query refs = %+v, want %+v", got, wantQueryRefs)
	}
	unicodeChunks, err := encoder.EncodeQuery(t.Context(), "🙂", QueryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	wantUnicodeRef := Ref{
		ID: "query-000000", DocID: "_query", Field: fts.DefaultField, StartByte: 0, EndByte: 4,
	}
	if len(unicodeChunks) != 1 || unicodeChunks[0].Ref != wantUnicodeRef {
		t.Fatalf("Unicode query chunks = %+v, want ref %+v", unicodeChunks, wantUnicodeRef)
	}

	mu.Lock()
	defer mu.Unlock()
	wantBatches := [][]string{
		{"document: abcd", "document: defg", "document: ghij", "document: xyz"},
		{"query: abcd", "query: defg", "query: ghij"},
		{"query: 🙂"},
	}
	if !reflect.DeepEqual(batches, wantBatches) {
		t.Fatalf("embed batches = %q, want %q", batches, wantBatches)
	}
}

func TestBuiltinEncoderPreservesErrorsAndCancellation(t *testing.T) {
	t.Parallel()

	modelErr := errors.New("model failed")
	client := builtinEncoderTestClient(t, func(context.Context, []string) ([][]float32, error) {
		return nil, modelErr
	})
	splitter, err := chunk.NewSplitter(chunk.Config{TargetBytes: 8, MaxBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	model := newModelConfig(client.Model())
	encoder, err := newEncoder(
		client,
		splitter,
		InputLimits{MaxFields: 1, MaxSourceBytes: 8, MaxChunks: 1, MaxEmbeddingBytes: 8},
		InputLimits{MaxFields: 1, MaxSourceBytes: 8, MaxChunks: 1, MaxEmbeddingBytes: 8},
		&model,
	)
	if err != nil {
		t.Fatal(err)
	}

	document := fts.Document{ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "text"}}}
	if _, err := encoder.EncodeDocument(t.Context(), document); !errors.Is(err, modelErr) {
		t.Fatalf("EncodeDocument() error = %v, want model error", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := encoder.EncodeDocument(canceled, document); !errors.Is(err, context.Canceled) {
		t.Fatalf("EncodeDocument(canceled) error = %v, want context.Canceled", err)
	}
	invalidUTF8 := fts.Document{ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: string([]byte{0xff})}}}
	if _, err := encoder.EncodeDocument(t.Context(), invalidUTF8); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("EncodeDocument(invalid UTF-8) error = %v, want ErrInvalidDocument", err)
	}
}

func builtinEncoderTestClient(t testing.TB, embed embedding.ClientFunc) embedding.Client {
	t.Helper()
	client, err := embedding.NewClient(embedding.Model{
		ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 512,
		DocumentPrefix: "document: ", QueryPrefix: "query: ",
	}, embed)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func chunkRefs(chunks []ChunkEmbedding) []Ref {
	refs := make([]Ref, len(chunks))
	for i, item := range chunks {
		refs[i] = item.Ref
	}
	return refs
}
