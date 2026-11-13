package encoding

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
)

func TestEncoderEncodeDocument(t *testing.T) {
	var gotTexts []string
	firstVector := []float32{3, 4}
	encoder := newTestEncoder(t, testEmbeddingClient(t, func(_ context.Context, texts []string) ([][]float32, error) {
		gotTexts = append([]string(nil), texts...)
		return [][]float32{firstVector, {0, 5}}, nil
	}))

	encoded, err := encoder.EncodeDocument(t.Context(), fts.Document{
		ID: "doc",
		Fields: map[string]fts.FieldData{
			"title": {Text: "Title"},
			"body":  {Value: "Body"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(gotTexts) != 2 || gotTexts[0] != "Body" || gotTexts[1] != "Title" {
		t.Fatalf("embedding texts = %v", gotTexts)
	}
	if len(encoded) != 2 || !slices.Equal(encoded[0].Embedding, []float32{3, 4}) {
		t.Fatalf("encoded vectors = %+v", encoded)
	}
	if &encoded[0].Embedding[0] != &firstVector[0] {
		t.Fatal("EncodeDocument() copied or replaced the provider vector row")
	}
	wantRefs := []semantic.Ref{
		{ID: "_whole/body", DocID: "doc", Field: "body", EndByte: 4},
		{ID: "_whole/title", DocID: "doc", Field: "title", EndByte: 5},
	}
	if got := encodedRefs(encoded); !reflect.DeepEqual(got, wantRefs) {
		t.Fatalf("encoded refs = %+v, want %+v", got, wantRefs)
	}
}

func TestEncoderRejectsEmbeddingCountMismatch(t *testing.T) {
	encoder := newTestEncoder(t, testEmbeddingClient(t, func(context.Context, []string) ([][]float32, error) {
		return nil, nil
	}))
	_, err := encoder.EncodeDocument(t.Context(), fts.Document{ID: "doc", Fields: map[string]fts.FieldData{"body": {Text: "text"}}})
	if !errors.Is(err, ErrEmbeddingCountMismatch) {
		t.Fatalf("Encode() error = %v, want ErrEmbeddingCountMismatch", err)
	}
}

func TestNewEncoderRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	client := testEmbeddingClient(t, func(context.Context, []string) ([][]float32, error) {
		return nil, nil
	})
	validLimits := InputLimits{
		MaxFields: 1, MaxSourceBytes: 16, MaxChunks: 1, MaxEmbeddingBytes: 8,
	}
	tests := []struct {
		name   string
		config EncoderConfig
	}{
		{name: "nil client", config: EncoderConfig{DocumentLimits: validLimits, QueryLimits: validLimits}},
		{name: "zero limits", config: EncoderConfig{Client: client}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewEncoder(test.config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewEncoder() error = %v, want ErrInvalidConfig", err)
			}
			if err := test.config.Validate(); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("EncoderConfig.Validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestNewEncoderKeepsIndependentLimits(t *testing.T) {
	t.Parallel()

	client := testEmbeddingClient(t, func(context.Context, []string) ([][]float32, error) { return nil, nil })
	documentLimits := InputLimits{MaxFields: 2, MaxSourceBytes: 64, MaxChunks: 7, MaxEmbeddingBytes: 64}
	queryLimits := InputLimits{MaxFields: 1, MaxSourceBytes: 32, MaxChunks: 3, MaxEmbeddingBytes: 32}
	encoder, err := NewEncoder(EncoderConfig{
		Client: client, DocumentLimits: documentLimits, QueryLimits: queryLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	if encoder.documentLimits != documentLimits || encoder.queryLimits != queryLimits {
		t.Fatalf("limits = %+v/%+v, want %+v/%+v", encoder.documentLimits, encoder.queryLimits, documentLimits, queryLimits)
	}
}

func TestEncoderNumberedDocumentContract(t *testing.T) {
	t.Parallel()

	var received []string
	client := testEmbeddingClient(t, func(_ context.Context, texts []string) ([][]float32, error) {
		received = append(received, texts...)
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	})
	splitter, err := chunk.NewSplitter(chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewEncoder(EncoderConfig{
		Splitter: splitter, Client: client,
		DocumentLimits: InputLimits{MaxFields: 2, MaxSourceBytes: 64, MaxChunks: 8, MaxEmbeddingBytes: 64},
		QueryLimits:    InputLimits{MaxFields: 1, MaxSourceBytes: 64, MaxChunks: 8, MaxEmbeddingBytes: 64},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encoder.EncodeDocument(t.Context(), fts.Document{
		ID: "doc",
		Fields: map[string]fts.FieldData{
			"title": {Text: "xyz"},
			"body":  {Value: "abcdefghij"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantRefs := []semantic.Ref{
		{ID: "body/chunk-000000", DocID: "doc", Field: "body", StartByte: 0, EndByte: 4},
		{ID: "body/chunk-000001", DocID: "doc", Field: "body", Ordinal: 1, StartByte: 3, EndByte: 7},
		{ID: "body/chunk-000002", DocID: "doc", Field: "body", Ordinal: 2, StartByte: 6, EndByte: 10},
		{ID: "title/chunk-000000", DocID: "doc", Field: "title", StartByte: 0, EndByte: 3},
	}
	if got := encodedRefs(encoded); !reflect.DeepEqual(got, wantRefs) {
		t.Fatalf("document refs = %+v, want %+v", got, wantRefs)
	}
	wantTexts := []string{"abcd", "defg", "ghij", "xyz"}
	if !slices.Equal(received, wantTexts) {
		t.Fatalf("embedding texts = %q, want %q", received, wantTexts)
	}
}

func TestEncoderUsesModelPrefixes(t *testing.T) {
	model := embedding.Model{
		ID: "model", Revision: "digest", Dimensions: 2, ContextTokens: 512,
		DocumentPrefix: "document: ", QueryPrefix: "query: ",
	}
	var received []string
	client, err := embedding.NewClient(model, func(_ context.Context, texts []string) ([][]float32, error) {
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
	limits := InputLimits{MaxFields: 1, MaxSourceBytes: 1024, MaxChunks: 1, MaxEmbeddingBytes: 8}
	encoder, err := NewEncoder(EncoderConfig{
		DocumentLimits: limits, QueryLimits: limits, Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.EncodeDocument(t.Context(), fts.Document{
		ID: "input", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "text"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.EncodeQuery(t.Context(), "text"); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.EncodeQueryDocument(t.Context(), fts.Document{
		ID: "query", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "text"}},
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"document: text", "query: text", "query: text"}) {
		t.Fatalf("embedded inputs = %q", received)
	}
}

func TestEncoderUsesSplitterForQueries(t *testing.T) {
	var received []string
	client := testEmbeddingClient(t, func(_ context.Context, texts []string) ([][]float32, error) {
		received = append(received, texts...)
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	})
	splitter, err := chunk.NewSplitter(chunk.Config{TargetBytes: 4, MaxBytes: 6, OverlapBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewEncoder(EncoderConfig{
		Splitter: splitter, Client: client,
		DocumentLimits: InputLimits{MaxFields: 1, MaxSourceBytes: 1024, MaxChunks: 4, MaxEmbeddingBytes: 32},
		QueryLimits:    InputLimits{MaxFields: 1, MaxSourceBytes: 1024, MaxChunks: 4, MaxEmbeddingBytes: 32},
	})
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := encoder.EncodeQuery(t.Context(), "abcdefghij")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"abcd", "defg", "ghij"}
	if !slices.Equal(received, want) {
		t.Fatalf("embedded inputs = %q, want %q", received, want)
	}
	wantRefs := []semantic.Ref{
		{ID: "query-000000", DocID: "_query", Field: fts.DefaultField, StartByte: 0, EndByte: 4},
		{ID: "query-000001", DocID: "_query", Field: fts.DefaultField, Ordinal: 1, StartByte: 3, EndByte: 7},
		{ID: "query-000002", DocID: "_query", Field: fts.DefaultField, Ordinal: 2, StartByte: 6, EndByte: 10},
	}
	if got := encodedRefs(encoded); !reflect.DeepEqual(got, wantRefs) {
		t.Fatalf("query refs = %+v, want %+v", got, wantRefs)
	}
}

func TestEncoderUsesSemanticErrorsAndCancellation(t *testing.T) {
	t.Parallel()

	encoder := newTestEncoder(t, testEmbeddingClient(t, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	}))
	invalidUTF8 := fts.Document{ID: "doc", Fields: map[string]fts.FieldData{
		fts.DefaultField: {Text: string([]byte{0xff})},
	}}
	if _, err := encoder.EncodeDocument(t.Context(), invalidUTF8); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("EncodeDocument(invalid UTF-8) error = %v, want ErrInvalidDocument", err)
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := encoder.EncodeDocument(canceled, fts.Document{
		ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "text"}},
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("EncodeDocument(canceled) error = %v, want context.Canceled", err)
	}
}

func TestEncoderRejectsInvalidSplitterRange(t *testing.T) {
	t.Parallel()

	client := testEmbeddingClient(t, func(context.Context, []string) ([][]float32, error) {
		t.Fatal("embedding client called for an invalid range")
		return nil, nil
	})
	encoder, err := NewEncoder(EncoderConfig{
		DocumentLimits: InputLimits{MaxFields: 1, MaxSourceBytes: 16, MaxChunks: 1, MaxEmbeddingBytes: 8},
		QueryLimits:    InputLimits{MaxFields: 1, MaxSourceBytes: 16, MaxChunks: 1, MaxEmbeddingBytes: 8},
		Splitter:       invalidRangeSplitter{},
		Client:         client,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = encoder.EncodeDocument(t.Context(), fts.Document{
		ID: "doc", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "text"}},
	})
	if !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("EncodeDocument() error = %v, want ErrInvalidDocument", err)
	}
}

func TestEncoderConcurrentUse(t *testing.T) {
	t.Parallel()

	client := testEmbeddingClient(t, func(_ context.Context, texts []string) ([][]float32, error) {
		vectors := make([][]float32, len(texts))
		for i := range vectors {
			vectors[i] = []float32{1, 0}
		}
		return vectors, nil
	})
	encoder := newTestEncoder(t, client)

	const workers = 32
	var wait sync.WaitGroup
	errorsByWorker := make([]error, workers)
	for worker := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, errorsByWorker[worker] = encoder.EncodeQuery(t.Context(), "🙂")
		}()
	}
	wait.Wait()
	for worker, err := range errorsByWorker {
		if err != nil {
			t.Fatalf("worker %d: %v", worker, err)
		}
	}
}

func testEmbeddingClient(t testing.TB, embed embedding.ClientFunc) embedding.Client {
	t.Helper()
	client, err := embedding.NewClient(embedding.Model{
		ID: "model", Revision: "v1", Dimensions: 2, ContextTokens: 512,
	}, embed)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newTestEncoder(t *testing.T, provider embedding.Client) *Encoder {
	t.Helper()
	encoder, err := NewEncoder(EncoderConfig{
		DocumentLimits: InputLimits{MaxFields: 4, MaxSourceBytes: 1024, MaxChunks: 4, MaxEmbeddingBytes: 1024},
		QueryLimits:    InputLimits{MaxFields: 4, MaxSourceBytes: 1024, MaxChunks: 4, MaxEmbeddingBytes: 1024},
		Client:         provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoder
}

func encodedRefs(chunks []semantic.ChunkEmbedding) []semantic.Ref {
	refs := make([]semantic.Ref, len(chunks))
	for i, item := range chunks {
		refs[i] = item.Ref
	}
	return refs
}

type invalidRangeSplitter struct{}

func (invalidRangeSplitter) Split(string) ([]chunk.Range, error) {
	return []chunk.Range{{StartByte: 1, EndByte: 5}}, nil
}
