package semanticencode

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

type embeddingProviderFunc func(context.Context, []EmbeddingInput) ([][]float32, error)

func (f embeddingProviderFunc) Embed(ctx context.Context, inputs []EmbeddingInput) ([][]float32, error) {
	return f(ctx, inputs)
}

func TestDocumentEncoderEncode(t *testing.T) {
	var gotInputs []EmbeddingInput
	encoder := newTestEncoder(t, embeddingProviderFunc(func(_ context.Context, inputs []EmbeddingInput) ([][]float32, error) {
		gotInputs = append([]EmbeddingInput(nil), inputs...)
		return [][]float32{{3, 4}, {0, 5}}, nil
	}))

	encoded, err := encoder.Encode(t.Context(), fts.Document{
		ID: "doc",
		Fields: map[string]fts.FieldData{
			"title": {Text: "Title"},
			"body":  {Value: "Body"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(gotInputs) != 2 || gotInputs[0].Ref.Field != "body" || gotInputs[1].Ref.Field != "title" {
		t.Fatalf("embedding inputs are not deterministic: %+v", gotInputs)
	}
	if gotInputs[0].Text != "Body" || gotInputs[1].Text != "Title" {
		t.Fatalf("embedding texts = %q, %q", gotInputs[0].Text, gotInputs[1].Text)
	}
	if len(encoded) != 2 || math.Abs(float64(encoded[0].Vector[0])-0.6) > 1e-6 || math.Abs(float64(encoded[0].Vector[1])-0.8) > 1e-6 {
		t.Fatalf("encoded vectors = %+v", encoded)
	}
}

func TestDocumentEncoderRejectsEmbeddingCountMismatch(t *testing.T) {
	encoder := newTestEncoder(t, embeddingProviderFunc(func(context.Context, []EmbeddingInput) ([][]float32, error) {
		return nil, nil
	}))
	_, err := encoder.Encode(t.Context(), fts.Document{ID: "doc", Fields: map[string]fts.FieldData{"body": {Text: "text"}}})
	if !errors.Is(err, ErrEmbeddingCountMismatch) {
		t.Fatalf("Encode() error = %v, want ErrEmbeddingCountMismatch", err)
	}
}

func newTestEncoder(t *testing.T, provider EmbeddingProvider) *DocumentEncoder {
	t.Helper()
	embedding, err := semantic.NewEmbeddingDescriptor("test", "model", "v1", "pipeline", 2, vector.MetricCosine, 1)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := New(Config{
		Schema: semantic.Schema{
			Embedding: embedding,
			Chunking:  semantic.ChunkingDescriptor{ID: "whole", Version: 1, Fingerprint: "whole-v1"},
		},
		Limits:   Limits{MaxFields: 4, MaxSourceBytes: 1024, MaxChunks: 4, MaxEmbeddingBytes: 1024},
		Embedder: provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoder
}
