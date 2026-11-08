package semanticencode

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dariasmyr/fts-engine/v2/pkg/chunk"
	"github.com/dariasmyr/fts-engine/v2/pkg/embedding"
	"github.com/dariasmyr/fts-engine/v2/pkg/fts"
	"github.com/dariasmyr/fts-engine/v2/pkg/semantic"
	"github.com/dariasmyr/fts-engine/v2/pkg/vector"
)

func TestEncoderEncodeDocument(t *testing.T) {
	var gotTexts []string
	encoder := newTestEncoder(t, testEmbeddingClient(t, func(_ context.Context, texts []string) ([][]float32, error) {
		gotTexts = append([]string(nil), texts...)
		return [][]float32{{3, 4}, {0, 5}}, nil
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

func TestNewRejectsClientModelMismatch(t *testing.T) {
	tests := []struct {
		name  string
		model embedding.Model
	}{
		{
			name:  "model id",
			model: embedding.Model{ID: "other", Revision: "v1", Dimensions: 2, ContextTokens: 512},
		},
		{
			name:  "revision",
			model: embedding.Model{ID: "model", Revision: "v2", Dimensions: 2, ContextTokens: 512},
		},
		{
			name:  "dimensions",
			model: embedding.Model{ID: "model", Revision: "v1", Dimensions: 3, ContextTokens: 512},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := embedding.NewClient(tt.model, func(context.Context, []string) ([][]float32, error) {
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			schemaEmbedding, err := semantic.NewEmbeddingSchema(
				"model", "v1", "pipeline", 2, vector.MetricCosine, 1,
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = NewEncoder(EncoderConfig{
				Schema: semantic.Schema{
					Embedding: schemaEmbedding,
					Chunking:  semantic.ChunkingSchema{ID: "whole", Version: 1, Fingerprint: "whole-v1"},
				},
				Limits: InputLimits{MaxFields: 1, MaxSourceBytes: 1, MaxChunks: 1, MaxEmbeddingBytes: 16},
				Client: client,
			})
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestNewDerivesCompatibilityAndRolePrefix(t *testing.T) {
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
	documentEncoder, err := NewEncoder(EncoderConfig{
		Metric: vector.MetricCosine, Role: RoleDocument, Limits: limits, Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	queryEncoder, err := NewEncoder(EncoderConfig{
		Metric: vector.MetricCosine, Role: RoleQuery, Limits: limits, Client: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := documentEncoder.EncodeDocument(t.Context(), fts.Document{
		ID: "input", Fields: map[string]fts.FieldData{fts.DefaultField: {Text: "text"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queryEncoder.EncodeQuery(t.Context(), "text"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []string{"document: text", "query: text"}) {
		t.Fatalf("embedded inputs = %q", received)
	}
	if documentEncoder.Schema() != queryEncoder.Schema() {
		t.Fatal("document and query encoders derived different persisted schemas")
	}
	if documentEncoder.RetrievalFingerprint() != queryEncoder.RetrievalFingerprint() {
		t.Fatal("document and query encoders derived different retrieval fingerprints")
	}
	if got := documentEncoder.Schema().Chunking; got.ID != chunk.WholeID || got.Version != chunk.WholeVersion {
		t.Fatalf("derived whole-field chunking = %+v", got)
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
	embedding, err := semantic.NewEmbeddingSchema("model", "v1", "pipeline", 2, vector.MetricCosine, 1)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewEncoder(EncoderConfig{
		Schema: semantic.Schema{
			Embedding: embedding,
			Chunking:  semantic.ChunkingSchema{ID: "whole", Version: 1, Fingerprint: "whole-v1"},
		},
		Limits: InputLimits{MaxFields: 4, MaxSourceBytes: 1024, MaxChunks: 4, MaxEmbeddingBytes: 1024},
		Client: provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoder
}
