package semanticencode

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/pkg/chunk"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
	"github.com/dariasmyr/fts-engine/pkg/vector"
)

type testEmbedder struct {
	seen []string
}

func (e *testEmbedder) Embed(_ context.Context, chunks []chunk.Chunk) ([][]float32, error) {
	result := make([][]float32, len(chunks))
	for i, item := range chunks {
		e.seen = append(e.seen, string(item.Ref.ID))
		result[i] = []float32{float32(i), 0}
	}
	return result, nil
}

func testService(t *testing.T) *semantic.Service {
	t.Helper()
	service, err := semantic.New(semantic.Config{
		Space:                   semantic.SpaceDescriptor{ID: "test-space", Dimensions: 2, Metric: vector.MetricL2Squared, VectorFormatVersion: 1},
		Chunking:                semantic.ChunkingDescriptor{ID: "test-chunks"},
		MaxVectors:              10,
		MaxChunksPerDocument:    10,
		MaxK:                    10,
		MaxChunkCandidates:      10,
		MaxChunksPerDocumentHit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestDocumentEncoderWithoutChunkerUsesWholeFields(t *testing.T) {
	service := testService(t)
	encoder, err := New(nil, &testEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	document := Document{ID: "doc", Fields: map[string]string{
		"z-field": "second",
		"a-field": "first",
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
	result, err := service.SearchDocuments(context.Background(), encoder, Document{ID: "query", Fields: map[string]string{
		"a-field": "first",
		"z-field": "second",
	}}, 1)
	if err != nil || len(result.Hits) != 1 || len(result.Hits[0].Chunks) != 2 || result.Stats.Termination == "" {
		t.Fatalf("encoded search = %+v, %v", result, err)
	}
}

func TestDocumentEncoderRejectsEmbeddingCountMismatch(t *testing.T) {
	encoder, err := New(nil, mismatchedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = encoder.Encode(context.Background(), Document{ID: "doc", Fields: map[string]string{"field": "text"}})
	if !errors.Is(err, ErrEmbeddingCountMismatch) {
		t.Fatalf("embedding count error = %v", err)
	}
}

type mismatchedEmbedder struct{}

func (mismatchedEmbedder) Embed(context.Context, []chunk.Chunk) ([][]float32, error) {
	return nil, nil
}
