package semantic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dariasmyr/fts-engine/internal/memorystore"
	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/semantic"
)

func TestOpenRejectsVectorIndexValueMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, encoder := newLifecycleService(t)
	encoder.vectors["doc"] = []semantic.EncodedChunk{lifecycleChunk("doc", "chunk", []float32{1, 0})}
	if err := service.AddDocument(ctx, encoder, fts.Document{ID: "doc"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	state, err := service.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	calculator, err := state.Config.Schema.Embedding.Calculator()
	if err != nil {
		t.Fatal(err)
	}
	mismatched, err := memorystore.New(calculator, [][]float32{{2, 0}})
	if err != nil {
		t.Fatal(err)
	}
	state.Segments[0].Data.Vectors = mismatched

	if _, err := semantic.Open(ctx, *state); !errors.Is(err, semantic.ErrInvalidSegment) {
		t.Fatalf("Open() error = %v, want ErrInvalidSegment", err)
	}
}
